# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


import os
import signal
from builtins import bytes
import subprocess
from subprocess import Popen, STDOUT, PIPE
from tempfile import gettempdir, NamedTemporaryFile

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.file import TemporaryDirectory
from airflow.utils.operator_helpers import context_to_airflow_vars


class BatchOperator(BaseOperator):
    """
    Execute a Windows Batch script, command or set of commands.

    If BatchOperator.do_xcom_push is True, the last line written to stdout
    will also be pushed to an XCom when the batch command completes

    :param batch_command: The command, set of commands or reference to a
        batch script (must be '.bat') to be executed. (templated)
    :type batch_command: str
    :param env: If env is not None, it must be a mapping that defines the
        environment variables for the new process; these are used instead
        of inheriting the current process environment, which is the default
        behavior. (templated)
    :type env: dict
    :param output_encoding: Output encoding of batch command
    :type output_encoding: str

    On execution of this operator the task will be up for retry
    when exception is raised. However, if a sub-command exits with non-zero
    value Airflow will not recognize it as failure unless the whole shell exits
    with a failure. The easiest way of achieving this is to prefix the command
    with ``&&``
    Example:

    .. code-block:: python

        batch_command = "set PATH && python3 script.py '{{ next_execution_date }}'"
    """
    template_fields = ('batch_command', 'env')
    template_ext = ('.bat')
    ui_color = '#f0ede4'

    @apply_defaults
    def __init__(
            self,
            batch_command,
            env=None,
            output_encoding='utf-8',
            *args, **kwargs):

        super(BatchOperator, self).__init__(*args, **kwargs)
        self.batch_command = batch_command
        self.env = env
        self.output_encoding = output_encoding
        if kwargs.get('xcom_push') is not None:
            raise AirflowException("'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead")

    def execute(self, context):
        """
        Execute the batch command in a temporary directory
        which will be cleaned afterwards
        """
        self.log.info('Tmp dir root location: \n %s', gettempdir())

        # Prepare env for child process.
        if self.env is None:
            self.env = os.environ.copy()

        airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
        self.log.info('Exporting the following env vars:\n' +
                      '\n'.join(["{}={}".format(k, v)
                                 for k, v in
                                 airflow_context_vars.items()]))
        self.env.update(airflow_context_vars)

        self.lineage_data = self.batch_command

        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, prefix=self.task_id) as tmp_file:
                tmp_file.write(bytes(self.batch_command, 'utf_8'))
                tmp_file.flush()
                script_location = os.path.abspath(tmp_file.name)
                self.log.info('Temporary script location: %s', script_location)

                self.log.info('Running command: %s', self.batch_command)
                sub_process = Popen(
                    tmp_file.name,
                    stdout=PIPE,
                    stderr=STDOUT,
                    cwd=tmp_dir,
                    env=self.env,
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
                    )

                self.sub_process = sub_process

                self.log.info('Output:')
                line = ''
                for raw_line in iter(sub_process.stdout.readline, b''):
                    line = raw_line.decode(self.output_encoding).rstrip()
                    self.log.info(line)

                sub_process.wait()

                self.log.info('Command exited with return code %s', sub_process.returncode)

                if sub_process.returncode:
                    raise AirflowException('Batch command failed')

        return line

    def on_kill(self):
        self.log.info('Sending SIGTERM signal to batch process group')
        self.sub_process.kill()
        self.sub_process.send_signal(signal.CTRL_BREAK_EVENT)
