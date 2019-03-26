# -*- coding: utf-8 -*-
#
import math
import os
import re
import subprocess
import time
import json
import traceback
from multiprocessing import Pool, cpu_count

from airflow import configuration
#from airflow.config_templates.default_tlp import DEFAULT_TLP_CONFIG
from airflow.contrib.tlp import states as tlp_states
#from airflow.contrib.tlp import client
from airflow.exceptions import AirflowException
from airflow.executors.base_executor import BaseExecutor
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.module_loading import import_string
from airflow.utils.timeout import timeout

# Make it constant for unit test.
TLP_FETCH_ERR_MSG_HEADER = 'Error fetching TLP task state'

TLP_SEND_ERR_MSG_HEADER = 'Error sending TLP task'

#if configuration.conf.has_option('tlp', 'tlp_config_options'):
#    tlp_configuration = import_string(
#        configuration.conf.get('tlp', 'tlp_config_options')
#    )
#else:
#    tlp_configuration = None
#
#tlp_client = TlpClient(config=tlp_configuration)

def tlp_submit_command(command, queue):
    log = LoggingMixin().log
    log.info("Submitting command in TLP: %s", command)
    env = os.environ.copy()
    tlp_batch = None
    if not queue:
        queue = 'regress_linux'
    try:
        tlp_tmpl = {'flow_params': {'all': {'airflow_task_command': ' '.join(command)}},
                    'project_params': {'all': {'HLM_TIER': 'a630v1'}},
                    'resources': {'all': {'TLP_QUEUE': queue}}}

        tlp_cmd = ['qe', 'tlp', 'airflow',
                   '--template', json.dumps(tlp_tmpl)]
        output = subprocess.check_output(tlp_cmd, stderr=subprocess.STDOUT,
                                         close_fds=True, env=env)
        match = re.search('Batch Job <(\d+)> Submitted', output.decode('utf-8'))
        if match:
            tlp_batch = match.group(1)
            log.info("TLP batch %s submitted", tlp_batch)
        else:
            raise AirflowException('TLP submit failed')
    except subprocess.CalledProcessError as e:
        log.exception('execute_command encountered a CalledProcessError')
        log.error(e.output)

        raise AirflowException('TLP submit failed')
    return tlp_batch


class ExceptionWithTraceback(object):
    """
    Wrapper class used to propagate exceptions to parent processes from subprocesses.

    :param exception: The exception to wrap
    :type exception: Exception
    :param exception_traceback: The stacktrace to wrap
    :type exception_traceback: str
    """

    def __init__(self, exception, exception_traceback):
        self.exception = exception
        self.traceback = exception_traceback


def fetch_tlp_task_state(tlp_task):
    """
    Fetch and return the state of the given tlp task. The scope of this function is
    global so that it can be called by subprocesses in the pool.

    :param tlp_task: a tuple of the TLP task key and the async TLP object used
        to fetch the task's state
    :type tlp_task: tuple(str, tlp.result.AsyncResult)
    :return: a tuple of the TLP task key and the TLP state of the task
    :rtype: tuple[str, str]
    """
    env = os.environ.copy()
    task_key, tlp_batch = tlp_task
    res = (task_key, tlp_states.SUBMITTED)
    try:
        with timeout(seconds=5):
            tlp_cmd = ['qe', 'rjobs', '-d', '-b', tlp_batch]
            output = subprocess.check_output(tlp_cmd, stderr=subprocess.STDOUT,
                                             close_fds=True, env=env)
            match = re.search('\d+\s+airflow_task\s+(\S+)\s+', output.decode('utf-8'))
            if match:
                state = match.group(1)
                res = (task_key, state)
            else:
                raise AirflowException('TLP batch not found')

    except subprocess.CalledProcessError as e:
        if e.returncode == 1:
            res = (task_key, tlp_states.FAIL)
    except Exception as e:
        exception_traceback = "TLP Task ID: {}\n{}".format(task_key,
                                                           traceback.format_exc())
        res = ExceptionWithTraceback(e, exception_traceback)
    return res


def send_task_to_executor(task_tuple):
    key, simple_ti, command, queue = task_tuple
    try:
        with timeout(seconds=5):
            # return tlp batch
            # result = task.apply_async(args=[command], queue=queue)
            result = tlp_submit_command(command=command, queue=queue)
    except Exception as e:
        exception_traceback = "TLP Task ID: {}\n{}".format(key,
                                                           traceback.format_exc())
        result = ExceptionWithTraceback(e, exception_traceback)

    return key, command, result


class TlpExecutor(BaseExecutor):
    """
    TlpExecutor allows distributing the execution of task instances to multiple 
    worker nodes on TLP farm.
    """

    def __init__(self):
        super(TlpExecutor, self).__init__()

        # Celery doesn't support querying the state of multiple tasks in parallel
        # (which can become a bottleneck on bigger clusters) so we use
        # a multiprocessing pool to speed this up.
        # How many worker processes are created for checking tlp task state.
        self._sync_parallelism = configuration.getint('tlp', 'SYNC_PARALLELISM')
        if self._sync_parallelism == 0:
            self._sync_parallelism = max(1, cpu_count() - 1)

        self._sync_pool = None
        self.tasks = {}
        self.last_state = {}

    def start(self):
        self.log.debug(
            'Starting TLP Executor using {} processes for syncing'.format(
                self._sync_parallelism))

    def _num_tasks_per_send_process(self, to_send_count):
        """
        How many TLP tasks should each worker process send.

        :return: Number of tasks that should be sent per process
        :rtype: int
        """
        return max(1,
                   int(math.ceil(1.0 * to_send_count / self._sync_parallelism)))

    def _num_tasks_per_fetch_process(self):
        """
        How many TLP tasks should be sent to each worker process.

        :return: Number of tasks that should be used per process
        :rtype: int
        """
        return max(1,
                   int(math.ceil(1.0 * len(self.tasks) / self._sync_parallelism)))

    def heartbeat(self):
        # Triggering new jobs
        if not self.parallelism:
            open_slots = len(self.queued_tasks)
        else:
            open_slots = self.parallelism - len(self.running)

        self.log.debug("{} running task instances".format(len(self.running)))
        self.log.debug("{} in queue".format(len(self.queued_tasks)))
        self.log.debug("{} open slots".format(open_slots))

        sorted_queue = sorted(
            [(k, v) for k, v in self.queued_tasks.items()],
            key=lambda x: x[1][1],
            reverse=True)

        task_tuples_to_send = []

        for i in range(min((open_slots, len(self.queued_tasks)))):
            key, (command, _, queue, simple_ti) = sorted_queue.pop(0)
            task_tuples_to_send.append((key, simple_ti, command, queue))

        if task_tuples_to_send:
            # Use chunking instead of a work queue to reduce context switching
            # since tasks are roughly uniform in size
            chunksize = self._num_tasks_per_send_process(len(task_tuples_to_send))
            num_processes = min(len(task_tuples_to_send), self._sync_parallelism)

            send_pool = Pool(processes=num_processes)
            key_and_async_results = send_pool.map(
                send_task_to_executor,
                task_tuples_to_send,
                chunksize=chunksize)

            send_pool.close()
            send_pool.join()
            self.log.debug('Sent all tasks.')

            for key, command, result in key_and_async_results:
                if isinstance(result, ExceptionWithTraceback):
                    self.log.error(
                        TLP_SEND_ERR_MSG_HEADER + ":{}\n{}\n".format(
                            result.exception, result.traceback))
                elif result is not None:
                    # Only pops when enqueued successfully, otherwise keep it
                    # and expect scheduler loop to deal with it.
                    self.queued_tasks.pop(key)
                    self.running[key] = command
                    self.tasks[key] = result
                    self.log.info('send task %s command: %s to tlp batch %s', key, command, result)

        # Calling child class sync method
        self.log.debug("Calling the {} sync method".format(self.__class__))
        self.sync()

    def sync(self):
        num_processes = min(len(self.tasks), self._sync_parallelism)
        if num_processes == 0:
            self.log.debug("No task to query tlp, skipping sync")
            return

        self.log.debug("Inquiring about %s tlp task(s) using %s processes",
                       len(self.tasks), num_processes)

        # Recreate the process pool each sync in case processes in the pool die
        self._sync_pool = Pool(processes=num_processes)

        # Use chunking instead of a work queue to reduce context switching since tasks are
        # roughly uniform in size
        chunksize = self._num_tasks_per_fetch_process()

        self.log.debug("Waiting for inquiries to complete...")
        task_keys_to_states = self._sync_pool.map(
            fetch_tlp_task_state,
            self.tasks.items(),
            chunksize=chunksize)
        self._sync_pool.close()
        self._sync_pool.join()
        self.log.debug("Inquiries completed.")

        for key_and_state in task_keys_to_states:
            if isinstance(key_and_state, ExceptionWithTraceback):
                self.log.error(
                    TLP_FETCH_ERR_MSG_HEADER + ", ignoring it:{}\n{}\n".format(
                        key_and_state.exception, key_and_state.traceback))
                continue
            key, state = key_and_state
            try:
                if state == tlp_states.PASS:
                    self.success(key)
                    del self.tasks[key]
                elif state in [tlp_states.FAIL, tlp_states.ERROR, tlp_states.TIMEOUT]:
                    self.fail(key)
                    del self.tasks[key]
            except Exception:
                self.log.exception("Error syncing the Celery executor, ignoring it.")

    def end(self, synchronous=False):
        if synchronous:
            while any([
                    task.state not in tlp_states.READY_STATES
                    for task in self.tasks.values()]):
                time.sleep(5)
        self.sync()
