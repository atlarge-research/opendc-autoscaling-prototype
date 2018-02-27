import os

import toposort
from sortedcontainers import SortedSet

from core import SimCore, Constants
from core.SimLogger import DBLogger
from utils import SimUtils


class CentralQueue(SimCore.SimEntity):
    """Central queue for new tasks."""

    def __init__(self, simulator, name):
        super(CentralQueue, self).__init__(simulator, name)

        output = SimUtils.get_output(self.config)
        DBLog_path = os.path.join(output, self.config['simulation']['DBLog'])
        self.logger = DBLogger(sim=self.sim, DBName=DBLog_path, BufferSize=10000)

        self.N_TICKS_MONITOR_SITE_STATUS = self.config['central_queue']['N_TICKS_MONITOR_SITE_STATUS']

        self.submitted_tasks_count = 0
        self.finished_tasks_count = 0
        self.total_available_resources = 0
        self.task_queue = []
        self.ready_tasks = SortedSet(key=lambda task: task.ts_submit)
        self.workflows = {}

        self.events_map = {
            Constants.CQ2CQs_MONITOR_SITE_STATUS: self.monitor_sites,
            Constants.S2U_TASK_DONE: self.task_done,
        }

        self.site_stats = []
        self.site_stats_changed = False  # used as a dirty flag for site_stats

        self.logger.log_and_db('CentralQueue initialized', 'debug')

    def set_task_list(self, task_list, first_submission_at_zero=True):
        """Set initial list of tasks."""

        self.logger.log_and_db('Assign tasks starts')
        self.task_queue = task_list

        # in case the first_submission_at_zero flag is set, first task will have
        # ts_submit=0; the rest of the tasks will follow by having their initial
        # ts_submit substracted with the first's task ts_submit
        if self.task_queue and first_submission_at_zero:
            first_ts_submit = self.task_queue[0].ts_submit
            for task in self.task_queue:
                task.ts_submit = max(task.ts_submit - first_ts_submit, 0)

        # logger.log('Task list:\n{0}\nFirstSubmitAtZero: {1}'.format(
        #     pprint.pformat(self.task_queue),
        #     first_submission_at_zero), 'debug'
        # )

        self.sort_by_ts_submit()
        # self.sort_by_dependencies()

        self.logger.db('CentralQueue got {0} tasks'.format(len(self.task_queue)))
        self.logger.db('Assign tasks ends')
        self.logger.flush()

    def set_workflow_dict(self, workflows):
        self.workflows = workflows

    def extend_task_list(self, tasks):
        """Used to resubmit tasks that have been interrupted."""

        self.submitted_tasks_count -= len(tasks)
        self.task_queue.extend(tasks)
        self.sort_by_ts_submit()

    def sort_by_ts_submit(self):
        self.task_queue.sort(key=lambda task: task.ts_submit)

    def sort_by_dependencies(self):
        """
        Topological sort based on each task's dependencies.
        Check https://pypi.python.org/pypi/toposort/1.5
        """

        id_dependencies_map = dict((task.id, task.dependencies) for task in self.task_queue)
        id_ts_submit_map = dict((task.id, task.ts_submit) for task in self.task_queue)

        id_sets = toposort.toposort(id_dependencies_map)

        sorted_ids = []
        for id_set in list(id_sets):
            sorted_ids.extend(sorted(id_set, key=lambda task_id: id_ts_submit_map[task_id]))

        self.task_queue.sort(key=lambda task: sorted_ids.index(task.id))

    def activate(self):
        """First monitor sites, then reschedule tasks."""

        self.events.enqueue(
            SimCore.Event(self.sim.ts_now, self.id, self.id, {'type': Constants.CQ2CQs_MONITOR_SITE_STATUS}))

    def sort_site_stats(self):
        # sort sites by amount of free resources
        if self.site_stats_changed:
            self.site_stats.sort(reverse=True)
            self.site_stats_changed = False

    def monitor_sites(self, params):
        """Get monitoring information from existing sites: read queue length."""

        self.site_stats = []
        self.total_available_resources = 0
        for site in self.sim.sites:
            if site.status == Constants.STATUS_SHUTDOWN:
                continue

            site_free_resources = site.free_resources - sum(task.cpus for task in site.task_queue)
            self.total_available_resources += site_free_resources

            self.site_stats.append(
                (site_free_resources,
                 site.name,
                 site.id,
                 site.leased_instance,
                 site.expiration_ts)
            )

        self.site_stats_changed = True

        # schedule the next monitoring event
        self.events.enqueue(
            SimCore.Event(self.sim.ts_now + self.N_TICKS_MONITOR_SITE_STATUS, self.id, self.id,
                          {'type': Constants.CQ2CQs_MONITOR_SITE_STATUS}))

    def add_site_stats(self, site):
        site_free_resources = site.free_resources - sum(task.cpus for task in site.task_queue)
        self.total_available_resources += site_free_resources
        self.site_stats.append(
            (site_free_resources,
             site.name,
             site.id,
             site.leased_instance,
             site.expiration_ts)
        )
        self.site_stats_changed = True

    def remove_site_stats(self, site_id):
        for index, site in enumerate(self.site_stats):
            if site[2] == site_id:
                self.total_available_resources -= site[0]
                del self.site_stats[index]
                self.site_stats_changed = True
                return

    def tasks_to_schedule(self):
        """
        Returns a list of tasks ready to be scheduled (they have all their dependencies met and ts_submit <= ts_now).
        Assumes self.task_queue is sorted by ts_submit.
        """

        minimal_task_amount = 0

        for task in self.task_queue[:]:
            if task.ts_submit > self.sim.ts_now:
                break

            if task.dependencies:
                continue

            self.ready_tasks.add(task)
            self.task_queue.remove(task)

            # Optimization: Count the amount of smallest tasks possible
            # If we exceed exceed the amount of available resources with this,
            # skip the rest of the tasks because they cannot be scheduled anyway.
            
            if task.cpus == 1:
                minimal_task_amount += 1

            if minimal_task_amount == self.total_available_resources:
                break

        return self.ready_tasks

    def try_schedule_tasks(self):
        """
        Override this function with your scheduling (allocation) logic.
        """
        pass

    def task_done(self, params):
        task = params['task']
        self.finished_tasks_count += 1
        # self.finished_tasks.add(task.id)

        # Check if the task belongs to a workflow; workflow_id can be 0, so check for not None
        if task.workflow_id is not None:
            workflow = self.workflows[task.workflow_id]

            # If it has no parents, it's an entry task. Try to mark WF as started.
            # Workflows are marked as started here and not when the task is started as a Task can be interrupted.
            if not task.parents and not workflow.workflow_started():
                workflow.start(task.ts_start)

            # If it has no children, it's an exit task. Check if the WF is completed
            if not task.children:
                if workflow.workflow_completed():
                    workflow.ts_finish = task.ts_end
            else:
                for child in task.children:
                    child.dependencies.remove(task.id)

    def report_stats(self):
        """
        Writes user metrics to file:
        First line: #_completed_workflows #_workflows average_task_throughput
        Rest of the file: workflow_id workflow_makespan workflow_response_time workflow_critical_path
        """

        log_user_metrics = SimUtils.add_file_logging(
            'user_metrics',
            self.config['central_queue']['USER_METRICS_FILENAME'],
            self.config
        )

        workflows = [workflow for workflow in self.workflows.values() if workflow.workflow_completed()]

        log_user_metrics.info('{0} {1} {2}'.format(
            len(workflows),  # number of completed workflows
            len(self.workflows),  # total number of workflows (including not completed)
            self.finished_tasks_count / float(3600)  # average task throughput (tasks/hour)
        ))

        for workflow in workflows:
            log_user_metrics.info('{0} {1} {2} {3}'.format(
                workflow.id,
                (workflow.ts_finish - workflow.ts_start),  # makespan
                (workflow.ts_start - workflow.ts_submit) + (workflow.ts_finish - workflow.ts_start),  # response time
                workflow.critical_path_length,
            ))

    def __str__(self):
        return '{0}: {1}'.format(self.__class__, self.task_queue)

    def __repr__(self):
        return '<CentralQueue object id={0}>'.format(self.id)
