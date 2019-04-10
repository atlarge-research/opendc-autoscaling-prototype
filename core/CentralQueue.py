import os

import toposort
from sortedcontainers import SortedListWithKey

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
        self.workflows = {}

        self.events_map = {
            Constants.CQ2CQs_MONITOR_SITE_STATUS: self.monitor_sites,
            Constants.S2U_TASK_DONE: self.task_done,
        }

        # Keep three task queues to track tasks in various conditions:
        # - Tasks that are not eligible for execution due to unmet dependencies
        # TODO: This queue can be replaced with a counter if the scheduler
        #       is invoked everytime a task completion moves a new task to the
        #       ready queue. Currently the scheduler needs to know when the
        #       next task *might* start once its dependencies are met, which
        #       may be before ts_now and thus cause many scheduling events
        #       despite a lack of updates to the global state.
        #       The current implementation is not event-driven!
        self._tasks_pending_dependencies = SortedListWithKey(
            key=lambda task: task.ts_submit)
        # - Tasks that are not eligible for execution because they have not
        #   yet been submitted
        self._tasks_submitted_after_now = SortedListWithKey(
            key=lambda task: task.ts_submit)
        # - Tasks that are ready for execution
        self._ready_tasks = SortedListWithKey(key=lambda task: task.ts_submit)

        # Each site stat is a 5-tuple of (free_resources, site_name,
        # site_id, is_leased_instance, expiration_ts)
        # The list of site stats is ordered by time of adding the site
        self._site_stats = []
        self._site_id_index_map = dict()
        # Another list is maintained sorted by (free_resources,
        # site_index) for schedulers that require this sorting
        self._site_stats_sorted = SortedListWithKey(key = lambda idx_site_stat: (idx_site_stat[1][0], idx_site_stat[0]))

        self.logger.log_and_db('CentralQueue initialized', 'debug')

    def set_task_list(self, task_list, first_submission_at_zero=True):
        """Set initial list of tasks."""

        self.logger.log_and_db('Assign tasks starts')

        # in case the first_submission_at_zero flag is set, first task will have
        # ts_submit=0; the rest of the tasks will follow by having their initial
        # ts_submit substracted with the first's task ts_submit
        if task_list and first_submission_at_zero:
            first_ts_submit = task_list[0].ts_submit
            for task in task_list:
                task.ts_submit = max(task.ts_submit - first_ts_submit, 0)

        # Create separate lists of tasks with pending dependencies and tasks
        # with fulfilled dependencies
        for task in task_list:
            if not task.dependencies:
                self._tasks_submitted_after_now.add(task)
            else:
                self._tasks_pending_dependencies.add(task)

        # logger.log('Task list:\n{0}\nFirstSubmitAtZero: {1}'.format(
        #     pprint.pformat(self.task_queue),
        #     first_submission_at_zero), 'debug'
        # )

        # self.sort_by_dependencies()

        self.logger.db('CentralQueue got {0} tasks'.format(len(task_list)))
        self.logger.db('Assign tasks ends')
        self.logger.flush()

    def set_workflow_dict(self, workflows):
        self.workflows = workflows

    def extend_task_list(self, tasks):
        """Used to resubmit tasks that have been interrupted."""

        self.submitted_tasks_count -= len(tasks)
        for task in tasks:
            if not task.dependencies:
                self._tasks_submitted_after_now.add(task)
            else:
                self._tasks_pending_dependencies.add(task)

    def activate(self):
        """First monitor sites, then reschedule tasks."""

        self.events.enqueue(
            SimCore.Event(self.sim.ts_now, self.id, self.id, {'type': Constants.CQ2CQs_MONITOR_SITE_STATUS}))

    def monitor_sites(self, params):
        """Get monitoring information from existing sites: read queue length."""

        self.total_available_resources = 0
        for site in self.sim.sites:
            if site.status == Constants.STATUS_SHUTDOWN:
                if site.id in self._site_id_index_map:
                    self.remove_site_stats(site.id)
                continue
            
            new_site_free_resources = site.free_resources - sum(task.cpus for task in site.task_queue)
            self.total_available_resources += new_site_free_resources

            site_index = self._site_id_index_map[site.id]
            self.set_site_free_resources(site_index, new_site_free_resources)

        # schedule the next monitoring event
        self.events.enqueue(
            SimCore.Event(self.sim.ts_now + self.N_TICKS_MONITOR_SITE_STATUS, self.id, self.id,
                          {'type': Constants.CQ2CQs_MONITOR_SITE_STATUS}))

    def add_site_stats(self, site):
        site_free_resources = site.free_resources - sum(task.cpus for task in site.task_queue)
        self.total_available_resources += site_free_resources
        
        new_site_stat = (
            site_free_resources,
            site.name,
            site.id,
            site.leased_instance,
            site.expiration_ts
        )
        self._site_id_index_map[site.id] = len(self._site_stats)
        self._site_stats_sorted.add((len(self._site_stats), new_site_stat))
        self._site_stats.append(new_site_stat)        

    def remove_site_stats(self, site_id):
        if not site_id in self._site_id_index_map:
            return
        site_index = self._site_id_index_map[site_id]
        site_stat = self._site_stats[site_index]
        self.total_available_resources -= site_stat[0]
        del self._site_id_index_map[site_id]
        del self._site_stats[site_index]
        self._site_stats_sorted.remove((site_index, site_stat))
        for new_index, st in enumerate(self._site_stats[site_index:], site_index):
            old_index = new_index + 1
            self._site_id_index_map[st[2]] = new_index
            self._site_stats_sorted.remove((old_index, st))
            self._site_stats_sorted.add((new_index, st))
    
    @property
    def site_stats(self):
        """
        Returns a list of site statistics as (free_resources, site_name,
        site_id, is_leased_instance, expiration_ts) tuples.
        """
        return self._site_stats
    
    @property
    def site_stats_by_ascending_free_resources(self):
        """
        Returns a SortedList of site indices and statistics as (index,
        statistics) tuples sorted by the free_resources field in ascending
        order. The statistics are tuples as desribed in
        CentralQueue.site_stats().
        """
        return self._site_stats_sorted

    def set_site_free_resources(self, site_index, new_site_free_resources):
        """Update free_resources value of the site at the given index"""
        last_site_stat = self._site_stats[site_index]
        if last_site_stat[0] == new_site_free_resources:
            return
        
        new_site_stat = (
            new_site_free_resources,
            last_site_stat[1],
            last_site_stat[2],
            last_site_stat[3],
            last_site_stat[4]
        )

        self._site_stats[site_index] = new_site_stat
        self._site_stats_sorted.remove((site_index, last_site_stat))
        self._site_stats_sorted.add((site_index, new_site_stat))

    def _check_tasks_submitted_after_now(self):
        # Check if there are new eligible tasks to be moved to the ready queue
        new_ready_tasks = []
        for task in self._tasks_submitted_after_now:
            # Stop when we find a task past the current time
            if task.ts_submit > self.sim.ts_now:
                break
            new_ready_tasks.append(task)

        # Remove ready tasks from the pending queue and insert them into the
        # ready queue
        for task in new_ready_tasks:
            self._tasks_submitted_after_now.remove(task)
            self._ready_tasks.add(task)

    def tasks_to_schedule(self):
        """
        Returns a list of tasks ready to be scheduled (they have all their dependencies met and ts_submit <= ts_now).
        """

        # Check if there are new eligible tasks to be moved to the ready queue
        self._check_tasks_submitted_after_now()

        return self._ready_tasks

    def remove_task_to_schedule(self, task):
        self._ready_tasks.remove(task)

    def try_schedule_tasks(self):
        """
        Override this function with your scheduling (allocation) logic.
        """
        pass

    @property
    def has_remaining_tasks(self):
        return self._tasks_pending_dependencies \
            or self._tasks_submitted_after_now \
            or self._ready_tasks

    @property
    def number_of_remaining_tasks(self):
        return len(self._tasks_pending_dependencies) + \
            len(self._tasks_submitted_after_now) + \
            len(self._ready_tasks)

    def count_tasks_above_resource_limit(self, limit):
        count = sum(1 for task in self._tasks_pending_dependencies if task.cpus > limit)
        count += sum(1 for task in self._tasks_submitted_after_now if task.cpus > limit)
        count += sum(1 for task in self._ready_tasks if task.cpus > limit)
        return count

    def compute_pending_task_load(self):
        # Move tasks to ready queue to ensure we count all eligible tasks
        self._check_tasks_submitted_after_now()

        load = sum(task.cpus for task in self._ready_tasks)

        # Also count tasks that are in the queue with dependencies not resolved yet.
        for task in self._tasks_pending_dependencies:
            if task.ts_submit > self.sim.ts_now:
                break
            load += task.cpus

        return load

    @property
    def ts_of_next_task(self):
        next_ts = None
        if self._ready_tasks:
            next_ts = self._ready_tasks[0].ts_submit
        if self._tasks_submitted_after_now:
            ts = self._tasks_submitted_after_now[0].ts_submit
            next_ts = min(ts, next_ts) if next_ts is not None else ts
        if self._tasks_pending_dependencies:
            ts = self._tasks_pending_dependencies[0].ts_submit
            next_ts = min(ts, next_ts) if next_ts is not None else ts
        return next_ts

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
                    # Check if the child has any remaining dependencies
                    # If not, the child is moved to the next task queue
                    if not child.dependencies:
                        self._tasks_pending_dependencies.remove(child)
                        self._tasks_submitted_after_now.add(child)

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
        return '{0}'.format(self.__class__)

    def __repr__(self):
        return '<CentralQueue object id={0}>'.format(self.id)
