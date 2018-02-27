import os

from core import SimCore, Constants
from core.SimLogger import DBLogger
from core.SimMonitors import SiteMonitor
from utils import SimUtils


class Site(SimCore.SimEntity):
    """
    Site -- implement the functionality of a site that monitors itself
    + activate: start the monitoring event
    + events: 'add a task', 'reschedule queued tasks', 'a task just finished',
              'monitor' (report status)
    """

    def __init__(self, simulator, name, resources, resource_speed, leased_instance=False):
        super(Site, self).__init__(simulator, name)

        output = SimUtils.get_output(self.config)
        DBLog_path = os.path.join(output, self.config['simulation']['DBLog'])
        self.logger = DBLogger(sim=self.sim, DBName=DBLog_path, BufferSize=10000)

        self.resources = resources
        self.resource_speed = resource_speed
        self.used_resources = 0
        self.task_queue = []

        self.report_interval = self.config['site_monitor']['N_TICKS_BETWEEN_MONITORING']

        self.leased_instance = leased_instance  # Shows if an instance is leased using e.g. autoscaling
        self.expiration_ts = 0  # Used to simulate leased resources, like an AWS instance for one hour

        self.events_map = {
            Constants.CQ2S_ADD_TASK: self.add_task,
            Constants.S2Ss_RESCHEDULE: self.reschedule,
            Constants.S2Ss_TASK_DONE: self.finish_task,
            Constants.S2Ss_MONITOR: self.monitor,
        }

        self.status = Constants.STATUS_RUNNING

        self.running_tasks = {}

        self.site_monitor = SiteMonitor(self)

    @property
    def expired(self):
        return 0 <= self.expiration_ts <= self.sim.ts_now

    @property
    def free_resources(self):
        return self.resources - self.used_resources

    @free_resources.setter
    def free_resources(self, value):
        self.used_resources -= value

    def is_idle(self):
        """An idle site can be easily shutdown, it has no running task and no tasks to process."""

        return not self.running_tasks and not self.task_queue

    def activate(self):
        """Schedule a monitoring event for time=NOW."""

        self.events.enqueue(
            SimCore.Event(
                self.sim.ts_now,
                self.id,
                self.id,
                {'type': Constants.S2Ss_MONITOR}
            )
        )

    def dispatch(self, event):
        """Stop receiving events if it was shutdown."""

        if self.status == Constants.STATUS_RUNNING:
            super(Site, self).dispatch(event)

    def monitor(self, params):
        self.site_monitor.run()

        # -- schedule another view for over N_TICKS_BETWEEN_MONITORING
        self.events.enqueue(
            SimCore.Event(
                self.sim.ts_now + self.report_interval,
                self.id,
                self.id,
                {'type': Constants.S2Ss_MONITOR}
            )
        )

    def add_task(self, params):
        """At the moment, tasks accepted no matter what."""

        self.site_monitor.stats_Total_NTasksIn += 1
        self.site_monitor.stats_LRTU_NTasksIn += 1
        self.site_monitor.add_arrived_task(int(self.sim.ts_now))

        task = params['task']
        task.queue_at_site(self.id)

        self.task_queue.append(task)

        self.events.enqueue(
            SimCore.Event(
                self.sim.ts_now,
                self.id,
                self.id,
                {'type': Constants.S2Ss_RESCHEDULE}
            )
        )

    def reschedule(self, params):
        """Uses a FCFS policy."""

        self.logger.log('Length of local task_queue is {0}'.format(len(self.task_queue)), 'debug')

        while self.task_queue and self.task_queue[0].cpus <= self.free_resources:
            self.site_monitor.stats_Total_NTasksStarted += 1
            self.site_monitor.stats_LRTU_NTasksStarted += 1

            task = self.task_queue.pop(0)

            # allocate resource(s)
            self.used_resources += task.cpus

            # fixed processing duration (homogeneous processing speeds)
            iRunTime = int(task.runtime / self.resource_speed)
            if task.runtime > iRunTime * self.resource_speed:
                iRunTime += 1
            task.run(self.sim.ts_now, self.sim.ts_now + iRunTime)

            self.running_tasks[self.site_monitor.stats_Total_NTasksStarted] = task

            self.logger.log('Task {0} of {1} started (duration={2}, ts_end={3})'.format(
                task.id, task.submission_site, task.runtime, task.ts_end), 'debug')

            self.events.enqueue(
                SimCore.Event(
                    task.ts_end,
                    self.id,
                    self.id,
                    {
                        'type': Constants.S2Ss_TASK_DONE,
                        'running_task_index': self.site_monitor.stats_Total_NTasksStarted
                    }
                )
            )

    def finish_task(self, params):
        task_index = params['running_task_index']
        task = self.running_tasks[task_index]

        task.stop()

        self.used_resources -= task.cpus
        del self.running_tasks[task_index]

        # -- compute overall stats
        self.site_monitor.stats_Total_NTasksFinished += 1
        self.site_monitor.stats_Total_ConsumedCPUTime += (self.sim.ts_now - task.ts_start) * task.cpus
        # -- compute last reporting time interval (LRTU) stats
        self.site_monitor.stats_LRTU_NTasksFinished += 1
        self.site_monitor.stats_LRTU_ConsumedCPUTime += min(self.sim.ts_now - task.ts_start,
                                                            self.report_interval) * task.cpus

        self.sim.DBTasksDoneTrace.addFinishedTask(
            task.submission_site, task.running_site, task.submission_site, task.ts_submit,
            task.ts_start, task.ts_end, 0, task.cpus,
            '%d/%s' % (self.id, self.name)
        )
        self.logger.log('Task {0} of {1} finished'.format(
            task.id, task.submission_site), 'debug')
        # write task finished in task trace
        # logger.db('JOB\t{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}'.format(
        #    task.id, task.owner, task.site,
        #    task.ts_submit, task.ts_start, task.ts_stop,
        #    task.status, task.result))

        # tell task owner the task was done
        self.events.enqueue(
            SimCore.Event(
                self.sim.ts_now,
                self.id,
                self.sim.central_queue.id,
                {
                    'type': Constants.S2U_TASK_DONE,
                    'task': task
                }
            )
        )

        # each task departure triggers a scheduling event
        self.events.enqueue(
            SimCore.Event(
                self.sim.ts_now,
                self.id,
                self.id,
                {'type': Constants.S2Ss_RESCHEDULE}
            )
        )

    def shutdown(self):
        """Prepares site to be shutdown: transfers running and queued tasks back Central Queue."""

        self.status = Constants.STATUS_SHUTDOWN

        if self.is_idle():
            return

        for task in self.running_tasks.values():
            task.interrupt()
            self.site_monitor.stats_Total_NInterrupted += 1

        self.sim.central_queue.extend_task_list(self.running_tasks.values())

        for task in self.task_queue:
            task.interrupt()

        self.sim.central_queue.extend_task_list(self.task_queue)
        self.used_resources = 0

    def __str__(self):
        return '{0}: {1}'.format(self.__class__, self.__dict__)

    def __repr__(self):
        return '<Site object id={0}>'.format(self.id)
