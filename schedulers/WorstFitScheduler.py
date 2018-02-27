from core import SimCore, Constants
from schedulers.Scheduler import Scheduler


class WorstFitScheduler(Scheduler):
    def auto_reschedule(self, params):
        """Assign tasks to free sites (based on info last acquired by the monitor) in a best fit order."""

        self.logger.log('task_queue length is {0}'.format(len(self.central_queue.task_queue)), 'debug')

        self.try_schedule_tasks()

        # If no more tasks to assign, no need to schedule a future event for this
        if not self.central_queue.task_queue and not self.central_queue.ready_tasks:
            return

        next_task_ts = self.central_queue.task_queue[0].ts_submit if self.central_queue.task_queue else None

        if self.central_queue.ready_tasks:
            # The ready task is always earlier than the ones in the task queue, no need to compare.
            next_task_ts = self.central_queue.ready_tasks[0].ts_submit

        if next_task_ts <= self.sim.ts_now + self.N_TICKS_BETWEEN_AUTO_RESCHEDULE:
            next_event_ts = self.sim.ts_now + self.N_TICKS_BETWEEN_AUTO_RESCHEDULE
        else:
            next_event_ts = next_task_ts

        self.events.enqueue(
            SimCore.Event(
                next_event_ts,
                self.id,
                self.id,
                {'type': Constants.CQ2S_SCHEDULER_AUTORESCHEDULE}
            )
        )

    def try_schedule_tasks(self):
        """Only assigns a task if resources for it are available."""

        tasks = self.central_queue.tasks_to_schedule()
        if tasks:
            # if there are tasks to schedule, sorts self.site_stats by free space
            self.central_queue.sort_site_stats()

        # Loop over all sites, starting with the freest site first.
        for task in tasks[:]:
            for index_site, (free_resources, site_name, site_id, is_leased_instance, expiration_ts) in \
                    enumerate(self.central_queue.site_stats):
                self.logger.log('Site {0} has {1} free resources'.format(site_name, free_resources), 'debug')

                # If we have a leased instance and it will expire before this task can complete, do not schedule it
                if is_leased_instance and expiration_ts > 0:
                    if expiration_ts < self.sim.ts_now + task.runtime:
                        continue

                if free_resources < task.cpus:
                    continue

                self.logger.log('Assigning task {0} to site {1}'.format(task.id, site_name), 'debug')
                self.central_queue.submitted_tasks_count += 1
                self.events.enqueue(
                    SimCore.Event(
                        self.sim.ts_now,
                        self.id,
                        site_id,  # task sent to free site
                        {'type': Constants.CQ2S_ADD_TASK, 'task': task}
                    )
                )

                # update knowledge about this site
                free_resources -= task.cpus
                self.central_queue.site_stats[index_site] = (free_resources, site_name, site_id, is_leased_instance, expiration_ts)
                self.central_queue.site_stats_changed = True

                self.central_queue.ready_tasks.remove(task)
                break  # We found a suitable site to submit the task to, so break.
