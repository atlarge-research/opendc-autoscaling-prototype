from itertools import izip

from core import SimCore, Constants
from schedulers.Scheduler import Scheduler


class BestFitScheduler(Scheduler):
    def auto_reschedule(self, params):
        """Assign tasks to free sites (based on info last acquired by the monitor) in a best fit order."""

        #self.logger.log('task_queue length is {0}'.format(len(self.central_queue.task_queue)), 'debug')

        self.try_schedule_tasks()

        # If no more tasks to assign, no need to schedule a future event for this component
        if not self.central_queue.has_remaining_tasks:
            return

        # Get the timestamp of the next task to be scheduled
        next_task_ts = self.central_queue.ts_of_next_task

        # Compute the timestamp of the next scheduling event, at least
        # N_TICKS_BETWEEN_AUTO_RESCHEDULE in the future
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

        for task in tasks[:]:
            # If the task does not fit the total available resources,
            # we do not need to check each site
            if task.cpus > self.central_queue.total_available_resources:
                # If there are no available resources, we do not need to check
                # other tasks either
                if self.central_queue.total_available_resources == 0:
                    break
                continue

            # Find the first site with the least amount of resources >= the size of our task
            sorted_sites = self.central_queue.site_stats_by_ascending_free_resources
            start_index = sorted_sites.bisect_left((-1, (task.cpus,)))

            # Loop through all viable sites in order to find the first one we can use
            for site_index, (free_resources, site_name, site_id, is_leased_instance, expiration_ts) in \
                    sorted_sites.islice(start_index):
                # If we have a leased instance and it will expire before this task can complete, do not schedule it
                if is_leased_instance and expiration_ts > 0:
                    if expiration_ts < self.sim.ts_now + task.runtime:
                        continue
                
                # Assign the task to this site
                self.central_queue.submitted_tasks_count += 1
                self.central_queue.remove_task_to_schedule(task)
                self.events.enqueue(
                    SimCore.Event(
                        self.sim.ts_now,
                        self.id,
                        site_id,  # task sent to free site
                        {'type': Constants.CQ2S_ADD_TASK, 'task': task}
                    )
                )

                # Update the site's free resource count
                self.central_queue.set_site_free_resources(site_index, free_resources - task.cpus)

                # We found a suitable site to submit the task to, so break
                break
