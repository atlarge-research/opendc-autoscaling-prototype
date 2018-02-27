from collections import deque

from autoscalers.Autoscaler import Autoscaler
from core import SimCore, Constants
from core.Task import Task


class PlanAutoscaler(Autoscaler):
    def __init__(self, simulator, logger):
        super(PlanAutoscaler, self).__init__(simulator, 'Plan', logger)

        # will contain one plan per processor
        self.plans = deque(maxlen=self.resource_manager.get_maximum_capacity())

        # simulated finish time
        self.finish_times = {}

    def get_level_of_parallelism(self):
        return sum(1 for processor_plan in self.plans if processor_plan)

    def get_min_processor_plan(self, eligible_plans):
        if not eligible_plans:
            return None

        min_possible_plan = None
        min_finish_time = None

        for processor_plan in eligible_plans:
            if not processor_plan:
                return processor_plan

            plan_finish_time = processor_plan[-1]
            if not min_possible_plan or plan_finish_time < min_finish_time:
                min_possible_plan = processor_plan
                min_finish_time = plan_finish_time

        return min_possible_plan

    def get_eligible_plans(self, max_parent_finish_time):
        eligible_plans = []
        for plan in self.plans:
            if not plan and not max_parent_finish_time:
                eligible_plans.append(plan)
            elif plan:
                # plan contains finish times of it's tasks
                plan_finish_time = plan[-1]
                if plan_finish_time >= max_parent_finish_time:
                    eligible_plans.append(plan)

        return eligible_plans

    def get_max_parent_finish_time(self, task):
        """Gets the critical parent of a task."""

        parent_tasks = task.dependencies
        if not parent_tasks:
            return 0

        critical_parent = 0
        for parent_id in parent_tasks:
            parent_finish_time = self.finish_times.get(parent_id, 0)

            if parent_finish_time > critical_parent:
                critical_parent = parent_finish_time

        return critical_parent

    def place_tasks(self, tasks):
        for task in tasks:
            critical_parent_finish_time = self.get_max_parent_finish_time(task)
            eligible_plans = self.get_eligible_plans(critical_parent_finish_time)

            # gets a reference to the processor plan with the least amount of work
            min_possible_plan = self.get_min_processor_plan(eligible_plans)
            if min_possible_plan == None:
                continue

            min_start_time = min_possible_plan[-1] if min_possible_plan else 0
            if min_start_time >= self.N_TICKS_PER_EVALUATE:
                self.logger.log('Time threshold reached, plan surpasses next autoscaling interval', 'debug')
                return True

            task_runtime = (task.ts_end - self.sim.ts_now) if task.status == Task.STATUS_RUNNING else task.runtime
            task_finish_time = min_start_time + task_runtime

            min_possible_plan.append(task_finish_time)
            self.finish_times[task.id] = task_finish_time

        return False

    def get_entry_tasks(self):
        """Tasks with dependencies that have been met, including running tasks."""

        running_tasks = []
        for site in self.resource_manager.sites:
            running_tasks += site.running_tasks.values()

        return running_tasks + list(self.sim.central_queue.tasks_to_schedule())

    def get_child_tasks(self, tasks):
        child_tasks = []
        for task in tasks:
            child_tasks.extend(task.children)

        return child_tasks

    def predict(self):
        self.plans.clear()
        for _ in xrange(self.plans.maxlen):
            # one plan per processor
            per_processor_plan = deque()
            self.plans.append(per_processor_plan)

        # (re)initialize simulated finish times
        self.finish_times.clear()

        tasks = self.get_entry_tasks()
        while tasks:
            time_threshold_reached = self.place_tasks(tasks)
            if time_threshold_reached:
                break
            tasks = self.get_child_tasks(tasks)

        return self.get_level_of_parallelism()

    def evaluate(self, params):
        super(PlanAutoscaler, self).evaluate(params)

        prediction = self.predict()

        mutation = 0
        current_capacity = self.resource_manager.get_current_capacity()
        target = prediction - current_capacity
        if target > 0:
            self.autoscale_op = 1
            mutation = self.resource_manager.start_up_best_effort(target)
            self.logger.log('Upscaled by {0}, target was {1}'.format(mutation, target))
        elif target < 0:
            self.autoscale_op = -1
            target = abs(target)
            mutation = self.resource_manager.release_resources_best_effort(target)
            self.logger.log('Downscaled by {0}, target was {1}'.format(mutation, target))

        self.log(current_capacity, mutation, target)
        self.refresh_stats(prediction, current_capacity + mutation * self.autoscale_op)

        self.sim.events.enqueue(
            SimCore.Event(
                self.sim.ts_now + self.N_TICKS_PER_EVALUATE,
                self.id,
                self.id,
                {'type': Constants.AUTO_SCALE_EVALUATE}
            )
        )
