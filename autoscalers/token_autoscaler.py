import math
from collections import deque

from autoscalers.Autoscaler import Autoscaler
from core import SimCore, Constants
from core.Task import Task


class TokenAutoscaler(Autoscaler):
    last_prediction = 0
    upward_ranks = {}
    critical_paths = {}

    def __init__(self, simulator, logger):
        super(TokenAutoscaler, self).__init__(simulator, 'Token', logger)

        self.time_threshold = self.config['autoscaler']['TOKEN_TIME_THRESHOLD']
        self.max_capacity = self.config['autoscaler']['TOKEN_MAX_CAPACITY']  # maximum amount of cores we can allocate

    def evaluate(self, params):
        super(TokenAutoscaler, self).evaluate(params)
        self.logger.log('Starting token autoscaling process')

        self.calculate_critical_paths()

        prediction = 0
        workflows = [workflow for workflow in self.sim.central_queue.workflows.values() if not workflow.workflow_completed()]
        for workflow in workflows:
            critical_path = self.critical_paths[workflow]
            critical_path_duration = 0
            critial_path_length = len(critical_path)
            for duration in critical_path:
                critical_path_duration += duration

            depth = int(math.ceil((self.time_threshold * critial_path_length) / float(critical_path_duration)))
            lop = self.estimate_lop(workflow, depth)
            prediction += lop
            if prediction >= self.max_capacity:
                break

        current_capacity = self.resource_manager.get_current_capacity()
        prediction -= current_capacity

        mutation = 0
        if prediction < 0:
            self.autoscale_op = -1
            mutation = self.resource_manager.release_resources_best_effort(abs(prediction))
        elif prediction > 0:
            self.autoscale_op = 1
            mutation = self.resource_manager.start_up_best_effort(prediction)

        self.log(current_capacity, mutation, abs(prediction))
        self.refresh_stats(current_capacity + prediction, current_capacity + mutation * self.autoscale_op)

        self.sim.events.enqueue(
            SimCore.Event(
                self.sim.ts_now + self.N_TICKS_PER_EVALUATE,
                self.id,
                self.id,
                {'type': Constants.AUTO_SCALE_EVALUATE}
            )
        )

    def calculate_critical_paths(self):
        workflows = [workflow for workflow in self.sim.central_queue.workflows.values() if not workflow.workflow_completed()]

        if not workflows:
            return

        new_upward_ranks = {}
        new_critical_paths = {}

        for workflow in workflows:
            if workflow in self.upward_ranks and workflow in self.critical_paths:
                new_upward_ranks[workflow] = self.upward_ranks[workflow]
                new_critical_paths[workflow] = self.critical_paths[workflow]
            else:
                task_upward_ranks = {}
                new_upward_ranks[workflow] = task_upward_ranks

                exit_tasks = self.get_exit_tasks(workflow)
                for task in exit_tasks:
                    self.compute_upward_ranks(task, task_upward_ranks)

                workflow_critical_path = self.get_critical_path(workflow, task_upward_ranks)
                new_critical_paths[workflow] = workflow_critical_path

        self.upward_ranks = new_upward_ranks
        self.critical_paths = new_critical_paths

    def get_exit_tasks(self, workflow):
        exit_tasks = deque()

        tasks = workflow.tasks

        for task in tasks:
            child_tasks = task.children
            if not child_tasks:
                exit_tasks.append(task)

        return exit_tasks

    def estimate_lop(self, workflow, depth):
        visited_nodes = []
        tokenized_nodes = self.get_entry_tasks(workflow)
        lop = len(tokenized_nodes)
        for i in xrange(0, depth):
            new_tokenized_nodes = []
            for task in tokenized_nodes:
                if task.children:
                    for child in task.children:
                        if self.all_parents_are_tokenized_or_visited(child, visited_nodes, tokenized_nodes):
                            # Place the token
                            if child not in new_tokenized_nodes:
                                new_tokenized_nodes.append(child)
                            # Mark the parent as visited
                            if task not in visited_nodes:
                                visited_nodes.append(task)
                        else:
                            # Otherwise keep the token in the same place
                            if task not in new_tokenized_nodes:
                                new_tokenized_nodes.append(task)

            tokenized_nodes = new_tokenized_nodes
            if not tokenized_nodes:
                break
            if len(tokenized_nodes) > lop:
                lop = len(tokenized_nodes)

        return lop

    def compute_upward_ranks(self, task, task_upward_ranks):
        max_child_upward_rank = 0

        if task.children:
            max_child_upward_rank = self.get_max_child_upward_rank(task, task_upward_ranks)

        task_upward_ranks[task] = task.runtime + max_child_upward_rank
        if task.dependencies:
            for parent in task.parents:
                self.compute_upward_ranks(parent, task_upward_ranks)

    def get_critical_path(self, workflow, task_upward_ranks):
        critical_path = deque()

        max_upward_rank_task = None
        child_tasks = self.get_entry_tasks(workflow)
        while child_tasks:
            for child_task in child_tasks:
                if not max_upward_rank_task:
                    max_upward_rank_task = child_task
                else:
                    max_entry_task_upward_rank = task_upward_ranks[max_upward_rank_task]
                    entry_task_upward_rank = task_upward_ranks[child_task]
                    if entry_task_upward_rank > max_entry_task_upward_rank:
                        max_upward_rank_task = child_task

            critical_path.append(max_upward_rank_task.runtime)
            child_tasks = max_upward_rank_task.children
            max_upward_rank_task = None

        return critical_path

    def get_max_child_upward_rank(self, task, task_upward_ranks):
        max_child_upward_rank = 0
        for child in task.children:
            if child in task_upward_ranks:
                upward_rank = task_upward_ranks[child]
                if upward_rank > max_child_upward_rank:
                    max_child_upward_rank = upward_rank

        return max_child_upward_rank

    def get_entry_tasks(self, workflow):
        entry_tasks = deque([])
        tasks = workflow.tasks

        for task in tasks:
            status = task.status
            if status is Task.STATUS_FINISHED:
                continue

            parent_tasks = task.dependencies
            if not parent_tasks:
                entry_tasks.append(task)
            else:
                all_parents_done = True
                for parent in task.parents:
                    if parent.status is not Task.STATUS_FINISHED:
                        all_parents_done = False
                        break
                if all_parents_done:
                    entry_tasks.append(task)

        return entry_tasks

    def all_parents_are_tokenized_or_visited(self, task, visited_nodes, tokenized_nodes):
        parents = task.parents
        if not parents:
            return True

        for parent in parents:
            if parent not in tokenized_nodes and parent not in visited_nodes:
                return False

        return True
