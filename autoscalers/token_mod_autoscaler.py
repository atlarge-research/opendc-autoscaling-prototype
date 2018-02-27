import math
from collections import deque

from autoscalers.Autoscaler import Autoscaler
from core import SimCore, Constants
from core.Task import Task
from utils import SimUtils


class TokenModAutoscaler(Autoscaler):
    """
    Modified version of Token autoscaler; differs from author's implementation in how
    it obtaines workflow critical path (uses already computed workflow critical path length
    and task count).
    """

    def __init__(self, simulator, logger):
        super(TokenModAutoscaler, self).__init__(simulator, 'Token', logger)

        self.time_threshold = self.config['autoscaler']['TOKEN_TIME_THRESHOLD']

    def evaluate(self, params):
        super(TokenModAutoscaler, self).evaluate(params)
        self.logger.log('Starting token autoscaling process')

        prediction = 0
        workflows = [workflow for workflow in self.sim.central_queue.workflows.values() if not workflow.workflow_completed()]
        for workflow in workflows:
            critical_path_length = workflow.critical_path_length
            critial_path_task_count = workflow.critical_path_task_count

            depth = int(math.ceil((self.time_threshold * critial_path_task_count) / float(critical_path_length)))
            lop = self.estimate_lop(workflow, depth)
            prediction += lop

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
