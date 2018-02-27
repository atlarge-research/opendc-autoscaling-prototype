from core.Task import Task


class Workflow(object):
    """
    Data-holder for a single workflow.
    A workflow consists of multiple Tasks (often referred to as tasks in other literature).
    This class offers functionality to reason about a single workflow and its tasks.
    """

    STATUS_SUBMITTED = 0
    STATUS_STARTED = 1
    STATUS_FINISHED = 2
    def __init__(self, id, ts_submit, tasks):
        self.id = id
        self.ts_submit = ts_submit
        self.tasks = tasks

        self.critical_path_length = -1
        self.critical_path_task_count = -1
        self.ts_start = -1
        self.ts_finish = -1
        self.status = Workflow.STATUS_SUBMITTED

    def workflow_started(self):
        return self.status != Workflow.STATUS_SUBMITTED

    def start(self, ts_now):
        if self.status != Workflow.STATUS_SUBMITTED:
            raise Exception('Workflow can be started only if status is STATUS_SUBMITTED')

        self.ts_start = ts_now
        self.status = Workflow.STATUS_STARTED

    def workflow_completed(self):
        if self.status == Workflow.STATUS_FINISHED:
            return True
        
        # Loop over all tasks which have no children, those are exit tasks.
        for task in [task for task in self.tasks if not task.children]:
            if task.status != Task.STATUS_FINISHED:
                return False
        self.status = Workflow.STATUS_FINISHED
        return True

    def __str__(self):
        return '{0}: {1}'.format(self.__class__, self.__dict__)

    def __repr__(self):
        return '{0}: {1}'.format(self.__class__, self.__dict__)
