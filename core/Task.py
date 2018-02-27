class Task(object):
    """
    Data-holder for a single task.
    Please use the queue_at_site(), run() and stop() methods to move the task through its lifecycle
    instead of manually modifying it's status, running_site, ts_start, ts_end attributes.
    Some attributes are set at task creation, the other ones should be only modified using the
    supplied calls.
    """

    status_count = 4
    STATUS_SUBMITTED, STATUS_QUEUED, STATUS_RUNNING, STATUS_FINISHED = range(status_count)

    def __init__(self, id, ts_submit, submission_site, runtime, cpus, dependencies, workflow_id=None, requirements=None):
        self.id = id
        self.ts_submit = ts_submit
        self.submission_site = submission_site
        self.runtime = max(runtime, 1)
        self.cpus = max(cpus, 1)
        self.dependencies = dependencies
        self.parents = []
        self.children = []
        self.requirements = requirements # none atm

        self.status = Task.STATUS_SUBMITTED
        self.running_site = -1
        self.ts_start = -1
        self.ts_end = -1
        self.workflow_id = workflow_id

    # def __setattr__(self, name, value):
    #     err_msg = None
    #     if name == 'status' and value not in range(self.status_count):
    #         err_msg = 'Invalid range for task status: {0}'.format(value)
    #     elif name in {'ts_submit', 'submission_site', 'runtime', 'cpus', 'running_site', 'ts_start', 'ts_end'} and \
    #         not isinstance(value, int):
    #         err_msg = 'Expected {0} to be of type int, instead got {1}'.format(name, value)
    #     elif name == 'dependencies' and not all(isinstance(dependency, int) for dependency in value):
    #         err_msg = 'Expected {0} to be of type int, instead got {1}'.format(name, value)
    #
    #     if err_msg:
    #         raise ValueError(err_msg)
    #
    #     super(Task, self).__setattr__(name, value)

    def queue_at_site(self, site):
        """Called when the task gets added to a site's queue."""

        self.status = Task.STATUS_QUEUED
        self.running_site = site

    def run(self, ts_start, ts_end):
        """Called when the task is finally ready to run."""

        self.status = Task.STATUS_RUNNING
        self.ts_start = ts_start
        self.ts_end = ts_end

    def interrupt(self):
        """
        Called when task is stopped before finishing execution.
        Reverts to default values for attributes that undergo changes during queuing or execution.
        """

        self.status = Task.STATUS_SUBMITTED
        self.running_site = -1
        self.ts_start = -1
        self.ts_end = -1

    def stop(self):
        """Called when the task is done, can be modfied to return a result or do other validations."""
        self.status = Task.STATUS_FINISHED

    def __str__(self):
        return '{0}: {1}'.format(self.__class__, self.__dict__)

    def __repr__(self):
        return 'Task {0}'.format(self.id)
