# Event types, they values indicate their priority and must be unique.
S2Ss_TASK_DONE = 1                # Frees up resources once a task is done. Must be executed before rescheduling
S2U_TASK_DONE = 2                 # Statistics update related to the S2Ss_TASK_DONE event.
CQ2CQs_MONITOR_SITE_STATUS = 3    # Updates site info, must be run at least after S2U_TASK_DONE
AUTO_SCALE_EVALUATE = 4           # Measures the current queue status and system status, after jobs are done to decide if we need to scale resources
CQ2S_SCHEDULER_AUTORESCHEDULE = 5 # Uses the available resources in the system to schedule tasks
CQ2S_ADD_TASK = 6                 # Processes the events created by CQ2S_SCHEDULER_AUTORESCHEDULE
S2Ss_RESCHEDULE = 7               # Executes the tasks according to the Site policy, must be called after all CQ2S_ADD_TASK have been processed.
SM2SMs_UPDATE_STATISTICS = 8      # Renews site stat statistics, must be done at the end but before the writes.
SM2SMs_MONITOR = 9                # Writes current site stats to the DB. Very related to  S2Ss_MONITOR. Not sure which takes precedence if at all.
S2Ss_MONITOR = 10                 # Writes current state of the system in the DB. Must be done at the end


# Constants related to statusses of objects, need to be unique per object.
STATUS_RUNNING = 1004
STATUS_SHUTDOWN = 1005

