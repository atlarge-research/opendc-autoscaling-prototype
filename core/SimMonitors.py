import bisect
import operator
import sys

import numpy as np

from core import Constants, SimCore

if "utils" not in sys.path: sys.path.append("utils")
from utils import SimUtils


class CTSiteStatType:
    TASK_ARRIVAL_RATE = 0
    TASK_START_RATE = 1
    TASK_FINISH_RATE = 2
    N_TASKS_ARRIVED = 3
    N_TASKS_STARTED = 4
    N_TASKS_FINISHED = 5
    CPUTIME_RATE = 6  # CPUs per Resource x s
    TOTAL_CPUTIME = 7
    TOTAL_CPUTIME_LRTU = 8


class SiteMonitor(object):
    def __init__(self, site):
        self.site = site

        config = self.site.config
        self.AMOUNT_OF_DAYS_HISTORY = config['site_monitor']['AMOUNT_OF_DAYS_HISTORY']
        self.AMOUNT_OF_MINUTES_TO_TRACK = config['site_monitor']['AMOUNT_OF_MINUTES_TO_TRACK']

        # statistics helper
        self.stats_Total_NTasksIn = 0
        self.stats_Total_NTasksStarted = 0
        self.stats_Total_NTasksFinished = 0
        self.stats_Total_NInterrupted = 0
        self.stats_Total_ConsumedCPUTime = 0  # in CPUs

        # stats for last reporting time interval (LRTU)
        self.stats_LRTU_NTasksIn = 0
        self.stats_LRTU_NTasksStarted = 0
        self.stats_LRTU_NTasksFinished = 0
        self.stats_LRTU_ConsumedCPUTime = 0  # in CPUs

        self.task_arrived_last_minutes = []

        self.tasks_arrival_per_day = {}

        #        #-- wait time
        #        self.stats_LRTU_WaitTime = AIStatistics.CWeightedStats(bIsNumeric = True, bKeepValues = False, bAutoComputeStats = False)   # statistics of wait time in trace
        #        #-- run time
        #        self.stats_LRTU_RunTime = AIStatistics.CWeightedStats(bIsNumeric = True, bKeepValues = False, bAutoComputeStats = False)    # statistics of run time in trace
        #        #-- response time
        #        self.stats_LRTU_ResponseTime = AIStatistics.CWeightedStats(bIsNumeric = True, bKeepValues = False, bAutoComputeStats = False)    # statistics of run time in trace
        #        #-- slowdown
        #        self.stats_LRTU_SlowDown = AIStatistics.CWeightedStats(bIsNumeric = True, bKeepValues = False, bAutoComputeStats = False)    # statistics of slowdown (grid overhead) in trace

    def getRunningTasksConsumedTime_LRTU(self):
        """Returns the time consumed during the last reporting time interval (LRTU) by tasks that have not yet finished."""

        CPU_time = 0
        for task in self.site.running_tasks.values():
            CPU_time += min(self.site.sim.ts_now - task.ts_start, self.site.report_interval) * task.cpus

        return CPU_time

    def getRunningTasksConsumedTime(self):
        """Returns the time consumed by tasks that have not yet finished."""

        CPU_time = 0
        for task in self.site.running_tasks.values():
            CPU_time += (self.site.sim.ts_now - task.ts_start) * task.cpus

        return CPU_time

    def remove_old_tasks_from_arrival_list(self):
        index = bisect.bisect_left(self.task_arrived_last_minutes, self.site.sim.ts_now - self.AMOUNT_OF_MINUTES_TO_TRACK * 60)
        self.task_arrived_last_minutes = self.task_arrived_last_minutes[index:]

    def get_num_tasks_arrived_in_last_minutes(self):
        self.remove_old_tasks_from_arrival_list()
        return len(self.task_arrived_last_minutes)

    def add_arrived_task(self, ts):
        # Remove old timestamps from the list
        self.remove_old_tasks_from_arrival_list()

        # Add this task's ts if it is within the tracking limit
        if ts >= self.site.sim.ts_now - self.AMOUNT_OF_MINUTES_TO_TRACK * 60:
            self.task_arrived_last_minutes.append(ts)

        hour, day = SimUtils.get_hour_and_day_for_ts(ts)

        # Add a list tracking incoming tasks per hour for this day if it isn't set yet.
        if day not in self.tasks_arrival_per_day:
            self.tasks_arrival_per_day[day] = [0] * 24

        self.tasks_arrival_per_day[day][hour] += 1

    def estimate_arrival_for_ts(self, ts, percentile):
        hour, day = SimUtils.get_hour_and_day_for_ts(ts)

        past_arrivals_per_hour = []

        for i in xrange(max(0, day - self.AMOUNT_OF_DAYS_HISTORY), day):
            if i in self.tasks_arrival_per_day:
                past_arrivals_per_hour.append(self.tasks_arrival_per_day[i][hour])

        # return self.tasks_arrival_per_day[hour]
        # Compute the value corresponding to the provided percentile of the number of arrivals
        # If there is no past information,
        return np.percentile(past_arrivals_per_hour, percentile) if len(past_arrivals_per_hour) else 0

    def get_exact_arrivals_for_ts(self, ts):
        hour, day = SimUtils.get_hour_and_day_for_ts(ts)
        # TODO(Laurens) if we have no info on this day, is it safe to return 0?
        return self.tasks_arrival_per_day[day][hour] if day in self.tasks_arrival_per_day else 0

    def run(self):
        """Show what happens in the system."""
        # site.sim.Logger.LogWrite4(None, site.sim.ts_now, "@%.2f" % site.sim.ts_now + " : " + "Site " + site.name + " : " + \
        #      "Resources:" + str(site.resources) +'/' + str(site.used_resources) + "("+\
        #      "%.2f%%" % (100.0 * site.used_resources/site.resources) + ") (All/Free[%])")
        site = self.site
        site.sim.DBStats.addSiteStats(site.sim.ts_now, CTSiteStatType.N_TASKS_ARRIVED, site.id,
                                      ivalue=self.stats_LRTU_NTasksIn)
        site.sim.DBStats.addSiteStats(site.sim.ts_now, CTSiteStatType.TASK_ARRIVAL_RATE, site.id,
                                      fvalue=float(self.stats_LRTU_NTasksIn) / site.report_interval)
        site.sim.DBStats.addSiteStats(site.sim.ts_now, CTSiteStatType.N_TASKS_STARTED, site.id,
                                      ivalue=self.stats_LRTU_NTasksStarted)
        site.sim.DBStats.addSiteStats(site.sim.ts_now, CTSiteStatType.TASK_START_RATE, site.id,
                                      fvalue=float(self.stats_LRTU_NTasksStarted) / site.report_interval)
        site.sim.DBStats.addSiteStats(site.sim.ts_now, CTSiteStatType.N_TASKS_FINISHED, site.id,
                                      ivalue=self.stats_LRTU_NTasksFinished)
        site.sim.DBStats.addSiteStats(site.sim.ts_now, CTSiteStatType.TASK_FINISH_RATE, site.id,
                                      fvalue=float(self.stats_LRTU_NTasksFinished) / site.report_interval)
        site.sim.DBStats.addSiteStats(site.sim.ts_now, CTSiteStatType.TOTAL_CPUTIME, site.id,
                                      ivalue=self.stats_Total_ConsumedCPUTime + self.getRunningTasksConsumedTime())
        itmp = self.getRunningTasksConsumedTime_LRTU()
        site.sim.DBStats.addSiteStats(site.sim.ts_now, CTSiteStatType.TOTAL_CPUTIME_LRTU, site.id,
                                      ivalue=self.stats_LRTU_ConsumedCPUTime + itmp)
        site.sim.DBStats.addSiteStats(site.sim.ts_now, CTSiteStatType.CPUTIME_RATE, site.id,
                                      fvalue=float(self.stats_LRTU_ConsumedCPUTime + itmp) / site.report_interval)


class SystemMonitor(SimCore.SimEntity):
    def __init__(self, simulator, name):
        super(SystemMonitor, self).__init__(simulator, name)
        # overwrite the events map
        self.events_map = {
            Constants.SM2SMs_MONITOR: self.evtMonitor,
            Constants.SM2SMs_UPDATE_STATISTICS: self.refresh_sstats,
        }

        self.report_interval = self.config['site_monitor']['N_TICKS_BETWEEN_MONITORING']
        self.N_TICKS_UPDATE_STATISTICS = self.config['system_monitor']['N_TICKS_UPDATE_STATISTICS']

        self.tasks_in_per_site = {}
        self.tasks_started_per_site = {}
        self.tasks_finished_per_site = {}
        self.tasks_interrupted_per_site = {}
        self.consumed_CPU_time_per_site = {}
        self.running_consumed_CPU_time_per_site = {}

        # sites statistics helper
        self.sstats_Total_NTasksIn = 0
        self.sstats_Total_NTasksStarted = 0
        self.sstats_Total_NTasksFinished = 0
        self.sstats_Total_NTasksInterrupted = 0
        self.sstats_Total_ConsumedCPUTime = 0  # in CPUs
        self.sstats_Total_RunningConsumedCPUTime = 0  # in CPUs

        # site stats for last reporting time interval (LRTU)
        self.sstats_old_NTasksIn = 0
        self.sstats_old_NTasksStarted = 0
        self.sstats_old_NTasksFinished = 0
        self.sstats_old_ConsumedCPUTime = 0  # in CPUs
        self.sstats_old_RunningConsumedCPUTime = 0

    def activate(self):
        # schedule a monitoring event for time=NOW
        self.events.enqueue(
            SimCore.Event(self.sim.ts_now, self.id, self.id, {'type': Constants.SM2SMs_MONITOR}))
        self.events.enqueue(
            SimCore.Event(self.sim.ts_now, self.id, self.id, {'type': Constants.SM2SMs_UPDATE_STATISTICS}))

    def getNTasksToCome(self):
        """Tasks that have not yet been submitted for processing on a site."""
        return len(self.sim.central_queue.task_queue) + len(self.sim.central_queue.ready_tasks)

    def count_tasks_too_large(self):
        if not self.sim.sites:
            return 0
        largest_site = max(self.sim.sites, key=operator.attrgetter('resources'))
        max_resources = largest_site.resources
        return sum(1 for task in self.sim.central_queue.task_queue if task.cpus > max_resources)

    def count_idle_resources(self):
        return sum(site.free_resources for site in self.sim.sites if site.status == Constants.STATUS_RUNNING)

    def get_total_tasks_in(self):
        # self.refresh_sstats(None)
        return self.sstats_Total_NTasksIn

    def get_total_num_incoming_tasks_past_minutes(self):
        total_num_tasks = 0
        for site in self.sim.sites:
            total_num_tasks += site.site_monitor.get_num_tasks_arrived_in_last_minutes()

        return total_num_tasks

    def get_total_load(self):
        total_load = 0
        # add number of running tasks and tasks that have been submitted to central queue
        for site in self.sim.sites:
            total_load += sum(task.cpus for task in site.running_tasks.values())
            total_load += sum(task.cpus for task in site.task_queue)

        total_load += self.get_pending_tasks_load()

        return total_load

    def get_pending_tasks_load(self):
        load = sum(task.cpus for task in self.sim.central_queue.ready_tasks)

        # Also count tasks that are in the queue with dependencies not resolved yet.
        for task in self.sim.central_queue.task_queue:
            if task.ts_submit > self.sim.ts_now:
                break
            load += task.cpus

        return load

    def get_estimated_total_arrival_rate_for_ts(self, ts, percentile):
        estimated_arrival_rate = 0
        for site in self.sim.sites:
            estimated_arrival_rate += site.site_monitor.estimate_arrival_for_ts(ts, percentile)

        return estimated_arrival_rate

    def get_total_predicted_arrivals_for_ts(self, ts, percentile):
        total_predicted = 0
        for site in self.sim.sites:
            total_predicted += site.site_monitor.estimate_arrival_for_ts(ts, percentile)

        return total_predicted

    def get_total_observed_arrivals_for_ts(self, ts):
        total_observed = 0
        for site in self.sim.sites:
            total_observed += site.site_monitor.get_exact_arrivals_for_ts(ts)

        return total_observed

    def refresh_sstats(self, params):
        """Get and sum stats from all sites."""

        # loop over shallow copy of sites as we're going to delete any sites
        # that have STATUS_SHUTDOWN within the resource_manager.drop_site() call
        for site in self.sim.sites[:]:
            site_monitor = site.site_monitor
            site_id = site.id

            self.tasks_in_per_site[site_id] = site_monitor.stats_Total_NTasksIn
            self.tasks_started_per_site[site_id] = site_monitor.stats_Total_NTasksStarted
            self.tasks_finished_per_site[site_id] = site_monitor.stats_Total_NTasksFinished
            self.tasks_interrupted_per_site[site_id] = site_monitor.stats_Total_NInterrupted
            self.consumed_CPU_time_per_site[site_id] = site_monitor.stats_Total_ConsumedCPUTime
            self.running_consumed_CPU_time_per_site[site_id] = site_monitor.getRunningTasksConsumedTime()

            self.sstats_Total_NTasksIn = sum(self.tasks_in_per_site.values())
            self.sstats_Total_NTasksStarted = sum(self.tasks_started_per_site.values())
            self.sstats_Total_NTasksFinished = sum(self.tasks_finished_per_site.values())
            self.sstats_Total_NTasksInterrupted = sum(self.tasks_interrupted_per_site.values())
            self.sstats_Total_ConsumedCPUTime = sum(self.consumed_CPU_time_per_site.values())
            self.sstats_Total_RunningConsumedCPUTime = sum(self.running_consumed_CPU_time_per_site.values())

            if site.status == Constants.STATUS_SHUTDOWN:
                self.sim.resource_manager.drop_site(site)

        # Schedule the next update statistics event
        self.events.enqueue(
            SimCore.Event(self.sim.ts_now + self.N_TICKS_UPDATE_STATISTICS,
                          self.id,
                          self.id,
                          {'type': Constants.SM2SMs_UPDATE_STATISTICS})
        )

    def evtMonitor(self, params):
        """Show what happens in the system."""

        # Renew our knowledge of sstats
        # TODO(Laurens): we now periodically refresh the information already. Do we need this really?
        # self.refresh_sstats(None)

        # report stats
        self.sim.DBStats.addSystemSitesStats(self.sim.ts_now, CTSiteStatType.N_TASKS_ARRIVED,
                                             ivalue=self.sstats_Total_NTasksIn - self.sstats_old_NTasksIn)
        self.sim.DBStats.addSystemSitesStats(self.sim.ts_now, CTSiteStatType.TASK_ARRIVAL_RATE,
                                             fvalue=float(
                                                 self.sstats_Total_NTasksIn - self.sstats_old_NTasksIn) / self.report_interval)
        self.sim.DBStats.addSystemSitesStats(self.sim.ts_now, CTSiteStatType.N_TASKS_STARTED,
                                             ivalue=self.sstats_Total_NTasksStarted - self.sstats_old_NTasksStarted)
        self.sim.DBStats.addSystemSitesStats(self.sim.ts_now, CTSiteStatType.TASK_START_RATE,
                                             fvalue=float(
                                                 self.sstats_Total_NTasksStarted - self.sstats_old_NTasksStarted) / self.report_interval)
        self.sim.DBStats.addSystemSitesStats(self.sim.ts_now, CTSiteStatType.N_TASKS_FINISHED,
                                             ivalue=self.sstats_Total_NTasksFinished - self.sstats_old_NTasksFinished)
        self.sim.DBStats.addSystemSitesStats(self.sim.ts_now, CTSiteStatType.TASK_FINISH_RATE,
                                             fvalue=float(
                                                 self.sstats_Total_NTasksFinished - self.sstats_old_NTasksFinished) / self.report_interval)
        self.sim.DBStats.addSystemSitesStats(self.sim.ts_now, CTSiteStatType.TOTAL_CPUTIME,
                                             ivalue=self.sstats_Total_ConsumedCPUTime + self.sstats_Total_RunningConsumedCPUTime)
        itmp = self.sstats_Total_ConsumedCPUTime + self.sstats_Total_RunningConsumedCPUTime - self.sstats_old_ConsumedCPUTime - self.sstats_old_RunningConsumedCPUTime
        self.sim.DBStats.addSystemSitesStats(self.sim.ts_now, CTSiteStatType.TOTAL_CPUTIME_LRTU,
                                             ivalue=itmp)
        self.sim.DBStats.addSystemSitesStats(self.sim.ts_now, CTSiteStatType.CPUTIME_RATE,
                                             fvalue=float(itmp) / self.report_interval)

        # save as old values -> used in the next reporting time unit (RTU) computation
        self.sstats_old_NTasksIn = self.sstats_Total_NTasksIn
        self.sstats_old_NTasksStarted = self.sstats_Total_NTasksStarted
        self.sstats_old_NTasksFinished = self.sstats_Total_NTasksFinished
        self.sstats_old_ConsumedCPUTime = self.sstats_Total_ConsumedCPUTime  # in CPUs
        self.sstats_old_RunningConsumedCPUTime = self.sstats_Total_RunningConsumedCPUTime

        # check if the system should stop
        # stop condition: no more tasks to submit to sites,
        # all tasks have been submitted (by users), and all tasks have finished (by sites)
        if not self.getNTasksToCome() and \
                        self.sim.central_queue.submitted_tasks_count == self.sim.central_queue.finished_tasks_count:
            self.sim.forced_stop = True

        # schedule another view for over N_REPORT_TICKS
        self.events.enqueue(
            SimCore.Event(self.sim.ts_now + self.report_interval, self.id, self.id,
                          {'type': Constants.SM2SMs_MONITOR}))
