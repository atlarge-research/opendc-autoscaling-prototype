"""
Simulation of the meta-scheduler computing model.

There are n clusters, each with its own number of resources.
Resources in a cluster have the same processing speed; different
clusters may have independent processing speeds. Users submit all
their tasks to a central queue. A central (meta-)scheduler dispatches
tasks on the sites with free resources. Tasks stay in the queue until
free resources are found. The information about the number of free
resources is gathered periodically by a monitoring service.

In practice, a task may arrive on a site previously considered free
after the site has been occupied (by another, competing, scheduler,
or by a local task, if it exists). In this simulation such situations
do not occur (hence, the performance of the simulated system is higher
than the performance of the real system).

Pros:
    + task push means that the central point decides when to release the tasks
    + local usage policies easy to implement
Cons:
    - central point of failure (central queue)
    - bottleneck (central queue)
    - all users known by all participating sites

Usage:
  SystemSim.py [--quiet | --verbose] [-o FILE]
  SystemSim.py <config_filename> [--quiet | --verbose] [-o FILE]
  SystemSim.py <N_TICKS> --GWF=<file_or_folder> [--N=<clusters>] [--quiet | --verbose] [-o FILE]
  SystemSim.py -h | --help

Examples:
  SystemSim.py                             # Uses the 'default_config.ini'
  SystemSim.py conf                        # Uses config file 'conf'
  SystemSim.py 86400 --GWF=test1.gwf       # Uses default settings with N_TICKS=86400 and applies test1.gwf
                                           # to ClusterSetup.txt
  SystemSim.py 86400 --GWF=test2.gwf --N 5 # Uses default settings with N_TICKS=86400 and applies test2.gwf
                                           # to the first 5 clusters defined in ClusterSetup.txt

Options:
  --GWF=<file_or_folder>  A ./gwf/.gwf workflow file applied to clusters or a folder with .gwf files inside ./gwf/
  --N=<clusters>          Apply the workflow to the first N clusters defined in ClusterSetup.txt
  -v --verbose            Enable simulator debug logging on stdout
  -q --quiet              Silence simulator output on stdout
  -o FILE                 Save simulator output to file
  -h --help               Show this screen.
"""

import datetime
import logging
import os
import pprint
import sys
import time

# This needs to be here, else it cannot find root level files when importing them.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from docopt import docopt

import autoscalers
import ProjectUtils
import SimCore
from core import Site, Constants
from core.CentralQueue import CentralQueue
from core.SimLogger import DBStats, DBTaskTrace, DBLogger, setup_logging, cleanup_logging
from core.SimMonitors import SystemMonitor
from core.SimResourceManager import ResourceManager
from schedulers import get_scheduler_by_name
from utils import AIStatistics, SimUtils


if "utils" not in sys.path: sys.path.append("utils")

config_schema = '''
    [experiment]
    ID                           = string(default='')
    ITERATION                    = string(default='')

    [simulation]
    N_TICKS                      = integer(min=1)
    OUTPUT_DIR                   = string(default='output')
    RUNTIME_OUTPUT_FILENAME      = string(default='runtime.out')
    DBLog                        = string(default='log.db3')
    DBLoggingEnabled             = boolean(default=True)
    DBStats                      = string(default='stats.db3')
    DBTasksDoneTrace             = string(default='tasksdone.db3')
    DBTasksInTrace               = string(default='tasksin.db3')
    ClusterSetup                 = string(default='clustersetup.csv')
    Autoscaler                   = string(default='')
    Scheduler                    = string(default='fillworstfit')

    [central_queue]
    N_TICKS_MONITOR_SITE_STATUS  = integer(default=5)
    USER_METRICS_FILENAME        = string(default='user_metrics.log')

    [autoscaler]
    OPS_FILENAME                 = string(default='autoscaler.log')
    ELASTICITY_METRICS_FILENAME  = string(default='elasticity_metrics.log')
    COST_METRICS_FILENAME        = string(default='cost_metrics.log')
    ELASTICITY_OVERVIEW_FILENAME = string(default='elasticity_overview.log')
    N_TICKS_PER_EVALUATE         = integer(default=30)
    HIST_PERCENTILE              = float(default=0.9)
    TOKEN_TIME_THRESHOLD         = integer(default=30)
    TOKEN_MAX_CAPACITY           = integer(default=500)
    SERVER_SPEED                 = float(default=1.0)

    [site_monitor]
    N_TICKS_BETWEEN_MONITORING  = integer(default=1)
    AMOUNT_OF_DAYS_HISTORY      = integer(default=3)
    AMOUNT_OF_MINUTES_TO_TRACK  = integer(default=5)

    [system_monitor]
    N_TICKS_UPDATE_STATISTICS   = integer(default=1)
'''.strip().splitlines()


class SystemSim(SimCore.CSimulation):
    def __init__(self, config):
        self.config = config
        self.output = SimUtils.get_output(config)

        simulation_config = self.config['simulation']
        DBLog_path = os.path.join(self.output, simulation_config['DBLog'])
        DBStats_path = os.path.join(self.output, simulation_config['DBStats'])
        DBTasksDoneTrace_path = os.path.join(self.output, simulation_config['DBTasksDoneTrace'])
        DBTasksInTrace_path = os.path.join(self.output, simulation_config['DBTasksInTrace'])
        RuntimeOutput_path = os.path.join(self.output, simulation_config['RUNTIME_OUTPUT_FILENAME'])

        # write all runtime output to file
        self.runtime_handler = setup_logging(logging.FileHandler(RuntimeOutput_path, 'w+'), logging_level=logging.DEBUG)

        self.DBStats = DBStats(DBName=DBStats_path, BufferSize=10000)
        self.DBTasksDoneTrace = DBTaskTrace(DBName=DBTasksDoneTrace_path, BufferSize=10000)
        self.DBTasksInTrace = DBTaskTrace(DBName=DBTasksInTrace_path, BufferSize=10000)

        self.logger = DBLogger(sim=self, DBName=DBLog_path, BufferSize=10000)
        super(SystemSim, self).__init__(config=self.config)

    def log_tasks_in(self, workflows, tasks):
        workflows_in = os.path.join(SimUtils.get_output(self.config), 'workflows.in')
        tasks_in = os.path.join(SimUtils.get_output(self.config), 'tasks.in')

        with open(workflows_in, 'w') as f:
            pprint.pprint(workflows, stream=f)

        with open(tasks_in, 'w') as f:
            pprint.pprint([str(task) for task in tasks], stream=f)

        # [SubmitTime, TaskNumber, NCPUs, RunTime, SubmissionSite, True]
        for task in tasks:
            self.DBTasksInTrace.addFinishedTask(
                task.submission_site,
                0,
                0,
                task.ts_submit,
                0,
                task.runtime,
                0,
                task.cpus,
                None
            )
        self.logger.db('Saved %d tasks.' % (len(tasks)))
        self.DBTasksInTrace.flush()

    def setup(self):
        """
        Launches the site entities and assigns them workload.

        1. Read cluster setup from config['simulation']['ClusterSetup'].
        2. Use only first N_CLUSTERS sites, if the flag is set.
        3. Start sites.
        4. Check if the GWF in config is set. It overwrites gwfs from
        ClusterSetup.csv
        5. Map tasks from GWF to sites.
        """

        dt_start = datetime.datetime.now()

        self.forced_stop = False
        self.cycle_messages_count = {}

        self.logger.log('\n{0}'.format(pprint.pformat(self.config.dict(), indent=2)))
        simulation_config = self.config['simulation']

        # get names and number of sites
        cluster_setup_path = os.path.join(ProjectUtils.root_path, simulation_config['ClusterSetup'])
        cluster_setup, gwf_filenames = SimUtils.read_cluster_setup(cluster_setup_path)

        if simulation_config.get('N_CLUSTERS'):
            cluster_setup = cluster_setup[
                            :int(simulation_config['N_CLUSTERS'])]  # use only the first N_CLUSTERS clusters

        # create a unique system entry point
        self.central_queue = CentralQueue(
            simulator=self,
            name='CQ',
        )
        self.resource_manager = ResourceManager(
            logger=self.logger,
            simulator=self,
            SiteClass=Site.Site,
            cluster_setup=cluster_setup,
        )

        self.sites = self.resource_manager.sites

        self.system_monitor = SystemMonitor(
            simulator=self,
            name='SystemMonitor',
        )

        autoScalerClass = autoscalers.get_autoscaler_by_name(simulation_config.get('Autoscaler'))
        self.autoscaler = autoScalerClass(simulator=self, logger=self.logger) if autoScalerClass else None

        scheduler_class = get_scheduler_by_name(simulation_config['Scheduler'])

        if not scheduler_class:
            raise NotImplementedError("Scheduler class {0} does not exist".format(simulation_config['Scheduler']))

        self.scheduler = scheduler_class(
            simulator=self,
            name="Scheduler",
            config = self.config,
            central_queue=self.central_queue,
        )

        # GWF in config has priority over gwf(s) set in clustersetup
        # if GWF is set in config, discard the ones from clustersetup
        if simulation_config.get('GWF'):
            gwf_filenames = [simulation_config.get('GWF')]

        gwf_paths = []
        for gwf in gwf_filenames:
            gwf_path = SimUtils.prepend_gwf_path(gwf)

            # if gwf_path stands for a dir, make sure to add only the
            # files inside that dir
            if os.path.isdir(gwf_path):
                gwf_paths.extend(
                    [os.path.join(gwf_path, file)
                     for file in os.listdir(gwf_path)
                     if os.path.isfile(os.path.join(gwf_path, file))]
                )
            else:
                gwf_paths.append(gwf_path)

        workflows, tasks = SimUtils.read_tasks(cluster_setup, gwf_paths)
        self.log_tasks_in(workflows, tasks)

        self.central_queue.set_task_list(tasks, first_submission_at_zero=False)
        self.central_queue.set_workflow_dict(workflows)

        dt_end = datetime.datetime.now()

        self.logger.log('init StartTime = {0}'.format(dt_start.strftime(SimUtils.DATE_FORMAT)))
        self.logger.log('init EndTimes  = {0}'.format(dt_end.strftime(SimUtils.DATE_FORMAT)))
        self.logger.log('init RunTime   = {0}'.format(dt_end - dt_start))

    def start(self, ts_end=100):
        dtMainStartTime = datetime.datetime.now()
        cycle_duration = AIStatistics.CStats(bIsNumeric=True, bKeepValues=False, bAutoComputeStats=True)

        # reset timestamps
        self.ts_end = ts_end
        self.ts_now = 0  # must be done before all activate, just to be sure

        self.activate_entities()

        # start processing events
        last_ts_now = None
        dtTSStartTime = datetime.datetime.now()
        cycle_index = 0
        # reset current cycle's message count
        self.crt_cycle_messages_count = {}

        # ['S2Ss_JOB_DONE', 'S2Ss_MONITOR', 'S2Ss_RESCHEDULE', 'S2U_JOB_DONE', 'U2S_ADD_JOB', 'U2Us_SEND_JOBS]
        event_types = []
        for event_type in self.sites[0].events_map:
            event_types.append(event_type)
        for event_type in self.central_queue.events_map:
            event_types.append(event_type)
        for event_type in self.system_monitor.events_map:
            event_types.append(event_type)

        self.logger.log_and_db('Sys: Tasks In      ={0}'.format(self.system_monitor.sstats_Total_NTasksIn))
        self.logger.log_and_db('Sys: Tasks Started ={0}'.format(self.system_monitor.sstats_Total_NTasksStarted))
        self.logger.log_and_db('Sys: Tasks Finished={0}'.format(self.system_monitor.sstats_Total_NTasksFinished))
        self.logger.log_and_db('Sys: Tasks To Come ={0}'.format(self.system_monitor.getNTasksToCome()))

        def gen_task_scheduler_event():
            return SimCore.Event(
                self.ts_now,
                self.scheduler.id,
                self.scheduler.id,
                {'type': Constants.CQ2S_SCHEDULER_AUTORESCHEDULE}
            )

        while not self.forced_stop and self.ts_now <= self.ts_end and self.events:
            event = self.events.dequeue()

            # Do not parse the event if it's later than ts_end.
            if event.ts_arrival > self.ts_end:
                break

            self.ts_now = event.ts_arrival

            self.logger.log('Processing event {0}'.format(event), 'debug')

            # statistics
            if 'type' in event.params:
                event_type = event.params['type']
            else:
                event_type = 'unknown'

            # count this event in the stats
            if event_type not in self.crt_cycle_messages_count: self.crt_cycle_messages_count[event_type] = 0
            self.crt_cycle_messages_count[event_type] += 1

            if last_ts_now is not None:
                if self.ts_now < last_ts_now:
                    cycle_index += 1
                    self.logger.log_and_db('HUH!? got next event before the last processed event!?', 'error')
                if self.ts_now > last_ts_now:
                    cycle_index += 1

                    ## amod 2007-04-07: bug: self.cycle_messages_count[last_ts_now] might not include all self.event_types
                    if last_ts_now not in self.cycle_messages_count:
                        self.cycle_messages_count[last_ts_now] = self.crt_cycle_messages_count
                        for event_type in event_types:
                            if event_type not in self.cycle_messages_count[last_ts_now]:
                                self.cycle_messages_count[last_ts_now][event_type] = 0
                    else:
                        for event_type in self.crt_cycle_messages_count:
                            self.cycle_messages_count[last_ts_now][event_type] += self.crt_cycle_messages_count[
                                event_type]

                    # XXX write stats in hpdc-3_messages_over_time_tmp.dat
                    # self.crt_cycle_messages_count -- logs all messages in this current cycle
                    NoEvents = 0
                    for event_type in self.crt_cycle_messages_count:
                        NoEvents += self.crt_cycle_messages_count[event_type]
                    for event_type in event_types:
                        if event_type in self.crt_cycle_messages_count:
                            self.DBStats.addNoMessages(self.ts_now, event_type,
                                                       self.crt_cycle_messages_count[event_type])
                        else:
                            self.DBStats.addNoMessages(self.ts_now, event_type, 0)
                    # if cycle_index % 100 == 0: fout.flush()

                    # reset current cycle's message count
                    self.crt_cycle_messages_count = {}

                    if cycle_index % 10000 == 0:
                        dtTSEndTime = datetime.datetime.now()
                        self.logger.log('======')
                        self.logger.log('CYCLE {0} (TS={1}) StartTime= {2}'.format(
                            cycle_index,
                            last_ts_now,
                            dtTSStartTime.strftime(SimUtils.DATE_FORMAT)
                        ))

                        self.logger.log('CYCLE {0} (TS={1}) EndTime  = {2}'.format(
                            cycle_index,
                            last_ts_now,
                            dtTSEndTime.strftime(SimUtils.DATE_FORMAT)
                        ))

                        self.logger.log('CYCLE {0} (TS={1}) RunTime  = {2}'.format(
                            cycle_index,
                            last_ts_now,
                            dtTSEndTime - dtTSStartTime
                        ))

                        cycle = time.mktime(dtTSEndTime.timetuple()) - time.mktime(dtTSStartTime.timetuple())
                        cycle_duration.addValue(cycle)
                        dtMainEndTime = datetime.datetime.now()

                        self.logger.log(
                            'CYCLE {0} (TS={1}) Last={2}\n\tStats for 10k cycles: Avg={3}s Min={4}s Max={5}s'.format(
                                cycle_index,
                                last_ts_now,
                                dtTSEndTime - dtTSStartTime,
                                cycle_duration.Avg,
                                cycle_duration.Min,
                                cycle_duration.Max
                            )
                        )
                        self.logger.log('CYCLES TotalRunTime  = {0}'.format(dtMainEndTime - dtMainStartTime))

                        self.logger.log_and_db('Sys: Tasks In      ={0}'.format(self.system_monitor.sstats_Total_NTasksIn))
                        self.logger.log_and_db(
                            'Sys: Tasks Started ={0}'.format(self.system_monitor.sstats_Total_NTasksStarted))
                        self.logger.log_and_db(
                            'Sys: Tasks Finished={0}'.format(self.system_monitor.sstats_Total_NTasksFinished))
                        self.logger.log_and_db('Sys: Tasks To Come ={0}'.format(self.system_monitor.getNTasksToCome()))

                        dtTSStartTime = dtTSEndTime

            last_ts_now = self.ts_now

            if self.ts_now > self.ts_end:
                self.logger.log_and_db(
                    'Got an event with ts_arrival={0} > ts_end={1} --> ending simulation'.format(self.ts_now,
                                                                                                 self.ts_end),
                    'warning')
                break

            self.dispatch(event)

        if self.forced_stop:
            self.logger.log_and_db('Was forced to stop!', 'warning')

        self.DBStats.flushall()
        self.DBTasksDoneTrace.flush()
        self.DBTasksInTrace.flush()

        dtMainEndTime = datetime.datetime.now()
        self.logger.log('======')
        self.logger.log('run StartTime = {0}'.format(dtMainStartTime.strftime(SimUtils.DATE_FORMAT)))
        self.logger.log('run EndTime   = {0}'.format(dtMainEndTime.strftime(SimUtils.DATE_FORMAT)))
        self.logger.log('run RunTime   = {0}'.format(dtMainEndTime - dtMainStartTime))

        self.system_monitor.refresh_sstats({})
        self.logger.log_and_db('Sys: Tasks In         ={0}'.format(self.system_monitor.sstats_Total_NTasksIn))
        self.logger.log_and_db('Sys: Tasks Started    ={0}'.format(self.system_monitor.sstats_Total_NTasksStarted))
        self.logger.log_and_db('Sys: Tasks Finished   ={0}'.format(self.system_monitor.sstats_Total_NTasksFinished))
        self.logger.log_and_db('Sys: Tasks Interrupted={0}'.format(self.system_monitor.sstats_Total_NTasksInterrupted))
        self.logger.log_and_db('Sys: Tasks To Come    ={0}'.format(self.system_monitor.getNTasksToCome()))
        self.logger.log('Sys: Tasks Too Large  ={0}'.format(self.system_monitor.count_tasks_too_large()))

    def report(self):
        self.logger.db('Simulation report')
        self.logger.db('=============================')
        self.logger.db('Events:' + '%8d' % self.events.count_events_in + '|' + \
                  '%8d' % self.events.count_events_peek + '|' + \
                  '%8d' % self.events.count_events_out + '(In/P/Out)')
        if self.events.count_events_in > 0:
            self.logger.db('   [%]: ' + '%7s%%' % ('%.2f' % 100.0) + '|' + \
                      '%7s%%' % ('%.2f' % (
                          100.0 * self.events.count_events_peek / self.events.count_events_in)) + '|' + \
                      '%7s%%' % ('%.2f' % (
                          100.0 * self.events.count_events_out / self.events.count_events_in)) + '(In/P/Out)')
        else:
            self.logger.db(' [%]: ' + '%8s' % 'n/a' + '|' + \
                      '%8s' % 'n/a' + '|' + \
                      '%8s%%' % 'n/a' + '(In/P/Out)')

        if self.events:
            next_event = self.events.peek()
            self.logger.db('TS of next event in the queue: {0}'.format(next_event.ts_arrival))

            self.logger.db('Simulated System Stats')
            self.logger.db('=============================')

        RptStats = {
            'Site_Tasks_In': AIStatistics.CStats(bIsNumeric=True, bKeepValues=False, bAutoComputeStats=False),
            'Site_Tasks_StartedPer': AIStatistics.CStats(bIsNumeric=True, bKeepValues=False, bAutoComputeStats=False),
            'Site_Tasks_FinishedPer': AIStatistics.CStats(bIsNumeric=True, bKeepValues=False, bAutoComputeStats=False),
            'CentralQueue_Tasks_Submitted': AIStatistics.CStats(bIsNumeric=True, bKeepValues=False,
                                                               bAutoComputeStats=False),
            'CentralQueue_Tasks_Finished': AIStatistics.CStats(bIsNumeric=True, bKeepValues=False,
                                                              bAutoComputeStats=False),
            'CentralQueue_Tasks_FinishedPer': AIStatistics.CStats(bIsNumeric=True, bKeepValues=False,
                                                                 bAutoComputeStats=False)
        }

        for Site in self.sites:
            site_monitor = Site.site_monitor
            self.logger.db('Site: {0}'.format(Site.name))
            self.logger.db('Tasks: ' + '%8d' % site_monitor.stats_Total_NTasksIn + \
                      '|' + '%8d' % site_monitor.stats_Total_NTasksStarted + '|' + '%8d' % site_monitor.stats_Total_NTasksFinished + '(In/S/F)')
            self.logger.flush()
            RptStats['Site_Tasks_In'].addValue(site_monitor.stats_Total_NTasksIn)
            if site_monitor.stats_Total_NTasksIn > 0:
                RptStats['Site_Tasks_StartedPer'].addValue(
                    100.0 * site_monitor.stats_Total_NTasksStarted / site_monitor.stats_Total_NTasksIn)
                RptStats['Site_Tasks_FinishedPer'].addValue(
                    100.0 * site_monitor.stats_Total_NTasksFinished / site_monitor.stats_Total_NTasksIn)
                self.logger.db(' [%]: ' + '%7s%%' % ('%.2f' % 100.0) + '|' + \
                          '%7s%%' % ('%.2f' % (
                              100.0 * site_monitor.stats_Total_NTasksStarted / site_monitor.stats_Total_NTasksIn)) + '|' + \
                          '%7s%%' % ('%.2f' % (
                              100.0 * site_monitor.stats_Total_NTasksFinished / site_monitor.stats_Total_NTasksIn)) + '(In/S/F)')
            else:
                # RptStats['Site_Tasks_StartedPer'].addValue( 0.0 )
                # RptStats['Site_Tasks_FinishedPer'].addValue( 0.0 )
                self.logger.db(' [%]: ' + '%8s' % 'n/a' + '|' + \
                          '%8s' % 'n/a' + '|' + \
                          '%8s%%' % 'n/a' + '(In/S/F)')

        RptStats['CentralQueue_Tasks_Submitted'].addValue(self.central_queue.submitted_tasks_count)
        RptStats['CentralQueue_Tasks_Finished'].addValue(self.central_queue.finished_tasks_count)
        if self.central_queue.submitted_tasks_count > 0:
            RptStats['CentralQueue_Tasks_FinishedPer'].addValue(
                100.0 * self.central_queue.finished_tasks_count / self.central_queue.submitted_tasks_count)

            self.logger.db('Complete Stats')
            self.logger.db('=============================')
            self.logger.db('%s\t%s\t%s\t%s\t%s\t%s\t%s' % ('Name', 'NItems', 'Avg', 'Min', 'Max', 'Sum', 'CoV'))
        for Stat in ['Site_Tasks_In',
                     'Site_Tasks_StartedPer',
                     'Site_Tasks_FinishedPer',
                     'CentralQueue_Tasks_Submitted',
                     'CentralQueue_Tasks_Finished',
                     'CentralQueue_Tasks_FinishedPer']:
            RptStats[Stat].doComputeStats()
            # logger.db( "%s\t%s\t%s\t%s\t%s\t%s\t%s" % (
            #     Stat, RptStats[Stat].NItems, "%.1f" % RptStats[Stat].Avg,
            #     "%.1f" % RptStats[Stat].Min, "%.1f" % RptStats[Stat].Max, "%.1f" % RptStats[Stat].Sum,
            #     "%.1f" % RptStats[Stat].COV))

        if self.autoscaler:
            self.autoscaler.report_stats(self.ts_now, self.resource_manager.get_maximum_capacity())
        self.central_queue.report_stats()

    def run(self):
        dt_start = datetime.datetime.now()

        self.setup()
        self.start(self.config['simulation']['N_TICKS'])
        self.report()

        dt_end = datetime.datetime.now()

        self.logger.log('-------------')
        self.logger.log('StartTime = {0}'.format(dt_start.strftime(SimUtils.DATE_FORMAT)))
        self.logger.log('EndTime   = {0}'.format(dt_end.strftime(SimUtils.DATE_FORMAT)))
        self.logger.log('RunTime   = {0}'.format(dt_end - dt_start))

        self.logger.close()
        cleanup_logging(self.runtime_handler)  # ensure handlers don't persist between simulation runs

if __name__ == "__main__":
    arguments = docopt(__doc__)

    if arguments['--quiet']:
        setup_logging(logging.NullHandler())
    elif arguments['--verbose']:
        setup_logging(logging.StreamHandler(sys.stdout), logging.DEBUG)
    else:
        setup_logging(logging.StreamHandler(sys.stdout), logging.INFO)

    if arguments['<config_filename>']:
        config = SimUtils.load_config(arguments['<config_filename>'], config_schema)
    elif arguments['<N_TICKS>']:
        config = SimUtils.generate_config(
            N_TICKS=arguments['<N_TICKS>'],
            GWF=arguments['--GWF'],
            N_CLUSTERS=arguments['--N'],
            config_schema=config_schema,
        )
    else:
        config = SimUtils.load_config(os.path.join(ProjectUtils.root_path, 'default_config.ini'), config_schema)

    if arguments['-o']:
        config['simulation']['RUNTIME_OUTPUT_FILENAME'] = arguments['-o']

    system_sim = SystemSim(config)
    system_sim.run()
