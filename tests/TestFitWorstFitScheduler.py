from mock import MagicMock

from core import Constants
from core.Task import Task
from core.Site import Site
from core.SystemSim import SystemSim
from tests.TestBase import BaseTest
from utils import SimUtils


class TestKoalaSC(BaseTest):
    def test_sim_creation_config(self):
        sim = SystemSim(config=self.config)
        self.assertEqual(sim.config, self.config, "Configs were not equal!")

    def test_sim_output(self):
        sim = SystemSim(config=self.config)
        self.assertEqual(sim.output, SimUtils.get_output(self.config),
                         "Output paths are not equal! Expected {0} but was {1}".format(SimUtils.get_output(self.config),
                                                                                       sim.output))
    def test_sim_setup(self):
        sim = SystemSim(config=self.config)
        sim.setup()

        self.assertFalse(sim.forced_stop)
        self.assertEqual(len(sim.resource_manager.sites), 2)

    def test_task_initialisation(self):
        task = Task("TestOwner", 1337, 0, 42, 1000, [])
        self.assertEqual(task.id, "TestOwner")
        self.assertEqual(task.ts_submit, 1337)
        self.assertEqual(task.submission_site, 0)
        self.assertEqual(task.runtime, 42)
        self.assertEqual(task.cpus, 1000)
        self.assertEqual(task.ts_start, -1)
        self.assertEqual(task.ts_end, -1)
        self.assertEqual(task.status, Task.STATUS_SUBMITTED)

    def test_task_non_positive_duration(self):
        task = Task("TestOwner", 1337, 0, -1, 1000, [])
        self.assertEqual(task.runtime, 1)

    def test_task_non_positive_size(self):
        task = Task("TestOwner", 1337, 0, 42, -1, [])
        self.assertEqual(task.cpus, 1)

    def test_site_initialisation(self):
        fakeSimulator = MagicMock()
        fakeSimulator.config = self.config
        fakeSimulator.events = {}
        site = Site(fakeSimulator, "TestSite", 1, 2)
        self.assertEqual(site.name, "TestSite")
        self.assertEqual(site.resources, 1)
        self.assertEqual(site.used_resources, 0)
        self.assertEqual(site.resource_speed, 2)
        self.assertEqual(site.task_queue, [])
        self.assertEqual(site.report_interval, 1)
        self.assertEqual(len(site.events_map), 4)
        self.assertEqual(len(site.running_tasks), 0)

        # Test the method mapping
        self.assertEqual(site.events_map[Constants.CQ2S_ADD_TASK], site.add_task)
        self.assertEqual(site.events_map[Constants.S2Ss_RESCHEDULE], site.reschedule)
        self.assertEqual(site.events_map[Constants.S2Ss_TASK_DONE], site.finish_task)
        self.assertEqual(site.events_map[Constants.S2Ss_MONITOR], site.monitor)

        # Test the stats for total
        site_monitor = site.site_monitor
        self.assertEqual(site_monitor.stats_Total_NTasksIn, 0)
        self.assertEqual(site_monitor.stats_Total_NTasksStarted, 0)
        self.assertEqual(site_monitor.stats_Total_NTasksFinished, 0)
        self.assertEqual(site_monitor.stats_Total_ConsumedCPUTime, 0)

        # Test the LRTU stats
        self.assertEqual(site_monitor.stats_LRTU_NTasksIn, 0)
        self.assertEqual(site_monitor.stats_LRTU_NTasksStarted, 0)
        self.assertEqual(site_monitor.stats_LRTU_NTasksFinished, 0)
        self.assertEqual(site_monitor.stats_LRTU_ConsumedCPUTime, 0)

    def test_site_activate(self):
        fakeSimulator = MagicMock()
        fakeSimulator.config = self.config
        fakeQueue = MagicMock()
        fakeQueue.enqueue = MagicMock(name='enqueue')
        fakeSimulator.events = fakeQueue
        site = Site(fakeSimulator, "TestSite", 1, 1)
        site.activate()
        fakeQueue.enqueue.assert_called_once()

