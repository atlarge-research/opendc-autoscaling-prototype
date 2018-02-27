"""
This file contains tests which test a certain setup that ALWAYS holds for all allocation policies (not necessarily all
provision policies!).

THIS TEST ASSUMES THAT ENOUGH RESOURCES ARE AVAILABLE AT T=0 (I.E. ONE OR MORE BOOTED INSTANCES WITH ENOUGH RESOURCES),
WE ARE NOT TESTING IF THE AUTOSCALING POLICIES SCALE CORRECTLY.
"""
import os.path

import ProjectUtils
from core import SystemSim
from tests.TestBase import BaseTest
from utils import SimUtils


class TestSimulationRuns(BaseTest):
    def __init__(self, *args, **kwargs):
        super(TestSimulationRuns, self).__init__(*args, **kwargs)
        self.test_clustersetup_filename = os.path.join(ProjectUtils.root_path, "test_setup.csv")
        # We need to create the file in the gwf folder since the Simulator checks there
        self.test_workload_filename = os.path.join(ProjectUtils.root_path, "gwf", "test_workload.gwf")

    def setUp(self):
        # Check that the test files do not exist (dirty work environment check).
        self.check_file(self.test_clustersetup_filename, False)
        self.check_file(self.test_workload_filename, False)

        self.test_setup_file = open(self.test_clustersetup_filename, "w")
        self.test_workload_file = open(self.test_workload_filename, "w")

    def tearDown(self):
        self.test_setup_file.close()
        self.test_workload_file.close()

        # Delete the test file (clean up)
        os.remove(self.test_clustersetup_filename)
        os.remove(self.test_workload_filename)
        self.check_file(self.test_clustersetup_filename, False)
        self.check_file(self.test_workload_filename, False)

    def test_bot_same_submit_same_runtime(self):
        """
        This test tests all combinations of allocation and provisioning parameters using a workload of 5 tasks, all
        submitted at t=0 and having a runtime of 5 seconds. Just enough resources are available.
        """
        with open(self.test_clustersetup_filename, "w") as test_setup_file:
            test_setup_file.write("ClusterID, Cluster, Resource, Speed, Gwf\n")
            test_setup_file.write("test, test, 5, 1, {}\n".format(os.path.basename(self.test_workload_filename)))

        with open(self.test_workload_filename, "w") as test_workload_file:
            test_workload_file.write("WorkflowID, JobID, SubmitTime, RunTime, NProcs, ReqNProcs, Dependencies\n")
            for i in xrange(5):
                test_workload_file.write("{0}, {1}, {2}, {3}, {4}, {5}, {6}\n".format(0, i, 0, 5, 1, 1, ""))

        # Check that files do exist now.
        self.check_file(self.test_clustersetup_filename, True)
        self.check_file(self.test_workload_filename, True)

        provision_policies = ["reg", "hist", "conpaas", "react", "token"]
        allocation_policies = ["bestfit", "worstfit", "fillworstfit"]

        # Test all combinations of provision policies and allocation policies.
        for provision_policy in provision_policies:
            for allocation_policy in allocation_policies:
                config = SimUtils.generate_config(
                    N_TICKS=5,
                    N_CLUSTERS=1,
                    config_schema=SystemSim.config_schema,
                )

                config['simulation']['Autoscaler'] = provision_policy
                config['simulation']['Scheduler'] = allocation_policy
                config['simulation']['ClusterSetup'] = self.test_clustersetup_filename

                system_sim = SystemSim.SystemSim(config=config)
                system_sim.run()

                self.assertEqual(system_sim.ts_now, 5)
                self.assertEqual(system_sim.system_monitor.sstats_Total_NTasksFinished, 5)
                self.assertEqual(len(system_sim.central_queue.task_queue), 0)

    def test_bot_same_submit_different_runtime(self):
        """
        This test tests all combinations of allocation and provisioning parameters using a workload of 5 tasks,
        submitted incrementally and having a runtime of 5 seconds. Just enough resources are available.
        """
        with open(self.test_clustersetup_filename, "w") as test_setup_file:
            test_setup_file.write("ClusterID, Cluster, Resource, Speed, Gwf\n")
            test_setup_file.write("test, test, 5, 1, {}\n".format(os.path.basename(self.test_workload_filename)))

        with open(self.test_workload_filename, "w") as test_workload_file:
            test_workload_file.write("WorkflowID, JobID, SubmitTime, RunTime, NProcs, ReqNProcs, Dependencies\n")
            for i in xrange(5):
                test_workload_file.write("{0}, {1}, {2}, {3}, {4}, {5}, {6}\n".format(0, i, i, 5, 1, 1, ""))

        # Check that files do exist now.
        self.check_file(self.test_clustersetup_filename, True)
        self.check_file(self.test_workload_filename, True)

        provision_policies = ["reg", "hist", "conpaas", "react", "token"]
        allocation_policies = ["bestfit", "worstfit", "fillworstfit"]

        # Test all combinations of provision policies and allocation policies.
        for provision_policy in provision_policies:
            for allocation_policy in allocation_policies:
                config = SimUtils.generate_config(
                    N_TICKS=9,
                    N_CLUSTERS=1,
                    config_schema=SystemSim.config_schema,
                )

                config['simulation']['Autoscaler'] = provision_policy
                config['simulation']['Scheduler'] = allocation_policy
                config['simulation']['ClusterSetup'] = self.test_clustersetup_filename

                system_sim = SystemSim.SystemSim(config=config)
                system_sim.run()

                self.assertEqual(system_sim.ts_now, 9)
                self.assertEqual(system_sim.system_monitor.sstats_Total_NTasksFinished, 5)
                self.assertEqual(len(system_sim.central_queue.task_queue), 0)

    def test_simple_workflow(self):
        """
        This test tests a simple workflow consisting of 5 tasks. The structure is as follows:
        o   o
         \ /
          o
         / \
        o   o
        submitted at t=0 and having a runtime of 5 seconds. Just enough resources are available.
        """
        with open(self.test_clustersetup_filename, "w") as test_setup_file:
            test_setup_file.write("ClusterID, Cluster, Resource, Speed, Gwf\n")
            test_setup_file.write("test, test, 2, 1, {}\n".format(os.path.basename(self.test_workload_filename)))

        with open(self.test_workload_filename, "w") as test_workload_file:
            test_workload_file.write("WorkflowID, JobID, SubmitTime, RunTime, NProcs, ReqNProcs, Dependencies\n")
            test_workload_file.write("{0}, {1}, {2}, {3}, {4}, {5}, {6}\n".format(0, 0, 0, 5, 1, 1, ""))
            test_workload_file.write("{0}, {1}, {2}, {3}, {4}, {5}, {6}\n".format(0, 1, 0, 5, 1, 1, ""))
            test_workload_file.write("{0}, {1}, {2}, {3}, {4}, {5}, {6}\n".format(0, 2, 0, 5, 1, 1, "0 1"))
            test_workload_file.write("{0}, {1}, {2}, {3}, {4}, {5}, {6}\n".format(0, 3, 0, 5, 1, 1, "2"))
            test_workload_file.write("{0}, {1}, {2}, {3}, {4}, {5}, {6}\n".format(0, 4, 0, 5, 1, 1, "2"))

        # Check that files do exist now.
        self.check_file(self.test_clustersetup_filename, True)
        self.check_file(self.test_workload_filename, True)

        provision_policies = ["reg", "hist", "conpaas", "react", "token"]
        allocation_policies = ["bestfit", "worstfit", "fillworstfit"]

        # Test all combinations of provision policies and allocation policies.
        for provision_policy in provision_policies:
            for allocation_policy in allocation_policies:
                config = SimUtils.generate_config(
                    N_TICKS=15,
                    N_CLUSTERS=1,
                    config_schema=SystemSim.config_schema,
                )

                config['simulation']['Autoscaler'] = provision_policy
                config['simulation']['Scheduler'] = allocation_policy
                config['simulation']['ClusterSetup'] = self.test_clustersetup_filename

                system_sim = SystemSim.SystemSim(config=config)
                system_sim.run()

                self.assertEqual(system_sim.ts_now, 15)
                self.assertEqual(system_sim.system_monitor.sstats_Total_NTasksFinished, 5,
                                 "Expected {0} but was {1} for {2} and {3}".format(5,
                                                                                   system_sim.system_monitor.sstats_Total_NTasksFinished,
                                                                                   provision_policy,
                                                                                   allocation_policy))
                self.assertEqual(len(system_sim.central_queue.task_queue), 0)
                self.assertEqual(len(system_sim.central_queue.workflows), 1)

    def test_more_complicated_workflow(self):
        """
        This test tests a more complicated workflow consisting of 10 tasks. The structure is as follows:
              o
              |
              o
            /  \
           o    o
          / \  / \
         o  o o  o
         \  | | /
          \ |/ /
            o
            |
            o
        submitted at t=0 and having different runtimes. Just enough resources are available.
        """
        with open(self.test_clustersetup_filename, "w") as test_setup_file:
            test_setup_file.write("ClusterID, Cluster, Resource, Speed, Gwf\n")
            # TODO(Laurens): change N_TICKS_BETWEEN_MONITORING and set amount of resources to 4 so that we check tight
            # situations.
            test_setup_file.write("test, test, 4, 1, {}\n".format(os.path.basename(self.test_workload_filename)))

        with open(self.test_workload_filename, "w") as test_workload_file:
            test_workload_file.write("WorkflowID, JobID, SubmitTime, RunTime, NProcs, ReqNProcs, Dependencies\n")
            test_workload_file.write("{0}, {1}, {2}, {3}, {4}, {5}, {6}\n".format(0, 0, 0, 1, 1, 1, ""))
            test_workload_file.write("{0}, {1}, {2}, {3}, {4}, {5}, {6}\n".format(0, 1, 0, 2, 1, 1, "0"))
            test_workload_file.write("{0}, {1}, {2}, {3}, {4}, {5}, {6}\n".format(0, 2, 0, 3, 1, 1, "1"))
            test_workload_file.write("{0}, {1}, {2}, {3}, {4}, {5}, {6}\n".format(0, 3, 0, 3, 1, 1, "1"))
            test_workload_file.write("{0}, {1}, {2}, {3}, {4}, {5}, {6}\n".format(0, 4, 0, 4, 1, 1, "2"))
            test_workload_file.write("{0}, {1}, {2}, {3}, {4}, {5}, {6}\n".format(0, 5, 0, 4, 1, 1, "2"))
            test_workload_file.write("{0}, {1}, {2}, {3}, {4}, {5}, {6}\n".format(0, 6, 0, 4, 1, 1, "3"))
            test_workload_file.write("{0}, {1}, {2}, {3}, {4}, {5}, {6}\n".format(0, 7, 0, 4, 1, 1, "3"))
            test_workload_file.write("{0}, {1}, {2}, {3}, {4}, {5}, {6}\n".format(0, 8, 0, 2, 1, 1, "4 5 6 7"))
            test_workload_file.write("{0}, {1}, {2}, {3}, {4}, {5}, {6}\n".format(0, 9, 0, 1, 1, 1, "8"))

        # Check that files do exist now.
        self.check_file(self.test_clustersetup_filename, True)
        self.check_file(self.test_workload_filename, True)

        provision_policies = ["reg", "hist", "conpaas", "react", "token"]
        allocation_policies = ["bestfit", "worstfit", "fillworstfit"]

        # Test all combinations of provision policies and allocation policies.
        for provision_policy in provision_policies:
            for allocation_policy in allocation_policies:
                config = SimUtils.generate_config(
                    N_TICKS=13,
                    N_CLUSTERS=1,
                    config_schema=SystemSim.config_schema,
                )

                config['simulation']['Autoscaler'] = provision_policy
                config['simulation']['Scheduler'] = allocation_policy
                config['simulation']['ClusterSetup'] = self.test_clustersetup_filename

                config['autoscaler']['N_TICKS_PER_EVALUATE'] = 30
                config['central_queue']['N_TICKS_MONITOR_SITE_STATUS'] = 1

                system_sim = SystemSim.SystemSim(config=config)
                system_sim.run()

                self.assertEqual(system_sim.ts_now, 13)
                self.assertEqual(system_sim.system_monitor.sstats_Total_NTasksFinished, 10,
                                 "Expected {0} but was {1} for {2} and {3}".format(10,
                                                                                   system_sim.system_monitor.sstats_Total_NTasksFinished,
                                                                                   provision_policy,
                                                                                   allocation_policy))
                self.assertEqual(len(system_sim.central_queue.task_queue), 0)
                self.assertEqual(len(system_sim.central_queue.workflows), 1)
