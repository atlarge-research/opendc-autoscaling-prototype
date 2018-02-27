import os.path
from unittest import skip

import ProjectUtils
from core import SystemSim
from tests.TestBase import BaseTest
from utils import SimUtils

class TestTokenConsistency(BaseTest):
    def __init__(self, *args, **kwargs):
        super(TestTokenConsistency, self).__init__(*args, **kwargs)

        # We need to create the file in the gwf folder since the Simulator checks there
        self.test_clustersetup_filename = os.path.join(ProjectUtils.root_path, "test_setup.csv")

    def setUp(self):
        self.check_file(self.test_clustersetup_filename, False)
        self.test_clustersetup_file = open(self.test_clustersetup_filename, 'w')

    def tearDown(self):
        self.test_clustersetup_file.close()
        os.remove(self.test_clustersetup_filename)
        self.check_file(self.test_clustersetup_filename, False)

    @skip("This takes a lot of time, uncomment if changes are made to token or token_mod")
    def test_token_modified_behaviour(self):
        with open(self.test_clustersetup_filename, "w") as test_setup_file:
            test_setup_file.write("ClusterID, Cluster, Resource, Speed, Gwf\n")
            for i in xrange(50):
                test_setup_file.write("test{0}, test{0}, 1, 1\n".format(i))

        self.check_file(self.test_clustersetup_filename, True)

        provision_policies = ['token', 'token_mod']
        allocation_policies = ['bestfit', 'worstfit', 'fillworstfit']

        for allocation_policy in allocation_policies:
            results = {}
            for provision_policy in provision_policies:
                config = SimUtils.generate_config(
                    N_TICKS=600,
                    GWF='alexey_icpe_2017_workload_1.gwf',
                    config_schema=SystemSim.config_schema,
                )

                config['simulation']['Autoscaler'] = provision_policy
                config['simulation']['Scheduler'] = allocation_policy
                config['autoscaler']['N_TICKS_PER_EVALUATE'] = 2
                system_sim = SystemSim.SystemSim(config=config)
                system_sim.run()

                with open(os.path.join(SimUtils.get_output(config), config['autoscaler']['OPS_FILENAME'])) as f:
                    results[provision_policy] = f.read()
                with open(os.path.join(SimUtils.get_output(config), config['autoscaler']['ELASTICITY_METRICS_FILENAME'])) as f:
                    results[provision_policy] += f.read()

            self.assertEqual(results['token'], results['token_mod'])
