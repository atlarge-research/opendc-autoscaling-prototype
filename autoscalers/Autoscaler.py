import math

from core import SimCore, Constants
from utils import SimUtils


class Autoscaler(SimCore.SimEntity):
    N_TICKS_BETWEEN_PREDICTIVE_AUTO_SCHEDULE = 3600
    N_TICKS_BETWEEN_REACTIVE_AUTO_SCHEDULE = 60

    def __init__(self, simulator, name, logger):
        super(Autoscaler, self).__init__(simulator, name)

        self.logger = logger
        self.resource_manager = self.sim.resource_manager
        self.system_monitor = self.sim.system_monitor

        self.events_map = {
            Constants.AUTO_SCALE_EVALUATE: self.evaluate,
        }

        self.N_TICKS_PER_EVALUATE = self.config['autoscaler']['N_TICKS_PER_EVALUATE']
        self.SERVER_SPEED = self.config['autoscaler']['SERVER_SPEED']
        self.DELTA_T = self.N_TICKS_PER_EVALUATE
        self.EPSILON = 1

        self.CHARGE_PERIOD = 3600
        self.CHARGE_COST = 1

        self.sites = self.resource_manager.sites
        self.autoscale_steps = 0

        self.underprovisioning = 0
        self.overprovisioning = 0

        self.underprovisioning_normalized = 0
        self.overprovisioning_normalized = 0

        self.overprovisioning_mU = 0

        self.time_underprovisioning = 0
        self.time_overprovisioning = 0

        self.instability_k = 0
        self.instability_k_prime = 0

        self.average_resources = 0
        self.average_charged_CPU_hours = 0

        self.autoscale_op = 0

        self.log_autoscale_ops = SimUtils.add_file_logging(
            'autoscale_ops',
            self.config['autoscaler']['OPS_FILENAME'],
            self.config
        )

        self.log_elasticity_metrics = SimUtils.add_file_logging(
            'elasticity_metrics',
            self.config['autoscaler']['ELASTICITY_METRICS_FILENAME'],
            self.config
        )

        self.log_cost_metrics = SimUtils.add_file_logging(
            'cost_metrics',
            self.config['autoscaler']['COST_METRICS_FILENAME'],
            self.config
        )

        self.log_elasticity_overview = SimUtils.add_file_logging(
            'elasticity_overview',
            self.config['autoscaler']['ELASTICITY_OVERVIEW_FILENAME'],
            self.config
        )

    def evaluate(self, params):
        """
        This method is getting periodically called by the system to evaluate the current status of the system.
        Override to implement this logic. Should probably be calling either predictive or reactive autoscaling.
        """
        self.autoscale_steps += 1

    def _predictive_autoscaling(self, params=None):
        """Override this function to perform up- or downscaling of resources."""
        pass

    def _reactive_autoscaling(self, params=None):
        """Override this function to perform reactive autoscaling."""
        pass

    def activate(self):
        self.sim.events.enqueue(
            SimCore.Event(
                self.sim.ts_now + self.N_TICKS_PER_EVALUATE,
                self.id,
                self.id,
                {'type': Constants.AUTO_SCALE_EVALUATE}
            )
        )

    def log(self, prev_capacity, mutation, target):
        self.log_autoscale_ops.info('{0}, {1}, {2}, {3}'.format(
            self.sim.ts_now,
            prev_capacity + self.autoscale_op * mutation,
            prev_capacity + self.autoscale_op * target,
            self.system_monitor.get_pending_tasks_load()
        ))

        self.autoscale_op = 0

    def refresh_stats(self, prediction, supply):
        demand = self.system_monitor.get_total_load()
        sign = lambda x: x and (1, -1)[x < 0]

        self.underprovisioning += max(0, demand - supply) * self.DELTA_T
        self.overprovisioning += max(0, supply - demand) * self.DELTA_T

        self.underprovisioning_normalized += max(0, demand - supply) / float(max(demand, self.EPSILON)) * self.DELTA_T
        self.overprovisioning_normalized += max(0, supply - demand) / float(max(supply, self.EPSILON)) * self.DELTA_T

        self.overprovisioning_mU += self.system_monitor.count_idle_resources() * self.DELTA_T

        self.time_underprovisioning += max(0, sign(demand - supply)) * self.DELTA_T
        self.time_overprovisioning += max(0, sign(supply - demand)) * self.DELTA_T

        if self.autoscale_steps > 1:
            self.instability_k += min(1, max(0, sign(supply) - sign(demand))) * self.DELTA_T
            self.instability_k_prime += min(1, max(0, sign(demand) - sign(supply))) * self.DELTA_T

        self.average_resources += supply * self.DELTA_T
        self.average_charged_CPU_hours += math.ceil(
            self.N_TICKS_PER_EVALUATE / float(self.CHARGE_PERIOD)) * self.CHARGE_COST * supply

        self.log_elasticity_metrics.info('{0} {1} {2} {3} {4} {5} {6} {7} {8} {9}'.format(
            self.sim.ts_now,
            self.underprovisioning,
            self.overprovisioning,
            self.underprovisioning_normalized,
            self.overprovisioning_normalized,
            self.time_underprovisioning,
            self.time_overprovisioning,
            self.instability_k,
            self.instability_k_prime,
            self.overprovisioning_mU,
        ))

        self.log_cost_metrics.info('{0} {1} {2}'.format(
            self.sim.ts_now,
            self.average_resources,
            self.average_charged_CPU_hours
        ))

    def report_stats(self, time_horizon, cluster_resources):
        self.logger.log('''
            Underprovisioning accuracy = {0}
            Overprovisioning accuracy = {1}
            Underprovisioning accuracy normalized = {2}
            Overprovisioning accuracy normalized = {3}
            Time underprovisoned = {4}
            Time overprovisoned = {5}
            Instability k = {6}
            Instability k\' = {7}
            Underprovisioning accuracy mU = {8}
            Average number of resources = {9} VMS
            Average accounted CPU hours per VM = {10}
            Average charged CPU hours per VM = {11}'''.format(
            (self.underprovisioning / float(time_horizon * cluster_resources)) * 100,
            (self.overprovisioning / float(time_horizon * cluster_resources)) * 100,
            (self.underprovisioning_normalized / float(time_horizon)) * 100,
            (self.overprovisioning_normalized / float(time_horizon)) * 100,
            (self.time_underprovisioning / float(time_horizon)) * 100,
            (self.time_overprovisioning / float(time_horizon)) * 100,
            (self.instability_k / float(time_horizon - 1)) * 100,
            (self.instability_k_prime / float(time_horizon - 1)) * 100,
            (self.overprovisioning_mU / float(time_horizon * cluster_resources)) * 100,
            self.average_resources / float(time_horizon),
            (self.average_resources / float(time_horizon)) * 3600 / cluster_resources,
            self.average_charged_CPU_hours / float(cluster_resources)
        )
        )

        self.log_elasticity_overview.info("{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}".format(
            (self.underprovisioning / float(time_horizon * cluster_resources)) * 100,
            (self.overprovisioning / float(time_horizon * cluster_resources)) * 100,
            (self.underprovisioning_normalized / float(time_horizon)) * 100,
            (self.overprovisioning_normalized / float(time_horizon)) * 100,
            (self.time_underprovisioning / float(time_horizon)) * 100,
            (self.time_overprovisioning / float(time_horizon)) * 100,
            (self.instability_k / float(time_horizon - 1)) * 100,
            (self.instability_k_prime / float(time_horizon - 1)) * 100,
            (self.overprovisioning_mU / float(time_horizon * cluster_resources)) * 100,
            self.average_resources / float(time_horizon),
            (self.average_resources / float(time_horizon)) * 3600 / cluster_resources,
            self.average_charged_CPU_hours / float(cluster_resources)
        )
        )

        # on last line write simulator runtime and cluster capacity
        self.log_elasticity_metrics.info('{0} {1}'.format(time_horizon, cluster_resources))
        self.log_cost_metrics.info('{0} {1}'.format(time_horizon, cluster_resources))
