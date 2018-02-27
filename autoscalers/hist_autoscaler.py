import math

from autoscalers.Autoscaler import Autoscaler
from core import SimCore, Constants
from utils import SimUtils


class HistAutoscaler(Autoscaler):
    def __init__(self, simulator, logger):
        super(HistAutoscaler, self).__init__(simulator, 'Hist', logger)

        self.error_past_hours = []
        self.histogram = {}

        self.PERCENTILE = self.config['autoscaler']['HIST_PERCENTILE']

        # Initialize the histogram empty.
        for i in range(24):
            self.histogram[i] = []

    def estimate_amount_of_tasks(self, hour):
        total_error = 0
        if len(self.error_past_hours) == 7200:
            total_error = sum(self.error_past_hours) / 7200
            self.error_past_hours = self.error_past_hours[3600:]
        predictor = self.histogram[hour]
        predictor.sort()

        self.logger.log(predictor, 'debug')

        try:
            precentile = predictor[int(len(predictor) * self.PERCENTILE)]
            return precentile + total_error - self.resource_manager.get_current_capacity()
        except:
            return total_error

    def hist_repair(self, load, current_capacity):
        increased_load = load + 2
        if load > current_capacity:
            return increased_load

    def evaluate(self, params):
        super(HistAutoscaler, self).evaluate(params)
        self.logger.log('Starting evaluate autoscaling process', 'debug')

        server_speed = self.SERVER_SPEED
        current_load = self.system_monitor.get_total_load()

        current_capacity = self.resource_manager.get_current_capacity()

        server_load = int(math.ceil(float(current_load) / server_speed))
        self.error_past_hours += [current_capacity - server_load]

        self.error_past_hours += [current_capacity - server_load]
        hour, day = SimUtils.get_hour_and_day_for_ts(self.sim.ts_now)
        self.histogram[hour] += [server_load]
        results = self.estimate_amount_of_tasks(hour)
        self.logger.log("Initial estimation of machines: {0}".format(results), 'debug')
        counter = 0
        # Grab the last 10 errors
        for i in self.error_past_hours[-10:]:
            if i < 0:  # If we underestimated (negative error)
                counter += 1
        if counter > 5:  # More than half were underestimations, so react.
            results += self.hist_repair(server_load, current_capacity)
            self.error_past_hours = []

        self.logger.log("Estimated amount of machines needed: {0}".format(results))

        mutation = 0

        if results < 0:
            self.autoscale_op = -1
            mutation = self.resource_manager.release_resources_best_effort(abs(results))
        elif results > 0:
            self.autoscale_op = 1
            mutation = self.resource_manager.start_up_best_effort(results)

        self.logger.log("Mutation: {0}".format(mutation))

        self.log(current_capacity, mutation, abs(results))
        self.refresh_stats(current_capacity + results, current_capacity + mutation * self.autoscale_op)

        self.sim.events.enqueue(
            SimCore.Event(
                self.sim.ts_now + self.N_TICKS_PER_EVALUATE,
                self.id,
                self.id,
                {'type': Constants.AUTO_SCALE_EVALUATE}
            )
        )
