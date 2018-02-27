import math
import numpy
import warnings
from collections import deque

from core import SimCore, Constants
from autoscalers.Autoscaler import Autoscaler


class RegAutoscaler(Autoscaler):
    def __init__(self, simulator, logger):
        super(RegAutoscaler, self).__init__(simulator, 'Reg', logger)

        self.PastTime = deque(maxlen=72)
        self.PastLoad = deque(maxlen=72)

    def evaluate(self, params):
        super(RegAutoscaler, self).evaluate(params)
        self.logger.log('Starting reactive autoscaling process')

        total_load = self.system_monitor.get_total_load() / self.SERVER_SPEED
        current_capacity = self.resource_manager.get_current_capacity()

        self.PastTime.append(self.sim.ts_now)
        self.PastLoad.append(total_load)
 
        if current_capacity > total_load:
            with warnings.catch_warnings():
                warnings.filterwarnings('ignore')

                polynomial = numpy.poly1d(numpy.polyfit(self.PastTime, self.PastLoad, deg=2))
                future_load = math.ceil(polynomial(self.sim.ts_now))
                if future_load > current_capacity:
                    future_load = 0
        else:
            future_load = total_load

        mutation = 0
        target = future_load - current_capacity
        if target > 0:
            self.autoscale_op = 1
            mutation = self.resource_manager.start_up_best_effort(target)
            self.logger.log('Upscaled by {0}, target was {1}'.format(mutation, target))
        elif target < 0:
            self.autoscale_op = -1
            target = abs(target)
            mutation = self.resource_manager.release_resources_best_effort(target)
            self.logger.log('Downscaled by {0}, target was {1}'.format(mutation, target))

        self.log(current_capacity, mutation, target)
        self.refresh_stats(future_load, current_capacity + mutation * self.autoscale_op)

        self.sim.events.enqueue(
            SimCore.Event(
                self.sim.ts_now + self.N_TICKS_PER_EVALUATE,
                self.id,
                self.id,
                {'type': Constants.AUTO_SCALE_EVALUATE}
            )
        )
