from core import SimCore, Constants
from autoscalers.Autoscaler import Autoscaler


class ReactAutoscaler(Autoscaler):
    def __init__(self, simulator, logger):
        super(ReactAutoscaler, self).__init__(simulator, 'React', logger)

        self.server_speed = 1

    def evaluate(self, params):
        super(ReactAutoscaler, self).evaluate(params)
        total_load = self.system_monitor.get_total_load() / self.SERVER_SPEED
        current_capacity = self.resource_manager.get_current_capacity()

        prediction = 0
        target = 0
        mutation = 0
        missing_capacity = total_load - current_capacity
        if missing_capacity >= 0:
            self.autoscale_op = 1
            target = missing_capacity + 2
            prediction = current_capacity + target
            mutation = self.resource_manager.start_up_best_effort(target)
            self.logger.log('Upscaled by {0}, target was {1}'.format(mutation, target))
        elif missing_capacity < -2:
            self.autoscale_op = -1
            target = abs(missing_capacity) + 2
            prediction = current_capacity - target
            mutation = self.resource_manager.release_resources_best_effort(target)
            self.logger.log('Downscaled by {0}, target was {1}'.format(mutation, target))

        self.log(current_capacity, mutation, target)
        self.refresh_stats(prediction, current_capacity + mutation * self.autoscale_op)

        self.sim.events.enqueue(
            SimCore.Event(
                self.sim.ts_now + self.N_TICKS_PER_EVALUATE,
                self.id,
                self.id,
                {'type': Constants.AUTO_SCALE_EVALUATE}
            )
        )
