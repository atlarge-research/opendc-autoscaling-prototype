import os

from core import SimCore, Constants
from core.SimLogger import DBLogger

from utils import SimUtils


class Scheduler(SimCore.SimEntity):

    def __init__(self, simulator, name, config, central_queue):
        super(Scheduler, self).__init__(simulator, name)

        self.config = config
        self.central_queue = central_queue
        output = SimUtils.get_output(config)

        simulation_config = self.config['simulation']
        DBLog_path = os.path.join(output, simulation_config['DBLog'])
        self.logger = DBLogger(sim=self.sim, DBName=DBLog_path, BufferSize=10000)

        # TODO(Laurens): Make this changeable in the config.
        self.N_TICKS_BETWEEN_AUTO_RESCHEDULE = 1

        # overwrite the events map and register the Scheduler events
        self.events_map = {
            Constants.CQ2S_SCHEDULER_AUTORESCHEDULE: self.auto_reschedule,
        }

    def auto_reschedule(self, params):
        """
        Override this function in your Scheduler to implement your desired scheduling behavior.
        """
        raise NotImplementedError("The base class auto_reschedule function should be overridden.")

    def activate(self):
        self.events.enqueue(
            SimCore.Event(self.sim.ts_now, self.id, self.id, {'type': Constants.CQ2S_SCHEDULER_AUTORESCHEDULE})
        )

