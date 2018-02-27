import math

import time

from core import SimCore, Constants
from autoscalers.Autoscaler import Autoscaler


# The code in this file is from the authors. We did NOT refactor it becuase this may compromise the original workings
# of the autoscaler.
# We did add code at the end of the evaluate function to scale up/down according to the results.

class AdaptAutoscaler(Autoscaler):
    def __init__(self, simulator, logger):
        super(AdaptAutoscaler, self).__init__(simulator, 'Adapt', logger)

        self.NumberOfRequests = 0
        self.Number100 = 0
        self.Number200 = 0
        self.Number300 = 0
        self.Number400 = 0
        self.Number500 = 0

        self.NumberMachines = 0
        self.Errors = 0
        self.delta_T = 1
        self.repair_c = 0
        self.s = 0
        self.PausedVMs = []
        self.Experiment = open("Wiki.log", "w")
        self.u_estimate = 0
        self.P_estimate = 0
        self.sum500 = 0
        self.decisionCurrentCapacity = 0
        self.initial_Time = self.sim.ts_now
        self.t2 = self.sim.ts_now
        self.Time_Previous = self.sim.ts_now
        self.Time_lastEstimation = self.sim.ts_now
        self.CapacityList = []
        self.GammaTime = self.Time_Previous
        self.sigma_Alive = 1

    def estimator(self, t, Delta_Load, sigma_Alive, AvgCapacity, D):
        self.Delta_Load = Delta_Load
        self.avg_n = float(sigma_Alive) / t  # self.delta_T #t
        self.u_estimate = float(AvgCapacity) / self.avg_n
        self.P_estimate = float(Delta_Load) / self.avg_n  # tmath.ceil(self.delta_T) #math.ceil(self.delta_T) #
        if AvgCapacity != 0:
            self.delta_T = math.ceil(float(D) / AvgCapacity)
        else:
            self.delta_T = 1

    def controller(self, delta_Time):
        self.R = self.u_estimate * self.P_estimate * self.avg_n
        if self.R < 0:
            self.R = self.R / 15
            # print "Neg", self.R / 2
        else:
            self.R = self.R / delta_Time
        # print self.R / delta_Time

    def ProactiveRepair(self, Server_Speed, Load, delta_Time, CurrentCapacity):
        self.repair_c += (self.R)
        Proactive = 0
        self.PastMinute = 0
        if self.repair_c < 0:
            self.s = int(self.repair_c)
            # print self.s, "SSS", self.repair_c
            self.repair_c -= self.s
            if CurrentCapacity + abs(self.s) >= math.ceil(self.NumberOfRequests / Server_Speed) + 2:
                self.decisionCurrentCapacity += math.ceil(
                    self.s)  # because when I scale down, no more requests go to the VMs to shut down.
                # print>> self.Experiment, "in c"
                return -abs(self.s)
            elif self.s < 0:
                self.decisionCurrentCapacity = math.ceil(self.NumberOfRequests / Server_Speed) + 2
                # print>> self.Experiment, "in b"
                return -abs(int(math.ceil(self.NumberMachines - self.decisionCurrentCapacity))) - 1
        elif self.repair_c >= 1:
            Proactive = int(self.repair_c)
            # print self.repair_c, "----------", Proactive
            self.repair_c -= Proactive

            return Proactive

    def ReactRepair(self, Load, delta_Time, CurrentCapacity, proactive):
        TemporaryVariable = math.ceil(Load) - CurrentCapacity + 2
        if proactive != None:

            if float(Load) > CurrentCapacity:
                if TemporaryVariable > proactive:
                    TemporaryVariable -= proactive
                elif TemporaryVariable < proactive:
                    TemporaryVariable = proactive
                return TemporaryVariable
            else:
                return proactive
        else:
            return TemporaryVariable

    def evaluate(self, params):
        super(AdaptAutoscaler, self).evaluate(params)

        self.logger.log("Starting adapt scheduling policy")
        t1 = self.sim.ts_now
        t = self.t2 - t1
        #     monitor.startMonitoring()
        #    time.sleep(10)
        # print self.TasksList
        Server_Speed = self.SERVER_SPEED
        Current_Time = self.sim.ts_now
        # Current capacity = number of e.g. cores or threads in the total system
        CurrentCapacity = self.resource_manager.get_current_capacity()

        # Current load = amount of tasks running + in queues at sites
        Current_Load = self.system_monitor.get_total_load()

        D = 0.01 * CurrentCapacity
        LoadServers = int(math.ceil(float(Current_Load) / Server_Speed))
        #             print Load
        # print Load
        delta_Time = Current_Time - self.Time_Previous
        self.logger.log("Delta time {0}".format(delta_Time))
        self.Time_Previous = Current_Time
        x = Current_Time - self.Time_lastEstimation
        self.CapacityList += [(CurrentCapacity, delta_Time)]
        tempCapac = 0
        t1 = self.sim.ts_now

        if Current_Time - self.GammaTime >= math.ceil(self.delta_T):  # To be revised, Why 500 !
            for i in self.CapacityList:
                #                        print i[0],i[1],self.AvgCapacity,Current_Time,GammaTime
                tempCapac += i[0] * i[1]
                AvgCapacity = float(tempCapac) / (Current_Time - self.GammaTime)
            PreviousCapacity = CurrentCapacity
            self.GammaTime = Current_Time
            self.CapacityList = []
        if x >= self.delta_T:
            #         print "Load", LoadServers, "Capacity",CurrentCapacity
            Delta_Load = LoadServers - CurrentCapacity
            t_calc = Current_Time - self.initial_Time
            self.estimator(t_calc, Delta_Load, self.sigma_Alive, AvgCapacity, D)
            self.controller(delta_Time)
            self.Time_lastEstimation = Current_Time
            Previous_Load = Current_Load
        proactive = self.ProactiveRepair(Server_Speed, LoadServers, delta_Time, CurrentCapacity)
        results = self.ReactRepair(LoadServers, delta_Time, CurrentCapacity, proactive)
        self.sigma_Alive += CurrentCapacity
        self.t2 = self.sim.ts_now

        # BEGIN OF ADDED CODE

        self.logger.log("Final estimated result: {0}".format(results))

        mutation = 0

        if results > CurrentCapacity:
            self.autoscale_op = 1
            mutation = self.resource_manager.start_up_best_effort(results - CurrentCapacity)
        elif CurrentCapacity > results:
            self.autoscale_op = -1
            mutation = self.resource_manager.release_resources_best_effort(CurrentCapacity - results)

        self.log(CurrentCapacity, mutation, abs(CurrentCapacity - results))
        self.refresh_stats(results, CurrentCapacity + mutation * self.autoscale_op)

        self.sim.events.enqueue(
            SimCore.Event(
                self.sim.ts_now + self.N_TICKS_PER_EVALUATE,
                self.id,
                self.id,
                {'type': Constants.AUTO_SCALE_EVALUATE}
            )
        )
