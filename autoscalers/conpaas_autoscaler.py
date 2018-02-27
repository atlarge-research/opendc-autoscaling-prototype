import math

from core import SimCore, Constants
from autoscalers.Autoscaler import Autoscaler
from autoscalers.conpaas_sources.performance import StatUtils
from autoscalers.conpaas_sources.prediction_models import Prediction_Models

# The code in this file is from the authors. We did NOT refactor it because this may compromise the original workings
# of the autoscaler.
# We did add code at the end of the evaluate function to scale up/down according to the results.
# And modify the global parameters into class fields.
from core import SimCore, Constants


class ConpaasAutoscaler(Autoscaler):
    def __init__(self, simulator, logger):
        super(ConpaasAutoscaler, self).__init__(simulator, 'ConPaaS', logger)

        self.NumberOfRequests = 0
        self.performance_predictor = Prediction_Models()
        self.stat_utils = StatUtils()
        self.p = 0
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
        self.forecast_req_rate_model_selected = 0
        self.forecast_req_rate_predicted = 0
        self.forecast_list_req_rate = {}
        self.t2 = self.sim.ts_now
        self.LoadServers = []
        self.Time_Previous = self.sim.ts_now
        self.Time_lastEstimation = self.sim.ts_now
        self.CapacityList = []

    def prediction_evaluation(self, req_rate_data):
        data_req_rate_filtered = req_rate_data

        async_result_req_ar = self.performance_predictor.auto_regression(data_req_rate_filtered, 20)
        async_result_req_lr = self.performance_predictor.linear_regression(data_req_rate_filtered, 20)
        async_result_req_exp_smoothing = self.performance_predictor.exponential_smoothing(data_req_rate_filtered, 2)

        self.forecast_list_req_rate[1] = async_result_req_lr
        self.forecast_list_req_rate[2] = async_result_req_exp_smoothing
        self.forecast_list_req_rate[0] = async_result_req_ar
        #  self.forecast_list_req_rate[0] = async_result_req_arma.get()


        #             try:
        #             print "Getting the forecast request rate for the best model in the previous iteration " + str(self.forecast_req_rate_model_selected)
        weight_avg_predictions = self.stat_utils.compute_weight_average(
            self.forecast_list_req_rate[self.forecast_req_rate_model_selected])

        #             if weight_avg_predictions > 0:
        self.forecast_req_rate_predicted = weight_avg_predictions

        #             print "Prediction request rate for model " + str(self.forecast_req_rate_model_selected) + "--  Prediction req. rate: " + str(self.forecast_req_rate_predicted)
        return weight_avg_predictions
        #             except Exception as e:
        #                 print "Warning trying to predict a future value for the model." + str(e)

    def get_req_rate_prediction(self):
        return self.forecast_req_rate_predicted

    def evaluate(self, params):
        super(ConpaasAutoscaler, self).evaluate(params)
        # global t2, LoadServers, Time_Previous, Time_lastEstimation, CapacityList
        t1 = self.sim.ts_now
        t = self.t2 - t1
        #     monitor.startMonitoring()
        #    time.sleep(10)
        # print self.TasksList
        Server_Speed = self.SERVER_SPEED
        Current_Time = self.sim.ts_now
        CurrentCapacity = self.resource_manager.get_current_capacity()
        Current_Load = self.system_monitor.get_total_load()

        LoadTotalServers = int(math.ceil(float(Current_Load) / Server_Speed))
        self.LoadServers += [Current_Load]
        #             print Load
        # print Load
        delta_Time = Current_Time - self.Time_Previous
        Time_Previous = Current_Time
        x = Current_Time - self.Time_lastEstimation
        self.CapacityList += [(CurrentCapacity, delta_Time)]
        tempCapac = 0
        t1 = self.sim.ts_now

        if len(self.LoadServers) < 21:
            Predicted = math.ceil(Current_Load / float(Server_Speed))
        else:
            if Current_Load > Server_Speed:
                Predicted = math.ceil(self.prediction_evaluation(self.LoadServers) / Server_Speed)
                if Predicted == 0 or Predicted == None:
                    Predicted = math.ceil(CurrentCapacity)
            else:
                Predicted = 1
                self.LoadServers.pop(0)

        t2 = self.sim.ts_now
        t2 = self.sim.ts_now

        mutation = 0

        # Added logic to perform up and downscaling of resouces
        if CurrentCapacity > Predicted:
            self.autoscale_op = -1
            mutation = self.resource_manager.release_resources_best_effort(CurrentCapacity - Predicted)
        elif Predicted > CurrentCapacity:
            self.autoscale_op = 1
            mutation = self.resource_manager.start_up_best_effort(Predicted - CurrentCapacity)

        self.log(CurrentCapacity, mutation, abs(CurrentCapacity - Predicted))
        self.refresh_stats(Predicted, CurrentCapacity + mutation * self.autoscale_op)

        self.sim.events.enqueue(
            SimCore.Event(
                self.sim.ts_now + self.N_TICKS_PER_EVALUATE,
                self.id,
                self.id,
                {'type': Constants.AUTO_SCALE_EVALUATE}
            )
        )
