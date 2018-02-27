from schedulers.BestFitScheduler import BestFitScheduler
from schedulers.FillWorstFitScheduler import FillWorstFitScheduler
from schedulers.WorstFitScheduler import WorstFitScheduler


def get_scheduler_by_name(name):
    map = {
        'fillworstfit': FillWorstFitScheduler,
        'bestfit' : BestFitScheduler,
        'worstfit' : WorstFitScheduler,
    }

    return map[name] if name in map else None
