#!/usr/bin/env python2.7

import logging
import os
import sys

from core.SimLogger import setup_logging


def run_script(script):
    os.system("python experiments/ccgrid_2018/{0}".format(script))


if __name__ == "__main__":
    setup_logging(logging.StreamHandler(sys.stdout), logging.INFO)

    scripts = [
        "alexey_icpe2017_workload1_experiment.py",
        "bursty_workload_experiment.py",
        "diff_alloc_policies_experiment.py",
        "scale_experiment.py"
    ]

    for script in scripts:
        run_script(script)
