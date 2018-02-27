import numpy
import pandas as pd

# COLOR_SCHEMA = [
#     '#ff1a1a',  # Torch Red
#     '#ffb319',  # Orange
#     '#b2ff19',  # Green Yellow
#     '#19ff1a',  # Free Speech Green
#     '#1ab2ff',  # Deep Sky Blue
#     '#1a19ff',  # Blue
#     '#b21aff',  # Electric Purple
#     '#ff1ab3',  # Spicy Pink
#     '#1bffb2',  # Medium Spring Green
#     '#FF0000',  # Red
# ]

COLOR_SCHEMA = [
    '#000000',
    '#404040',
    '#606060',
    '#808080',
    '#A0A0A0',
    '#C0C0C0',
    '#E0E0E0',
    '#ff1ab3',
    '#1bffb2',
    '#FF0000',
]

MARKERS = [
    'o',
    '+',
    '*',
    's',
    'p',
    'x',
    '^',
    '.',
]


def load_from_autoscale_ops(autoscale_ops_file, ts_now_col=False, supply_col=False, demand_col=False,
                            pending_tasks_col=False):
    columns_requested = []

    header = ['ts_now', 'supply', 'demand', 'pending_tasks']
    if ts_now_col:
        columns_requested.append('ts_now')
    if supply_col:
        columns_requested.append('supply')
    if demand_col:
        columns_requested.append('demand')
    if pending_tasks_col:
        columns_requested.append('pending_tasks')
    if not columns_requested:
        return {}

    df = pd.read_csv(autoscale_ops_file, names=header)

    df = df.loc[0:, df.columns.isin(columns_requested)]
    data = df.transpose().values.tolist()

    d = {}
    for index, column in enumerate(columns_requested):
        d[column] = data[index]
    return d


def load_from_user_metrics(user_metrics_file, id_col=False, makespan_col=False, response_time_col=False,
                           critical_path_col=False, normalized_schedule_length=False):
    columns_requested = []

    header = ['id', 'makespan', 'response_time', 'critical_path']
    if id_col:
        columns_requested.append('id')
    if makespan_col or normalized_schedule_length:
        columns_requested.append('makespan')
    if response_time_col:
        columns_requested.append('response_time')
    if critical_path_col or normalized_schedule_length:
        columns_requested.append('critical_path')
    if not columns_requested:
        return {}

    df = pd.read_csv(user_metrics_file, names=header, delim_whitespace=True, skiprows=1)
    df = df.loc[0:, df.columns.isin(columns_requested)]
    data = df.transpose().values.tolist()

    d = {}
    for index, column in enumerate(columns_requested):
        d[column] = data[index]

    # Compute the Normalized Schedule Length:
    if normalized_schedule_length:
        critical_path = sum(d['critical_path'])
        makespan = sum(d['makespan'])

        d['normalized_schedule_length'] = float(makespan) / float(critical_path)

    return d
