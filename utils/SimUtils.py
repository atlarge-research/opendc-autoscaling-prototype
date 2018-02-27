import csv
import datetime
import inspect
import io
import json
import logging
import os
import sys
from collections import deque
from operator import attrgetter

import toposort
from configobj import ConfigObj, flatten_errors, get_extra_values

import ProjectUtils
from core.Task import Task
from core.Workflow import Workflow
from validate import Validator

DATE_FORMAT = '%Y-%m-%d/%H:%M:%S'

GWF_FOLDER = 'gwf'
GWF_EXTENSION = '.gwf'

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def current_date_time():
    path_friendly_date_format = DATE_FORMAT.replace('/', '_')
    return datetime.datetime.now().strftime(path_friendly_date_format)

def save_config(config):
     config.filename = os.path.join(get_output(config), 'config.ini')
     config.write()

def load_config(filename, config_schema):
    config = ConfigObj(
        filename, 
        file_error=True, 
        configspec=config_schema
    )

    validate_config(config, config_schema)

    return config

def read_sim_info(config):
    experiment_config = config['experiment']
    simulator_map_filename = experiment_config.get('SimulatorMap', 'SimulatorMap.json')
    simulator_map_path = os.path.join(
        ProjectUtils.root_path,
        simulator_map_filename
    )

    with open(simulator_map_path) as simulator_map:
        return json.load(simulator_map)

def generate_config(N_TICKS, config_schema, ClusterSetup=None, N_CLUSTERS=None, GWF=None):
    if not N_TICKS or not config_schema:
        raise ValueError('Both N_TICKS and config_schema must be set')

    config = ConfigObj(configspec = config_schema)
    config['simulation'] = {
        'N_TICKS': N_TICKS,
    }

    if ClusterSetup:
        config['simulation']['ClusterSetup'] = ClusterSetup
    if N_CLUSTERS:
        config['simulation']['N_CLUSTERS'] = N_CLUSTERS
    if GWF:
        config['simulation']['GWF'] = GWF

    validate_config(config, config_schema)

    return config

def validate_config(config, config_schema):
    # to validate int_list() make sure you
    # have a trailing comma in your config
    # file (e.g: "Indices = 2,")

    # TODO: add validate steps that check there is at least on GWF value(either filename or clustername...)
    result = config.validate(Validator(), preserve_errors=True)

    err = flatten_errors(config, result)
    if err:
        sys.exit('Unable to validate config file:\n%s' % err)

    # everything that is extra to the config schema will show up here;
    # these settings won't be used
    extra_values = get_extra_values(config)
    if extra_values:
        logger.debug('Not enforced by schema: %s' % extra_values)

def get_output(config):
    experiment_config = config.get('experiment', {})
    simulation_config = config.get('simulation', {})

    output = os.path.join(
        ProjectUtils.root_path,
        simulation_config.get('OUTPUT_DIR', ''),
        experiment_config.get('ID', ''),
        experiment_config.get('ITERATION', '')
    )

    if not os.path.exists(output) or not os.path.isdir(output):
        os.makedirs(output)

    return output

class ClusterInfo:
    def __init__(self, ClusterID, Cluster, NProcs, ResourceSpeed=1.0):
        self.ClusterID = ClusterID
        self.Cluster = Cluster
        self.NProcs = NProcs
        self.ResourceSpeed = ResourceSpeed

def read_cluster_setup(csv_filename):
    clusters = []
    gwf_filenames = []
    with open(csv_filename) as csvfile:
        content = csvfile.read().replace(' ', '')
        datastring = io.StringIO(unicode(content))
        reader = csv.DictReader(datastring)
        for row in reader:
            clusters.append(ClusterInfo(
                row['ClusterID'],
                row['Cluster'],
                int(row['Resource']),
                float(row['Speed'])
                )
            )
            if row['Gwf']:
                gwf_filenames.append(row['Gwf'])

    return (clusters, gwf_filenames)


def calculate_critical_path_length(workflow):
    id_dependencies_map = dict((task.id, task.dependencies) for task in workflow.tasks)
    id_runtime_map = dict((task.id, task.runtime) for task in workflow.tasks)
    id_submit_map = dict((task.id, task.ts_submit) for task in workflow.tasks)

    sorted_ids = toposort.toposort(id_dependencies_map)

    finish_times = {}
    for ids in sorted_ids:
        for _id in ids:
            parents= id_dependencies_map[_id]
            runtime = id_runtime_map[_id]
            submit_time = id_submit_map[_id]
            if parents:
                critical_parent = max(finish_times[parent] for parent in parents)
            else:
                critical_parent = 0

            finish_time = max(critical_parent, submit_time) + runtime
            finish_times[_id] = finish_time

    return max(finish_times.values()) - min(id_submit_map.values())


def calculate_critical_path_length2(workflow):
    """
    In addition to critical path length (aggregated runtimes of longest path in workflow),
    also return the number of tasks of said path.
    """

    get_id_from_finish_time = lambda finish_time: finish_times.keys()[finish_times.values().index(finish_time)]

    id_dependencies_map = dict((task.id, task.dependencies) for task in workflow.tasks)
    id_runtime_map = dict((task.id, task.runtime) for task in workflow.tasks)
    id_submit_map = dict((task.id, task.ts_submit) for task in workflow.tasks)
    sorted_ids = toposort.toposort(id_dependencies_map)

    finish_times = {}
    path_lengths = {}
    for ids in sorted_ids:
        for _id in ids:
            parents= id_dependencies_map[_id]
            runtime = id_runtime_map[_id]
            submit_time = id_submit_map[_id]

            if parents:
                critical_parent_finish_time = max(finish_times[parent] for parent in parents)
                path_lengths[_id] = path_lengths[get_id_from_finish_time(critical_parent_finish_time)] + 1
            else:
                critical_parent_finish_time = 0
                path_lengths[_id] = 1

            finish_time = max(critical_parent_finish_time, submit_time) + runtime
            finish_times[_id] = finish_time

    max_finish_time = max(finish_times.values())
    task_count = path_lengths[get_id_from_finish_time(max_finish_time)]

    return (max_finish_time - min(id_submit_map.values()), task_count)

def create_from_gwf(row, submission_site, workflow_id=None):
    return Task(
        row['task_id'],
        row['ts_submit'],
        submission_site,
        row['runtime'],
        row['cpus'],
        row['dependencies'],
        workflow_id=workflow_id
    )


def read_tasks(clusters, gwf_filenames):
    workflows = {}
    tasks = []

    first_task_id = 0
    current_workflow_id = None
    prev_workflow_id_task_count = 0
    for index, gwf_filename in enumerate(gwf_filenames):
        prev_gwf_workflow_id = None

        cluster_id = index % len(clusters)
        cluster_tasks = []
        tasks_by_id = {}
        logger.debug('Cluster {0} uses {1}'.format(cluster_id, gwf_filename))

        for row in rows_from_gwf(gwf_filename):
            if row['workflow_id'] != None:
                if prev_gwf_workflow_id != row['workflow_id']:  # if True, we've reached a new workflow
                    prev_gwf_workflow_id = row['workflow_id']

                    # update current_workflow_id
                    if current_workflow_id == None:
                        current_workflow_id = 0
                    else:
                        current_workflow_id += 1

                        first_task_id += prev_workflow_id_task_count
                        prev_workflow_id_task_count = 0  # reset count; count has already been aggregated to first_task_id

                prev_workflow_id_task_count += 1

            row['task_id'] += first_task_id
            row['dependencies'] = set(dependency + first_task_id for dependency in row['dependencies'])

            task = create_from_gwf(row, cluster_id, current_workflow_id)
            tasks_by_id[task.id] = task

            cluster_tasks.append(task)

        for task in cluster_tasks:
            # append task to its workflow
            workflow_id = task.workflow_id
            if workflow_id is not None:
                if workflow_id not in workflows:
                    workflows[workflow_id] = Workflow(workflow_id, None, [])
                workflows[workflow_id].tasks.append(task)

            for dependency in task.dependencies:
                other_task = tasks_by_id[dependency]
                other_task.children.append(task)
                task.parents.append(other_task)

        tasks.extend(cluster_tasks)

        logger.info('Read {0} tasks for cluster {1}'.format(len(cluster_tasks), cluster_id))

    # fill in ts_submit and critical_path_length for all workflows
    for workflow in workflows.values():
        first_entry_task = min(workflow.tasks, key=attrgetter('ts_submit'))  # workflows can have multiple entry nodes
        workflow.ts_submit = first_entry_task.ts_submit
        workflow.critical_path_length, workflow.critical_path_task_count = calculate_critical_path_length2(workflow)
    logger.info('{0} workflows have been found'.format(len(workflows)))

    return workflows, tasks

def rows_from_gwf(gwf_filename):
    with open(gwf_filename, 'r') as gwf_file:
        csv_reader = csv.DictReader(gwf_file)
        for row in csv_reader:
            row = dict((key.strip(), value.strip()) for key, value in row.items())
            yield {
                'workflow_id': int(row['WorkflowID']) if row['WorkflowID'] else None,
                'task_id': int(row['JobID']),
                'ts_submit': int(row['SubmitTime']),
                'runtime': int(row['RunTime']),
                'cpus': int(row['NProcs']),
                'dependencies': set(int(dependency) for dependency in row['Dependencies'].split())
            }

def prepend_gwf_path(gwf_filename):
    return os.path.join(ProjectUtils.root_path, GWF_FOLDER, gwf_filename)

def get_hour_and_day_for_ts(ts):
    return int(ts / 3600), int(ts / (24 * 3600))

def add_file_logging(name, filename, config):
    frame = inspect.stack()[1]
    calling_module = inspect.getmodule(frame[0])
    logger = logging.getLogger('{}.{}'.format(calling_module.__name__, name))

    remove_logger_handlers(logger)

    full_path = os.path.join(get_output(config), filename)
    filelog = logging.FileHandler(full_path, 'w+')

    logger.addHandler(filelog)
    logger.setLevel(logging.INFO)
    logger.propagate = 0

    return logger

def remove_logger_handlers(logger, logging_handler=None):
    """
    Remove handler(s) from logger.
    If logging_handler is omitted, removes all handlers from logger.
    """

    for handler in logger.handlers:
        if logging_handler and logging_handler is not handler:
            continue

        handler.flush()
        logger.removeHandler(handler)
        handler.close()


def get_gwf_files(file_or_folder):
    """
    Returns a list with a gwf file or a list with
    the gwf files contained in file_or_folder.
    Filters out dirs in file_or_folder or files that don't have gwf extension.
    Sorts files at the end, this helps in some case, for ex: add_workflow_id_column
    script assigns workflow ids that match gwf's prefix if it has one (001_alexey_...)
    """

    files = []
    if os.path.isfile(file_or_folder) and file_or_folder.endswith(GWF_EXTENSION):
        files = [file_or_folder]
    else:
        files = [os.path.join(file_or_folder, f) for f in os.listdir(file_or_folder)]
        files = [f for f in files if os.path.isfile(f) and f.endswith(GWF_EXTENSION)]  # filter files
        files.sort()

    return files

def subset_closest_to_sum(lst, target, key=lambda x: x, with_duplicates=False, gt=True):
    """
    Returns subset that sums up to target or the closest one to it. Assumes lst is sorted.
    
    with_duplicates=True will consider an existing item multiple times.
    gt=True will consider closest greater sum

    e.g.
        subset_with_sum([1], 8)
        None

        subset_with_sum([1], 8, True)
        [1, 1, 1, 1, 1, 1, 1, 1]

        subset_with_sum([1, 2], 8, True)
        [2, 2, 2, 2]

        subset_with_sum([1, 2, 3, 2, 1], 7)
        [2, 3, 2]

        subset_with_sum([(1, 0), (2, 1), (3, 2)], 4, \
            key=lambda x: x[0], with_duplicates=False)
        [(1, 0), (3, 2)]
    """

    if not with_duplicates and sum(map(key, lst)) <= target:
        return lst

    def _subset_with_sum(lst, target, gt=True):
        reachable = {0: []}

        closest_sum = None
        closest_lst = []
        for item in lst:
            for number in sorted(reachable.keys(), reverse=True):
                result = key(item) + number

                if result > target:
                    if gt and (not closest_sum or result < closest_sum):
                        closest_sum = result
                        closest_lst = reachable[number] + [item]
                    continue
                elif result == target:
                    return reachable[number] + [item]
                else:
                    if not gt and (not closest_sum or result > closest_sum):
                        closest_sum = result
                        closest_lst = reachable[number] + [item]
                    reachable[result] = reachable[number] + [item]

        return closest_lst

    def _subset_with_sum_with_duplicates(lst, target):
        reachable = {0: []}
        added_something = True
        closest_sum = None
        closest_lst = []

        while added_something:
            added_something = False
            for number in sorted(reachable.keys(), reverse=True):
                for item in sorted(lst, key=key, reverse=True):
                    result = key(item) + number

                    if result > target:
                        if not closest_sum or result < closest_sum:
                            closest_sum = result
                            closest_lst = reachable[number] + [item]

                        continue
                    else:
                        if result not in reachable:
                            added_something = True
                            reachable[result] = reachable[number] + [item]
                        elif len(reachable[number]) + 1 <  len(reachable[result]):
                            added_something = True
                            reachable[result] = reachable[number] + [item]
        return closest_lst if target not in reachable else reachable[target]

    return _subset_with_sum(lst, target, gt) if not with_duplicates \
        else _subset_with_sum_with_duplicates(lst, target)

def subset_closest_to_sum2(lst, target, key=lambda x: x, key2=lambda x: x):
    """
    Similar to subset_closest_to_sum but uses key2 func to choose between two equal sets.
    The set with the smaller sum by key2 is chosen.

    e.g.
        subset_closest_to_sum2([(3, 5), (3, 1), (2, 2), (2, 3), (2, 0), (2, 5)], 9, key=lambda x: x[0], key2=lambda x: x[1])
        [(3, 1), (2, 2), (2, 3), (2, 0)]

        subset_closest_to_sum2([(2, 3), (2, 5), (2, 1), (2, 4), (2, 3), (2, 0)], 8, key=lambda x: x[0], key2=lambda x: x[1])
        [(2, 3), (2, 1), (2, 3), (2, 0)]
    """

    def _sum(lst):
        return sum(map(key2, lst))

    reachable = {0: []}

    closest_list = []
    closest_sum = None

    exact_match = []

    for item in lst:
        # We traverse in reversed order all reachable resource numbers
        # The order is reversed so that elements are not added multiple times to the same combination
        for number in sorted(reachable.keys(), reverse=True):
            result = key(item) + number
            result_list = reachable[number] + [item]

            if result > target:
                continue
            elif result == target:
                if not exact_match or _sum(exact_match) > _sum(result_list):
                    exact_match = result_list
            else:
                if not closest_sum or closest_sum < result or (closest_sum == result and _sum(closest_list) > _sum(result_list)):
                    closest_sum = result
                    closest_list = result_list
                if result not in reachable or _sum(reachable[result]) > _sum(result_list):
                    reachable[result] = result_list

    return exact_match if exact_match else closest_list
