# DGSim

A cloud simulator written in Python 2.7.

## Installation
Make sure you have Python 2.7 and PIP installed and on your path.

To install all PIP dependencies, run this command from the base directory of this repo:
```commandline
pip install --user -r requirements.txt
```

## Usage
Run the simulator using `SystemSim.py` found in the `core` package.

```text
SystemSim.py [--quiet | --verbose] [-o FILE]
SystemSim.py <config_filename> [--quiet | --verbose] [-o FILE]
SystemSim.py <N_TICKS> --GWF=<file_or_folder> [--N=<clusters>] [--quiet | --verbose] [-o FILE]
SystemSim.py -h | --help
```

## Examples:
```text
SystemSim.py                             # Uses the 'default_config.ini'
SystemSim.py conf                        # Uses config file 'conf'
SystemSim.py 86400 --GWF=test1.gwf       # Uses default settings with N_TICKS=86400 and applies test1.gwf
                                         # to ClusterSetup.txt
SystemSim.py 86400 --GWF=test2.gwf --N 5 # Uses default settings with N_TICKS=86400 and applies test2.gwf
                                         # to the first 5 clusters defined in ClusterSetup.txt
```

## Options:
```text
--GWF=<file_or_folder>  A ./gwf/.gwf workflow file applied to clusters or a folder with .gwf files inside ./gwf/
--N=<clusters>          Apply the workflow to the first N clusters defined in ClusterSetup.txt
-v --verbose            Enable simulator debug logging on stdout
-q --quiet              Silence simulator output on stdout
-o FILE                 Save simulator output to file
-h --help               Show this screen.
```


## Tests
To run all unit tests, execute this command from the base directory:

```commandline
nosetests
```
