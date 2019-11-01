#!/bin/sh
## This should be run in this folder (i.e. missioncontrol-v2)

export PYTHONNOUSERSITE=True
## see https://github.com/conda/conda/issues/7980 for activating conda in a bash script
eval "$(conda shell.bash hook)"
conda activate mc2
Rscript complete.runner.R 
## run as sh complete.runner.sh  2>&1 | tee logfile

