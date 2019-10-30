#!/bin/sh
## This should be run in this folder (i.e. missioncontrol-v2)

export PYTHONNOUSERSITE=True
## see https://github.com/conda/conda/issues/7980
## for activating conda
eval "$(conda shell.bash hook)"
conda activate mc2
Rscript download.data.and.build.model.R 
Rscript process.model.and.build.board.R 
Rscript create.dashboards.static.R

## run as sh complete.runner.sh  2>&1 | tee logfile

