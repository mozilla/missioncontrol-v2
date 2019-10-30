#!/bin/sh

export PYTHONNOUSERSITE=True
conda activate mc2
Rscript download.data.and.build.model.R > logfile 2>&1
Rscript process.model.and.build.board.R >> logfile 2>&1
Rscript create.dashboards.static.R >> logfile 2>&1

