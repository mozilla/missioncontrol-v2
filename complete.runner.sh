#!/bin/sh

export PYTHONNOUSERSITE=True
conda activate mc2
Rscript download.data.and.build.model.R
Rscript process.model.and.build.board.R
Rscript create.dashboards.static.R
