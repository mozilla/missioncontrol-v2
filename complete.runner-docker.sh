#!/bin/bash

set -euo pipefail
set -x

# verify expected environment variables are present
export GCP_PROJECT_ID=${GCP_PROJECT_ID?}
export GCS_OUTPUT_PREFIX=${GCS_OUTPUT_PREFIX?}
export GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS?}

# debug mode defaults to zero if unset
: "${DEBUG:=0}"

# authenticate
gcloud auth activate-service-account --key-file "${GOOGLE_APPLICATION_CREDENTIALS}"

echo "Running etl.R"
Rscript etl.R
echo "Running build.models.firefox.desktop.R"
Rscript build.models.firefox.desktop.R --debug=$DEBUG --out=./all.the.data.intermediate.Rdata
echo "Running process.model.firefox.desktop.R"
Rscript process.model.firefox.desktop.R
echo "Running create.dashboards.static.R"
Rscript create.dashboards.static.R  --data_file=./all.the.data.Rdata --backup=1
