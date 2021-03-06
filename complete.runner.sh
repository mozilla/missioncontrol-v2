#!/bin/bash

set -euo pipefail
set -x

# change directory to path of script
cd `dirname "$0"`

# verify expected environment variables are present
export GCP_PROJECT_ID=${GCP_PROJECT_ID?}
export GCS_OUTPUT_PREFIX=${GCS_OUTPUT_PREFIX?}
export RAW_OUTPUT_TABLE=${RAW_OUTPUT_TABLE?}
export MODEL_OUTPUT_TABLE=${MODEL_OUTPUT_TABLE?}

# simple mode defaults to zero if unset
: "${SIMPLE:=0}"

# assign to a default if variables are unset.
: "${GOOGLE_APPLICATION_CREDENTIALS:=}"

# authenticate
if [[ -f "${GOOGLE_APPLICATION_CREDENTIALS}" ]]; then
    gcloud auth activate-service-account --key-file "${GOOGLE_APPLICATION_CREDENTIALS}"
else
    # https://cloud.google.com/kubernetes-engine/docs/tutorials/authenticating-to-cloud-platform
    echo "No JSON credentials provided, using default scopes."
fi

# validate that we are authenticated before proceeding
echo "Checking credentials..."
gsutil ls "${GCS_OUTPUT_PREFIX}"

echo "Running etl.R"
Rscript etl.R
echo "Running build.models.firefox.desktop.R"
Rscript build.models.firefox.desktop.R --simple=$SIMPLE --out=./all.the.data.intermediate.Rdata
echo "Running process.model.firefox.desktop.R"
Rscript process.model.firefox.desktop.R
echo "Running create.dashboards.static.R"
Rscript create.dashboards.static.R  --data_file=./all.the.data.Rdata --backup=1
