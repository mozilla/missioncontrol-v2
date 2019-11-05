#!/bin/bash

gcloud beta compute instances --project "moz-fx-dev-sguha-rwasm" start  "instance-1"
sleep 10
gcloud beta compute  --project "moz-fx-dev-sguha-rwasm" ssh  "instance-1" --command " cd /home/sguha/missioncontrol-v2 ; rm -rf logfile; sh complete.runner.sh  2>&1 | tee logfile"
rc=$?;
if [[ $rc -eq 0 ]];  then
    rm -rf /tmp/private
    rm -rf /tmp/public
    mkdir /tmp/private /tmp/public
    gsutil -m rsync -d -r gs://moz-fx-data-derived-datasets-analysis/sguha/missioncontrol-v2/html/private/ /tmp/private/ && rsync -avz /tmp/private/ ~/mz/missioncontrol/ex1/mc2/
    gsutil -m rsync -d -r gs://moz-fx-data-derived-datasets-analysis/sguha/missioncontrol-v2/html/public/ /tmp/public/ && rsync -avz /tmp/public/ ~/pubsguha/mc2/
else
    echo "ERROR ERROR"
fi   
gcloud beta compute instances --project "moz-fx-dev-sguha-rwasm" stop  "instance-1"   
