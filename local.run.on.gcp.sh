#!/bin/bash
gcloud beta compute instances --project "moz-fx-dev-sguha-rwasm" start  "instance-1" --zone=us-central1-c
sleep 10
gcloud beta compute  --project "moz-fx-dev-sguha-rwasm" ssh  "instance-1"  --command " cd /home/sguha/missioncontrol-v2 ; rm -rf logfile; sh complete.runner.sh  2>&1 | tee logfile" --zone=us-central1-c
rc=$?;
rc=0


if [[ -z "$gcstop" ]]; then
   gcloud beta compute instances --project "moz-fx-dev-sguha-rwasm" stop  "instance-1"   --zone=us-central1-c
 else
     echo "Not stopping instance-1"
fi     
