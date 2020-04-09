setwd("~/missioncontrol-v2/")
source("missioncontrol.lib.R")



loginfo("Starting Downloading Data")

runner <- glue("#!/bin/sh
## you need to have conda installed somewhere and a path to conda
## and hence remvoe the sguha in the following path
## also bigquery utils(bqutils) needs to be initialized/logged in  else the uploads will fail
# /home/sguha/anaconda3/bin/conda  activate mc2
cd mc2
#python data/crud.py main --creds_loc '{BQCREDS}' --table_name missioncontrol_v2_raw_data
python data/crud.py main --creds_loc '{BQCREDS}' --table_name missioncontrol_v2_raw_data --project_id=\"moz-fx-data-derived-datasets\" --base_project_id=\"moz-fx-data-bq-data-science\"
")
writeLines(runner,con="./runner.sh")
res  <- system2("sh", "./runner.sh",stderr=TRUE,stdout=TRUE)
loginfo(paste(res, collapse="\n"))
if(any(grepl("(Traceback|(E|e)xception|Error)",res))|| any(grepl("(f|F)ailed",res))){
    logerror("Problem with Creating Raw Data")

    stop("Problem with Creating Raw Data")
}

loginfo("Finished Downloading Data")

0
