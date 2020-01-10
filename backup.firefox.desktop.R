setwd("~/missioncontrol-v2/")
source("missioncontrol.lib.R")


## Call as Rscript backup.firefox.desktop.R --backup=0  --data_file=default is ./all.the.data.Rdata 
command.line <- commandArgs(asValues=TRUE,defaults=list(backup=0,data_file="./all.the.data.Rdata",model_out="./desktop_model_output.fthr"),unique=TRUE)
loginfo(glue("loading data file from {command.line$data_file}"))
load(command.line$data_file)
backup.mode <- command.line$backup



runner <- glue('#!/bin/sh
## you need to have conda installed somewhere and a path to conda
## and hence remvoe the sguha in the following path
## also bigquery utils(bqutils) needs to be initialized/logged in  else the uploads will fail
# /home/sguha/anaconda3/bin/conda  activate mc2
cd mc2
python data/crud.py upload_model_data {command.line$model_out} --project_id "{GCP_PROJECT_ID}" --table_name=missioncontrol_v2_model_output')
writeLines(runner,con="./runner.sh")
if(backup.mode == 1){
    res  <- system2("sh", "./runner.sh",stderr=TRUE,stdout=TRUE)
    loginfo(paste(res, collapse="\n"))
    if(any(grepl("(Traceback|(E|e)xception|Error)",res))|| any(grepl("(f|F)ailed",res))){
        logerror("Problem with Uploading Model Results")
        stop("Problem Uploading Model Results")
    }else{
        loginfo("Successfully uploaded model results to missioncontrol_v2_model_output")
    }
}else{
    loginfo("Not running, just showing what would be run")
    loginfo(runner)
}


data.file <- glue("/tmp/models-{n}.Rdata",n=n)
system(glue("cp {command.line$data_file} {data.file}"))
loginfo(glue("Saving Data to temp file: {data.file}"))

if(backup.mode == 1){
    system(glue("gsutil cp {data.file} {GCS_OUTPUT_PREFIX}/archive/"))
    system(glue("gsutil cp {command.line$data_file} {GCS_OUTPUT_PREFIX}/archive/"))
    loginfo(glue("Data file saved at {GCS_OUTPUT_PREFIX}/archive/{data.file}. Download using gsutil cp"))
}else{
    loginfo("Not running, just showing what would be run")
    loginfo(glue("gsutil cp {data.file} {GCS_OUTPUT_PREFIX}/archive/"))
    loginfo(glue("gsutil cp {command.line$data_file} {GCS_OUTPUT_PREFIX}/archive/"))
    loginfo(glue("Data file saved at {GCS_OUTPUT_PREFIX}/archive/{data.file}. Download using gsutil cp"))
}
