setwd("~/missioncontrol-v2/")
source("missioncontrol.lib.R")

load("all.the.data.Rdata")


toBq <- local({
    keepOnlyLast <- FALSE
    rbind(
    fittedTableForBQ(dall.rel2, model=list( mr=cr.cm.rel,cr=cr.cc.rel,mi=ci.cm.rel,ci=ci.cc.rel),last=keepOnlyLast),
    fittedTableForBQ(dall.beta2, model=list( mr=cr.cm.beta,cr=cr.cc.beta,mi=ci.cm.beta,ci=ci.cc.beta),last=keepOnlyLast),
    fittedTableForBQ(dall.nightly2, model=list( mr=cr.cm.nightly,cr=cr.cc.nightly,mi=ci.cm.nightly,ci=ci.cc.nightly),last=keepOnlyLast)
    )
})

toBq[, "model_date" := n]
atm <- tempfile(fileext='.fth')
write_feather(toBq,path=atm)
runner <- glue('#!/bin/sh
## you need to have conda installed somewhere and a path to conda
## and hence remvoe the sguha in the following path
## also bigquery utils(bqutils) needs to be initialized/logged in  else the uploads will fail
# /home/sguha/anaconda3/bin/conda  activate mc2
cd mc2
python data/crud.py upload_model_data {atm}  --creds_loc "{BQCREDS}" --table_name=missioncontrol_v2_model_output',atm=atm)
writeLines(runner,con="./runner.sh")
res  <- system2("sh", "./runner.sh",stderr=TRUE,stdout=TRUE)
loginfo(paste(res, collapse="\n"))
if(any(grepl("(E|e)xception",res))|| any(grepl("(f|F)ailed",res))){
    logerror("Problem with Uploading Model Results")
    stop("Problem Uploading Model Results")
}else{
    loginfo("Successfully uploaded model results to missioncontrol_v2_model_output_test")
}


system(glue("gsutil cp {data.file}  gs://moz-fx-data-derived-datasets-analysis/sguha/missioncontrol-v2/archive/"))
system(glue("gsutil cp all.the.data.Rdata  gs://moz-fx-data-derived-datasets-analysis/sguha/missioncontrol-v2/archive/"))
loginfo(glue("Data file saved at   gs://moz-fx-data-derived-datasets-analysis/sguha/missioncontrol-v2/archive/{data.file}. Download using gsutil cp"))
