setwd("~/missioncontrol-v2/")
source("missioncontrol.lib.R")

BQCREDS <- "~/gcloud.json"

loginfo("Starting Downloading Data")

runner <- glue("#!/bin/sh
## you need to have conda installed somewhere and a path to conda
## and hence remvoe the sguha in the following path
## also bigquery utils(bqutils) needs to be initialized/logged in  else the uploads will fail
# /home/sguha/anaconda3/bin/conda  activate mc2
cd mc2
python data/crud.py main --creds_loc '{BQCREDS}'  --table_name missioncontrol_v2_raw_data")
writeLines(runner,con="./runner.sh")
res  <- system2("sh", "./runner.sh",stderr=TRUE,stdout=TRUE)
loginfo(paste(res, collapse="\n"))
if(any(grepl("exception",res))) {
    logerror("Problem with Creating Raw Data")

    stop("Problem with Creating Raw Data")
}

loginfo("Finished Downloading Data")


getModelDataForChannel <- function(ch, v,asfeather=FALSE){
    rtemp <- tempfile()
    runner <- glue("#!/bin/sh
## you need to have conda installed somewhere and a path to conda
## and hence remvoe the sguha in the following path
## also bigquery utils(bqutils) needs to be initialized/logged in  else the uploads will fail
## /home/sguha/anaconda3/bin/conda  activate mc2
cd mc2
python data/crud.py dl_raw --creds_loc {BQCREDS}  --channel {ch} --n_majors {v} --cache False --outname '{rtemp}'
")
    writeLines(runner,con="./runner.sh")
    loginfo(glue("Starting Gettting Model Data for channel {ch} and nversions {v}"))
    res  <- system2("sh", "./runner.sh",stderr=TRUE,stdout=TRUE)
    loginfo(paste(res, collapse="\n"))
    if(any(grepl("(E|e)xception",res))) {
        logerror(glue("Problem with Downloading Model Data for channel {ch}"))
        stop(glue("Problem with Downloading Model Data for channel {ch}"))
    }

    loginfo(glue("Finished Gettting Model Data for channel {ch} and nversions {v}"))
    if(asfeather) feather(rtemp)
    else{
        a <- data.table(data.frame(feather(rtemp)))
        a[, date:=as.Date(date)]
        a
    }
}

dall.rel2 <- data.table(getModelDataForChannel("release",3))[nvc>0,]
dall.beta2 <- data.table(getModelDataForChannel("beta",3))[nvc>0,]
dall.nightly2 <- data.table(getModelDataForChannel("nightly",3))[nvc>0,]


## BUILD MODELS
loginfo("Started Release Models")
## Release model
d.rel <- dall.rel2
cr.cm.rel %<-% make.a.model(d.rel,'cmr',channel='release')
cr.cc.rel %<-% make.a.model(d.rel,'ccr',channel='release')
ci.cm.rel %<-% make.a.model(d.rel,'cmi',channel='release')
ci.cc.rel %<-% make.a.model(d.rel,'cci',channel='release')

cr.cm.rel <- label(cr.cm.rel,'cmr');loginfo("Finished Release cr.cm");
cr.cc.rel <- label(cr.cc.rel,'ccr');loginfo("Finished Release cr.cc");
ci.cm.rel <- label(ci.cm.rel,'cmi');loginfo("Finished Release ci.cm");
ci.cc.rel <- label(ci.cc.rel,'cci');loginfo("Finished Release ci.cc");

loginfo("Finished Release Models")
## Beta Model

loginfo("Started Beta Models")
d.beta <- dall.beta2
cr.cm.beta %<-% make.a.model(d.beta,'cmr',channel='beta') 
cr.cc.beta %<-% make.a.model(d.beta,'ccr',channel='beta') 
ci.cm.beta %<-% make.a.model(d.beta,'cmi',channel='beta') 
ci.cc.beta %<-% make.a.model(d.beta,'cci',channel='beta') 
cr.cm.beta <- label(cr.cm.beta,'cmr');loginfo("Finished Beta cr.cm");
cr.cc.beta <- label(cr.cc.beta,'ccr');loginfo("Finished Beta cr.cc");
ci.cm.beta <- label(ci.cm.beta,'cmi');loginfo("Finished Beta ci.cm");
ci.cc.beta <- label(ci.cc.beta,'cci');loginfo("Finished Beta ci.cc");
loginfo("Finished Beta Models")

## Nightly Model

loginfo("Started Nightly Models")
d.nightly <- dall.nightly2
cr.cm.nightly %<-% make.a.model(d.nightly,'cmr',channel='nightly',iter=8000,thin=5)
cr.cc.nightly %<-% make.a.model(d.nightly,'ccr',channel='nightly',iter=8000,thin=5)
ci.cm.nightly %<-% make.a.model(d.nightly,'cmi',channel='nightly',iter=8000,thin=5)
ci.cc.nightly %<-% make.a.model(d.nightly,'cci',channel='nightly',iter=8000,thin=5)
cr.cm.nightly <- label(cr.cm.nightly,'cmr');loginfo("Finished Nightly cr.cm");
cr.cc.nightly <- label(cr.cc.nightly,'ccr');loginfo("Finished Nightly cr.cc");
ci.cm.nightly <- label(ci.cm.nightly,'cmi');loginfo("Finished Nightly ci.cm");
ci.cc.nightly <- label(ci.cc.nightly,'cci');loginfo("Finished Nightly ci.cc");
loginfo("Finished Nightly Models")

loginfo("Finished Modelling")
