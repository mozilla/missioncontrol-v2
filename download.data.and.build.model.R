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
python data/crud.py main --creds_loc '{BQCREDS}'  --table_name missioncontrol_v2_raw_data --cache True --drop_first False --add_schema False
")
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
cr.cm.rel.f <- future({ make.a.model(d.rel,'cmr') })
cr.cc.rel.f <- future({ make.a.model(d.rel,'ccr') })
ci.cm.rel.f <- future({ make.a.model(d.rel,'cmi') })
ci.cc.rel.f <- future({ make.a.model(d.rel,'cci') })

cr.cm.rel <- label(value(cr.cm.rel.f),'cmr');loginfo("Finished Release cr.cm");
cr.cc.rel <- label(value(cr.cc.rel.f),'ccr');loginfo("Finished Release cr.cc");
ci.cm.rel <- label(value(ci.cm.rel.f),'cmi');loginfo("Finished Release ci.cm");
ci.cc.rel <- label(value(ci.cc.rel.f),'cci');loginfo("Finished Release ci.cc");

loginfo("Finished Release Models")
## Beta Model

loginfo("Started Beta Models")
d.beta <- dall.beta2
cr.cm.beta.f <- future({ make.a.model(d.beta,'cmr',channel='beta') })
cr.cc.beta.f <- future({ make.a.model(d.beta,'ccr',channel='beta') })
ci.cm.beta.f <- future({ make.a.model(d.beta,'cmi',channel='beta') })
ci.cc.beta.f <- future({ make.a.model(d.beta,'cci',channel='beta') })
cr.cm.beta <- label(value(cr.cm.beta.f),'cmr');loginfo("Finished Beta cr.cm");
cr.cc.beta <- label(value(cr.cc.beta.f),'ccr');loginfo("Finished Beta cr.cc");
ci.cm.beta <- label( value(ci.cm.beta.f),'cmi');loginfo("Finished Beta ci.cm");
ci.cc.beta <- label(value(ci.cc.beta.f),'cci');loginfo("Finished Beta ci.cc");
loginfo("Finished Beta Models")

## Nightly Model

loginfo("Started Nightly Models")
d.nightly <- dall.nightly2
cr.cm.nightly.f <- future({ make.a.model(d.nightly,'cmr',channel='nightly') })
cr.cc.nightly.f <- future({ make.a.model(d.nightly,'ccr',channel='nightly') })
ci.cm.nightly.f <- future({ make.a.model(d.nightly,'cmi',channel='nightly') })
ci.cc.nightly.f <- future({ make.a.model(d.nightly,'cci',channel='nightly') })
cr.cm.nightly <- label(value(cr.cm.nightly.f),'cmr');loginfo("Finished Nightly cr.cm");
cr.cc.nightly <- label(value(cr.cc.nightly.f),'ccr');loginfo("Finished Nightly cr.cc");
ci.cm.nightly <- label(value(ci.cm.nightly.f),'cmi');loginfo("Finished Nightly ci.cm");
ci.cc.nightly <- label(value(ci.cc.nightly.f),'cci');loginfo("Finished Nightly ci.cc");
loginfo("Finished Nightly Models")

loginfo("Finished Modelling")
