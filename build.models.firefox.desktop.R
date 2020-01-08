source("missioncontrol.lib.R")

##@param ch: channel
##@param v: number of versions to get
##@param asfeather: rreturn feather file
##@param input_file: if present, read data from here(must be feather)
getModelDataForChannel <- function(ch, v, input_file = NULL,asfeather=FALSE){
    if(!is.null(input_file)){
        loginfo(glue("Using raw data from file {input_file} for channel {ch}"))
        a <- data.table(data.frame(feather(input_file)))
        a[, date:=as.Date(date)]
        a
    }else{
        loginfo(glue("Downloading raw data for channel {ch}"))
        rtemp <- tempfile()
        runner <- glue("#!/bin/sh
## you need to have conda installed somewhere and a path to conda
## and hence remvoe the sguha in the following path
## also bigquery utils(bqutils) needs to be initialized/logged in  else the uploads will fail
## /home/sguha/anaconda3/bin/conda  activate mc2
cd mc2
python data/crud.py dl_raw --base_project_id {GCP_PROJECT_ID} --project_id {GCP_PROJECT_ID} --creds_loc {BQCREDS}  --channel {ch} --n_majors {v} --cache False --outname '{rtemp}'
")
        writeLines(runner,con="./runner.sh")
        loginfo(glue("Starting Gettting Model Data for channel {ch} and nversions {v}"))
        res  <- system2("sh", "./runner.sh",stderr=TRUE,stdout=TRUE)
        loginfo(paste(res, collapse="\n"))
        if(any(grepl("(E|e)xception",res))|| any(grepl("(f|F)ailed",res))){
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
}

## You can call this like
## this will run debug models, use the release feather as model input and download for other channels
## Rscript build.models.firefox.desktop.R --debug=1 --release_raw=path-to-release-feather
## You can also name the output file (default is ./all.the.data.intermediate.Rdata)
## Rscript build.models.firefox.desktop.R --debug=1 --out=./all.the.data.intermediate.Rdata

command.line <- commandArgs(asValues=TRUE,defaults=list(debug="0",out="./all.the.data.intermediate.Rdata"),unique=TRUE)

if(command.line$debug == "0"){
    debug.mode <- 0
    loginfo("Using production models")
}else if(command.line$debug == "1"){
    debug.mode <- 1
    loginfo("Using debug models, much faster to run, less accurate. Please dont sync data to BQ")
}else stop(glue("Incorrect debug number passed: {command.line$debug}"))




dall.rel2 <- data.table(getModelDataForChannel("release",v=3,input_file=command.line$release_raw))[nvc>0,]
dall.beta2 <- data.table(getModelDataForChannel("beta",v=3,input_file=command.line$beta_raw))[nvc>0,]
dall.nightly2 <- data.table(getModelDataForChannel("nightly",v=3,input_file=command.line$nightly_raw))[nvc>0,]

invisible({
    dall.rel2[, nvc.logit:=boot::logit(nvc)]
    dall.beta2[, nvc.logit:=boot::logit(nvc)]
    dall.nightly2[, nvc.logit:=boot::logit(nvc)]
})

loginfo("Using following dates")
print(dall.rel2[, list(channel='release',UsingDateTill=max(date)),by=os][order(os),])
print(dall.beta2[, list(channel='beta',UsingDateTill=max(date)),by=os][order(os),])
print(dall.nightly2[, list(channel='nightly',UsingDateTill=max(date)),by=os][order(os),])

## BUILD MODELS
loginfo(glue("Started Release Models, debug.mode = {debug.mode}"))
## Release model
d.rel <- dall.rel2
cr.cm.rel.f <- future({ make.a.model(d.rel,'cmr',debug=debug.mode) })
cr.cc.rel.f <- future({ make.a.model(d.rel,'ccr',debug=debug.mode) })
ci.cm.rel.f <- future({ make.a.model(d.rel,'cmi',debug=debug.mode) })
ci.cc.rel.f <- future({ make.a.model(d.rel,'cci',debug=debug.mode) })

cr.cm.rel <- label(value(cr.cm.rel.f),'cmr');loginfo("Finished Release cr.cm"); 
cr.cc.rel <- label(value(cr.cc.rel.f),'ccr');loginfo("Finished Release cr.cc"); 
ci.cm.rel <- label(value(ci.cm.rel.f),'cmi');loginfo("Finished Release ci.cm"); 
ci.cc.rel <- label(value(ci.cc.rel.f),'cci');loginfo("Finished Release ci.cc"); 

loginfo("Finished Release Models")
## Beta Model

loginfo(glue("Started Beta Models, debug.mode = {debug.mode}"))
d.beta <- dall.beta2
cr.cm.beta.f <- future({ make.a.model(d.beta,'cmr',channel='beta',debug=debug.mode) })
cr.cc.beta.f <- future({ make.a.model(d.beta,'ccr',channel='beta',debug=debug.mode) })
ci.cm.beta.f <- future({ make.a.model(d.beta,'cmi',channel='beta',debug=debug.mode) })
ci.cc.beta.f <- future({ make.a.model(d.beta,'cci',channel='beta',debug=debug.mode) })
cr.cm.beta <- label(value(cr.cm.beta.f),'cmr');loginfo("Finished Beta cr.cm");
cr.cc.beta <- label(value(cr.cc.beta.f),'ccr');loginfo("Finished Beta cr.cc");
ci.cm.beta <- label( value(ci.cm.beta.f),'cmi');loginfo("Finished Beta ci.cm");
ci.cc.beta <- label(value(ci.cc.beta.f),'cci');loginfo("Finished Beta ci.cc");
loginfo("Finished Beta Models")

## Nightly Model

loginfo(glue("Started Nightly Models,  debug.mode = {debug.mode}"))
d.nightly <- dall.nightly2
cr.cm.nightly.f <- future({ make.a.model(d.nightly,'cmr',channel='nightly',debug=debug.mode,iter=4000) })
cr.cc.nightly.f <- future({ make.a.model(d.nightly,'ccr',channel='nightly',debug=debug.mode,iter=4000) })
ci.cm.nightly.f <- future({ make.a.model(d.nightly,'cmi',channel='nightly',debug=debug.mode,iter=4000,list0=list(adapt_delta = 0.99, max_treedepth=13)) })
ci.cc.nightly.f <- future({ make.a.model(d.nightly,'cci',channel='nightly',debug=debug.mode,iter=4000) })
cr.cm.nightly <- label(value(cr.cm.nightly.f),'cmr');loginfo("Finished Nightly cr.cm");
cr.cc.nightly <- label(value(cr.cc.nightly.f),'ccr');loginfo("Finished Nightly cr.cc");
ci.cm.nightly <- label(value(ci.cm.nightly.f),'cmi');loginfo("Finished Nightly ci.cm");
ci.cc.nightly <- label(value(ci.cc.nightly.f),'cci');loginfo("Finished Nightly ci.cc");
loginfo("Finished Nightly Models")

loginfo("Finished Modelling")


all.models <- list("cr.cm.rel"=cr.cm.rel,"cr.cc.rel"=cr.cc.rel,"ci.cm.rel"=ci.cm.rel,"ci.cc.rel"=ci.cc.rel,
               "cr.cm.beta"=cr.cm.beta,"cr.cc.beta"=cr.cc.beta,"ci.cm.beta"=ci.cm.beta,"ci.cc.beta"=ci.cc.beta,
               "cr.cm.nightly"=cr.cm.nightly,"cr.cc.nightly"=cr.cc.nightly,"ci.cm.nightly"=ci.cm.nightly,"ci.cc.nightly"=ci.cc.nightly)

bad.models <- names(all.models)[ unlist(Map(function(i,m){
    if(any( brms::rhat(m) >=1.1)) TRUE else FALSE
},names(all.models),all.models))]
if(length(bad.models)>0){
    loginfo(glue("The following models has R-hats>1.1, be careful: {f}",f=paste(bad.models,collapse=", ")))
}
               
loginfo(glue("Writing datasets to {command.line$out}"))
save(cr.cm.rel,cr.cc.rel,ci.cm.rel,ci.cc.rel,
     cr.cm.beta,cr.cc.beta,ci.cm.beta,ci.cc.beta,
     cr.cm.nightly,cr.cc.nightly,ci.cm.nightly,ci.cc.nightly,
     dall.rel2,dall.beta2,dall.nightly2,file=command.line$out)
