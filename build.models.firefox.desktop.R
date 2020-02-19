setwd("~/missioncontrol-v2/")
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
python data/crud.py dl_raw  --creds_loc {BQCREDS}  --channel {ch} --n_majors {v} --cache False --outname '{rtemp}'
")
        writeLines(runner,con="./runner.sh")
        loginfo(glue("Starting Gettting Model Data for channel {ch} and nversions {v}"))
        res  <- system2("sh", "./runner.sh",stderr=TRUE,stdout=TRUE)
        loginfo(paste(res, collapse="\n"))
        if(any(grepl("(Error|(E|e)xception)",res))|| any(grepl("(f|F)ailed",res))){
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
command.line <- commandArgs(asValues=TRUE,defaults=list(debug="0",out="./all.the.data.Rdata"),unique=TRUE)

if(command.line$debug == "0"){
    debug.mode <- 0
    loginfo("Using production models")
}else if(command.line$debug == "1"){
    debug.mode <- 1
    loginfo("Using debug models, much faster to run, less accurate. Please dont sync data to BQ")
}else stop(glue("Incorrect debug number passed: {command.line$debug}"))




dall.rel2 <- data.table(getModelDataForChannel("release",v=3,input_file=command.line$release_raw))[nvc>0,]
dall.beta2 <- data.table(getModelDataForChannel("beta",v=2,input_file=command.line$beta_raw))[nvc>0,]
dall.nightly2 <- data.table(getModelDataForChannel("nightly",v=2,input_file=command.line$nightly_raw))[nvc>0,]
dall.esr2 <- data.table(getModelDataForChannel("esr",v=2,input_file=command.line$esr_raw))[nvc>0,]


## At the time of writing these field did not have the extra minute added and hence we had infinities
## this protects from that. Ideally this should be in the SQL code.

invisible({
    dall.rel2[,     ":="(cmr=(cmain)/(usage_cm_crasher_cversion+1/60),ccr=(ccontent)/(usage_cc_crasher_cversion+1/60))]
    dall.beta2[,    ":="(cmr=(cmain)/(usage_cm_crasher_cversion+1/60),ccr=(ccontent)/(usage_cc_crasher_cversion+1/60))]
    dall.nightly2[, ":="(cmr=(cmain)/(usage_cm_crasher_cversion+1/60),ccr=(ccontent)/(usage_cc_crasher_cversion+1/60))]
    dall.esr2[,     ":="(cmr=(cmain)/(usage_cm_crasher_cversion+1/60),ccr=(ccontent)/(usage_cc_crasher_cversion+1/60))]
    dall.rel2[,     ":="(cmi.logit=boot::logit(cmi), cci.logit=boot::logit(cci))]
    dall.beta2[,    ":="(cmi.logit=boot::logit(cmi), cci.logit=boot::logit(cci))]
    dall.nightly2[, ":="(cmi.logit=boot::logit(cmi), cci.logit=boot::logit(cci))]
    dall.esr2[, ":="(cmi.logit=boot::logit(cmi), cci.logit=boot::logit(cci))]
})



## Is this or in SQL the best place for this to be?  I can;'t really
## say. Nevertheless, this is where we keep 'valid' data'. I will
## explain this For release, keep data for a version till a new one is
## released (similar to ESR, with no overlap).
dall.rel2 <-local({
    u <- jsonlite::fromJSON("https://product-details.mozilla.org/1.0/firefox.json")
    u <- u$release[ unlist(Map(function(n,s) if(grepl("(major|stability)",n) || grepl("(major|stability)",s$category)) TRUE else FALSE,names(u$release), u$release)) ]
    u <- u[ unlist(Map(function(n,s) if(!grepl("esr",n) & !grepl("esr",s$category)) TRUE else FALSE,names(u), u)) ]
    u <- rbindlist(Map(function(s1) data.table(c_version=s1$version, sdate = s1$date), u))[order(sdate),][, sdate:=as.Date(sdate)][order(sdate),]
    u <- u[,rdate:=as.Date(c(tail(u$sdate,-1),as.Date('2040-01-01')))]
    merge(dall.rel2, u[, list(c_version, rdate)], by='c_version',all.x=TRUE)[date<=rdate][, rdate:=NULL]
})

## For Nightly and Beta, we keep all data on a version till it reaches
## maximum adoption Note, we dont use current because sometimes _two_
## versions are current e.g. in Nightly a version released today will
## be in use for 2-3 days and versions are released everyday so we
## dont just keep data while a version is current

dall.beta2 <- local({
    dall.beta2[,{
        .SD[date<=date[which.max(nvc)],]
    },by=list(os,c_version)]
})

dall.nightly2 <- local({
    dall.nightly2[,{
        .SD[date<=date[which.max(nvc)],]
    },by=list(os,c_version)]
})

## For ESR, its much like release except some majors overlap(by design)
uesr <- local({
    u <- jsonlite::fromJSON("https://product-details.mozilla.org/1.0/firefox.json")
    uesr <- u$release[ unlist(Map(function(n,s) if(grepl("esr",n) || s$category=='esr') TRUE else FALSE,names(u$release), u$release)) ]
    uesr <- rbindlist(Map(function(s1) data.table(c_version=s1$version, sdate = s1$date), uesr))[order(sdate),][, sdate:=as.Date(sdate)]
    ## Add Major/Minor
    uesr <- cbind(uesr,rbindlist(Map(function(s) {
        f <- strsplit(s,".",fixed=TRUE)[[1]]
        data.table(major = f[1], minor=paste(f[-1],collapse="."))
    },uesr$c_version)))
    ## For minors with major, the end date of a minor is the start date of subsequent minor
    uesr <- uesr[,rdate:={
        x <- .SD[order(sdate),]
        as.Date(c(tail(x$sdate,-1),as.Date('2040-01-01')))
    },by=major]
    ## Now for major/minor dates=='2040-01-01' fill in with next most one
    ## which will correspond to end date of last minor on previous major
    ## This step is required since we two minors of a version overlap with
    ## subsequent major version.
    uesr[rdate=='2040-01-01', rdate:=uesr[ pmin(which((rdate=='2040-01-01'))+1,nrow(uesr)) ,rdate]]
})


dall.esr2 <- merge(dall.esr2,uesr[, list(c_version, rdate)], by="c_version",keep.x=TRUE)
## And this is how  we remove data beyond the 'end' date of a major/minor
dall.esr2 <- dall.esr2[date<=rdate][, rdate:=NULL]


invisible({
    dall.rel2[, nvc.logit:=boot::logit(nvc)]
    dall.beta2[, nvc.logit:=boot::logit(nvc)]
    dall.nightly2[, nvc.logit:=boot::logit(nvc)]
    dall.esr2[, nvc.logit:=boot::logit(nvc)]
})

loginfo("Using following dates")
print(dall.rel2[, list(channel='release',UsingDateTill=max(date)),by=os][order(os),])
print(dall.beta2[, list(channel='beta',UsingDateTill=max(date)),by=os][order(os),])
print(dall.nightly2[, list(channel='nightly',UsingDateTill=max(date)),by=os][order(os),])
print(dall.esr2[, list(channel='esr',UsingDateTill=max(date)),by=os][order(os),])

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


loginfo(glue("Started esr Models,  debug.mode = {debug.mode}"))
d.esr <- dall.esr2
cr.cm.esr.f <- future({ make.a.model(d.esr,'cmr',channel='esr',debug=debug.mode,iter=4000) })
cr.cc.esr.f <- future({ make.a.model(d.esr,'ccr',channel='esr',debug=debug.mode,iter=4000) })
ci.cm.esr.f <- future({ make.a.model(d.esr,'cmi',channel='esr',debug=debug.mode,iter=4000,list0=list(adapt_delta = 0.99, max_treedepth=13)) })
ci.cc.esr.f <- future({ make.a.model(d.esr,'cci',channel='esr',debug=debug.mode,iter=4000) })
cr.cm.esr <- label(value(cr.cm.esr.f),'cmr');loginfo("Finished Esr cr.cm");
cr.cc.esr <- label(value(cr.cc.esr.f),'ccr');loginfo("Finished Esr cr.cc");
ci.cm.esr <- label(value(ci.cm.esr.f),'cmi');loginfo("Finished Esr ci.cm");
ci.cc.esr <- label(value(ci.cc.esr.f),'cci');loginfo("Finished Esr ci.cc");
loginfo("Finished esr Models")

loginfo("Finished Modelling")


all.models <- list("cr.cm.rel"=cr.cm.rel,"cr.cc.rel"=cr.cc.rel,"ci.cm.rel"=ci.cm.rel,"ci.cc.rel"=ci.cc.rel,
               "cr.cm.beta"=cr.cm.beta,"cr.cc.beta"=cr.cc.beta,"ci.cm.beta"=ci.cm.beta,"ci.cc.beta"=ci.cc.beta,
               "cr.cm.nightly"=cr.cm.nightly,"cr.cc.nightly"=cr.cc.nightly,"ci.cm.nightly"=ci.cm.nightly,"ci.cc.nightly"=ci.cc.nightly
               ,"cr.cm.esr"=cr.cm.esr,"cr.cc.esr"=cr.cc.esr,"ci.cm.esr"=ci.cm.esr,"ci.cc.esr"=ci.cc.esr)

bad.models <- names(all.models)[ unlist(Map(function(i,m){
#    print(i)
    if(any( brms::rhat(m) >=1.1)) TRUE else FALSE
},names(all.models),all.models))]
if(length(bad.models)>0){
    loginfo(glue("The following models has R-hats>1.1, be careful: {f}",f=paste(bad.models,collapse=", ")))
}

loginfo(glue("Writing datasets to {command.line$out}"))
n <- as.character(dall.rel2[,max(date)])
save(cr.cm.rel,cr.cc.rel,ci.cm.rel,ci.cc.rel,n,
     cr.cm.beta,cr.cc.beta,ci.cm.beta,ci.cc.beta,
     cr.cm.nightly,cr.cc.nightly,ci.cm.nightly,ci.cc.nightly,
     cr.cm.esr,cr.cc.esr,ci.cm.esr,ci.cc.esr,
     dall.rel2,dall.beta2,dall.nightly2,dall.esr2,
     file=command.line$out)
