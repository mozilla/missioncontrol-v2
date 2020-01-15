## The objective of this code is to produce posterior distributions of
## every version we only keep estimates from models for example when
## 72.0b3 is released on say Date d we use estimates from models
## produced on dates d,d+1,d+2 . We stop then because 720b3 is not
## current anymore. We dont use the future (data from 72.0b4 onwards)
## to fit the past

## This code takes all the model outputs from runs throughout history,
## computers posterirors and saves them into BQ. Its a one time but
## can also be used to literally create the BQ table containing all
## posteriors


setwd("~/missioncontrol-v2/")
source("missioncontrol.lib.R")


models <- system(glue("gsutil ls {loc}",loc = getArchiveLoc()),intern=TRUE)
models <- models[grepl("models-", models)]
models <- sort(models)



models.container <- list()
last.model.date <- '2018-01-01'
for(index in seq_along(models)){
    if(index==62) next
    print(models[index])
    amodel <- models[index]
    amodel.date <- getModelDate(amodel)
    amodel.data <- loadArchiveData(path = amodel)
    
    mydata <- amodel.data$dall.rel2
    amodel.list <- list(cmr = amodel.data$cr.cm.rel, ccr = amodel.data$cr.cc.rel, cmi = amodel.data$ci.cm.rel, cci = amodel.data$ci.cc.rel)
    ll <- make_posteriors(mydata, CHAN='release', model.date = amodel.date,model.list=amodel.list,last.model.date = last.model.date)
    if(is.null(ll)) next
    models.container[[ length(models.container)+1 ]] <- ll
    
    mydata <- amodel.data$dall.beta2
    amodel.list <- list(cmr = amodel.data$cr.cm.beta, ccr = amodel.data$cr.cc.beta, cmi = amodel.data$ci.cm.beta, cci = amodel.data$ci.cc.beta)
    ll <- make_posteriors(mydata, CHAN='beta', model.date = amodel.date,model.list=amodel.list,last.model.date = last.model.date)
    if(is.null(ll)) next
    models.container[[ length(models.container)+1 ]] <- ll
    
    mydata <- amodel.data$dall.nightly2
    amodel.list <- list(cmr = amodel.data$cr.cm.nightly, ccr = amodel.data$cr.cc.nightly, cmi = amodel.data$ci.cm.nightly, cci = amodel.data$ci.cc.nightly)
    ll <- make_posteriors(mydata, CHAN='nightly', model.date = amodel.date,model.list=amodel.list,last.model.date = last.model.date)
    if(is.null(ll)) next
    models.container[[ length(models.container)+1 ]] <- ll
    
    last.model.date <- amodel.date
    
}

models.backfill <- rbindlist(models.container)


atemp <- tempfile()
fwrite(models.backfill, file=atemp,row.names=FALSE,quote=TRUE,na=0)
system(glue("bq load  --replace --project_id moz-fx-data-derived-datasets  --source_format=CSV --skip_leading_rows=1 --null_marker=NA",
            " analysis.missioncontrol_v2_posteriors {atemp} ./posterior_schema.json"))



dall.rel2 <- amodel.data$dall.rel2
dall.beta2 <- amodel.data$dall.beta2
dall.nightly2 <- amodel.data$dall.nightly2


current.release <- dall.rel2[c_version == getCurrentVersion(dall.rel2,'Windows_NT','release') ,]
older.release <-  dall.rel2[c_version == getPreviousVersion(dall.rel2,'Windows_NT','release') ,]
current.release.smry <- current.release[, .SD[date==max(date),list(c_version,date,major,minor,nvc,dau_cversion,call,dau_call_crasher_cversion,usage_call_crasher_cversion)],by=os]
current.release.smry[, "ncurrent":=current.release[os=='Windows_NT',.N]]
current.release.smry[, 'o_version':=older.release[,c_version[1]]]
current.release.smry[, 'nolder':=older.release[os=='Windows_NT',.N]]
current.release.smry[, channel:='release']
current.release.smry[, asOf:=max(date)]

current.beta <- dall.beta2[c_version == getCurrentVersion(dall.beta2,'Windows_NT','beta') ,]
older.beta <-  dall.beta2[c_version == getPreviousVersion(dall.beta2,'Windows_NT','beta') ,]
current.beta.smry <- current.beta[, .SD[date==max(date),list(c_version,date,major,minor,nvc,dau_cversion,call,dau_call_crasher_cversion,usage_call_crasher_cversion)],by=os]
current.beta.smry[, "ncurrent":=current.beta[os=='Windows_NT',.N]]
current.beta.smry[, 'o_version':=older.beta[,c_version[1]]]
current.beta.smry[, 'nolder':=older.beta[os=='Windows_NT',.N]]
current.beta.smry[, channel:='beta']
current.beta.smry[, asOf:=max(date)]



current.nightly <- dall.nightly2[c_version == getPreviousVersion(dall.nightly2,'Windows_NT','nightly') ,]
older.nightly <-  dall.nightly2[c_version == getMaxVersionBeforeX(dall.nightly2, 'Windows_NT','nightly',
                                                                  c(getCurrentVersion(dall.nightly2,'Windows_NT','nightly'),getPreviousVersion(dall.nightly2,'Windows_NT','nightly'))),]
current.nightly.smry <- current.nightly[, .SD[date==max(date),list(c_version,date,major,minor,nvc,dau_cversion,call,dau_call_crasher_cversion,usage_call_crasher_cversion)],by=os]
current.nightly.smry[, "ncurrent":=current.nightly[os=='Windows_NT',.N]]
current.nightly.smry[, 'o_version':=older.nightly[,c_version[1]]]
current.nightly.smry[, 'nolder':=older.nightly[os=='Windows_NT',.N]]
current.nightly.smry[, channel:='nightly']
current.nightly.smry[, asOf:=max(date)]

channel.summary <- rbindlist(list(current.nightly.smry,current.beta.smry,current.release.smry))
atemp <- tempfile()
fwrite(channel.summary, file=atemp,row.names=FALSE,quote=TRUE,na=0)
system(glue("bq load --replace   --project_id moz-fx-data-derived-datasets  --source_format=CSV --skip_leading_rows=1 --null_marker=NA",
                " analysis.missioncontrol_v2_channel_summaries {atemp} ./channel_summary_schema.json"))


