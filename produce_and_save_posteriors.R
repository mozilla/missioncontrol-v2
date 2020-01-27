setwd("~/missioncontrol-v2/")
source("missioncontrol.lib.R")


## From the models, generate posteriors and save to BQ
## Also create some summary tables for BQ

## Call as Rscript produce_and_save_posteriors.R --data_file=default is ./all.the.data.Rdata  --overwrite=0
command.line <- commandArgs(asValues=TRUE,defaults=list(data_file="./all.the.data.Rdata",backup=0,overwrite=0),unique=TRUE)
backup.mode <- command.line$backup
loginfo(glue("loading data file from {command.line$data_file}"))
load(command.line$data_file)


######################################################################
## Produce Posteriors
######################################################################

model.date <- max(dall.rel2$date)
last.model.date <- system("bq query --format=prettyjson --nouse_legacy_sql 'select max(model_date) as x from `moz-fx-data-derived-datasets`.analysis.missioncontrol_v2_posteriors'",intern=TRUE)
last.model.date <- rjson::fromJSON(paste(last.model.date,collapse="\n"))[[1]]$x

if(model.date == last.model.date & command.line$overwrite==1){
    system("bq query --format=prettyjson --nouse_legacy_sql 'delete from `moz-fx-data-derived-datasets`.analysis.missioncontrol_v2_posteriors where model_date={last.model.date}'")
}
rel.list <- list(cmr = cr.cm.rel, ccr = cr.cc.rel, cmi = ci.cm.rel, cci = ci.cc.rel)
ll.rel <- make_posteriors(dall.rel2, CHAN='release', model.date = model.date,model.list=rel.list,last.model.date= last.model.date)

if(!is.null(ll.rel) && nrow(ll.rel)>0) loginfo(glue("Release Posteriors updated to include {model.date}"))

beta.list <- list(cmr = cr.cm.beta, ccr = cr.cc.beta, cmi = ci.cm.beta, cci = ci.cc.beta)
ll.beta <- make_posteriors(dall.beta2, CHAN='beta', model.date = model.date,model.list=beta.list,last.model.date= last.model.date)
if(!is.null(ll.beta) && nrow(ll.beta)>0) loginfo(glue("Beta Posteriors updated to include {model.date}"))

nightly.list <- list(cmr = cr.cm.nightly, ccr = cr.cc.nightly, cmi = ci.cm.nightly, cci = ci.cc.nightly)
ll.nightly <- make_posteriors(dall.nightly2, CHAN='nightly', model.date = model.date,model.list=nightly.list,last.model.date= last.model.date)
if(!is.null(ll.nightly)&& nrow(ll.nightly)>0) loginfo(glue("Nightly Posteriors updated to include {model.date}"))

all.posteriors <- rbindlist(list(ll.rel,ll.beta,ll.nightly))
if(nrow(all.posteriors)>0 & backup.mode==1){
    ## Now write this
    atemp <- tempfile()
    fwrite(all.posteriors, file=atemp,row.names=FALSE,quote=TRUE,na=0)
    system(glue("bq load  --noreplace --project_id moz-fx-data-derived-datasets   --source_format=CSV --skip_leading_rows=1 --null_marker=NA",
            " analysis.missioncontrol_v2_posteriors {atemp} ./posterior_schema.json"))
}else loginfo("No data to write, all rows zero")

######################################################################
## Produce Summary
######################################################################


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


## Now Add Some Model information

all.models <- list("cr.cm.rel"=cr.cm.rel,"cr.cc.rel"=cr.cc.rel,"ci.cm.rel"=ci.cm.rel,"ci.cc.rel"=ci.cc.rel,
               "cr.cm.beta"=cr.cm.beta,"cr.cc.beta"=cr.cc.beta,"ci.cm.beta"=ci.cm.beta,"ci.cc.beta"=ci.cc.beta,
               "cr.cm.nightly"=cr.cm.nightly,"cr.cc.nightly"=cr.cc.nightly,"ci.cm.nightly"=ci.cm.nightly,"ci.cc.nightly"=ci.cc.nightly)
bad.models <- names(all.models)[ unlist(Map(function(i,m){
    if(any( brms::rhat(m) >=1.1)) TRUE else FALSE
},names(all.models),all.models))]


options(width=200)
w=invisible(Map(function(s,n){
    options(warn=0)    
    x1 <- paste(capture.output(print(s)),collapse="\n")
    options(warn=1)
    x2 <- paste(capture.output(print(s), type='message'),collapse='\n')
    x <- sprintf("%s\n\n%s\n%s\n",n,x1,x2)
    x
},all.models,names(all.models)))
w <- paste(unlist(w),collapse='\n')
    
w2 <- toJSON(list(w=w))
channel.summary[, smry:=w2]

if(backup.mode==1){
    atemp <- tempfile()
    fwrite(channel.summary, file=atemp,row.names=FALSE,quote=TRUE,na=0)
    system(glue("bq load --replace   --project_id moz-fx-data-derived-datasets  --source_format=CSV --skip_leading_rows=1 --null_marker=NA",
                " analysis.missioncontrol_v2_channel_summaries {atemp} ./channel_summary_schema.json"))
    loginfo("Uploaded Channel Summary")
}





