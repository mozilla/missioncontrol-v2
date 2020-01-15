## The objective of this code is to produce posterior distributions of
## every version we only keep estimates from models for example when
## 72.0b3 is released on say Date d we use estimates from models
## produced on dates d,d+1,d+2 . We stop then because 720b3 is not
## current anymore. We dont use the future (data from 72.0b4 onwards)
## to fit the past

setwd("~/missioncontrol-v2/")
source("missioncontrol.lib.R")

## This is backfilling. We need to load all the prior models and produce estimates
## Get all historic dates

N <- 1000

make_posterior_newdata <- function(mydata,model_date,CHAN){
    mydata <- mydata[, list(channel=CHAN,date,os,c_version,major,minor,nvc, dau_cversion,  dau_cc_crasher_cversion, dau_cm_crasher_cversion,
                            ccontent, cmain, usage_cc_crasher_cversion,usage_cm_crasher_cversion,cmr,ccr,cmi,cci)]
    mydata <- mydata[order(c_version,os,date),]
    mydata.posteriors.at <- merge(mydata,  adoptionsCompare(NULL,NULL,TRUE), by=c("channel","os"),all.x=TRUE)
    mydata.posteriors.at <- mydata.posteriors.at[,list(channel,model_date = model_date,date,os,c_version, major,minor,
                                                       nvc=adopt, dau_cversion = dau,
                                                       usage_cc_crasher_cversion =  usage_cc,
                                                       usage_cm_crasher_cversion =  usage_cm)]
    mydata.posteriors.at  <- mydata.posteriors.at [order(major,minor, os, date),]                                                   
}

getModelDate <- function(path){
    substring(tail(strsplit(path,"/")[[1]],1),8,8+9)
}

make_posteriors <- function(mydata, CHAN,model.date,model.list,last.model.date){
    mydata.posteriors.at <- make_posterior_newdata(mydata,model_date= model.date, CHAN=CHAN)
    mydata.posteriors.at <- mydata.posteriors.at[date>last.model.date,]
    if(nrow(mydata.posteriors.at) == 0) return(NULL)
    ## Only keep latest dates since all prior dates will have same crash rate
    mydata.posteriors.at <-
        mydata.posteriors.at[, .SD[date==max(date),],by=list(channel, os, c_version,major,minor)]

    ## Posteriors for CR(M,C) and CI(M,C)
    ## For Operating Systems
    posterior.os.individual.metrics <- rbindlist(Map(function(m, w){
        mydata.posteriors <- getPredictions(M=m,D=mydata.posteriors.at,nsamples=N)
        mydata.posteriors <- rbindlist(lapply(1:nrow(mydata.posteriors.at),function(i){
            cbind( mydata.posteriors.at[i,],data.table(modelname = w,rep=1:N, posterior=mydata.posteriors[i,]))
        }))
    }, m=model.list,w=names(model.list)))

    ## Posteriors for CR-Score and CI-Score
    ## For operating systems
    mydata.posteriors.cr.cm <- getPredictions(M=model.list$cmr,,D=mydata.posteriors.at,nsamples=N)
    mydata.posteriors.cr.cc <- getPredictions(M=model.list$ccr,,D=mydata.posteriors.at,nsamples=N)
    mydata.posteriors.cr <- mydata.posteriors.cr.cm + mydata.posteriors.cr.cc

    mydata.posteriors.ci.cm <- getPredictions(M=model.list$cmi,,D=mydata.posteriors.at,nsamples=N)
    mydata.posteriors.ci.cc <- getPredictions(M=model.list$cci,D=mydata.posteriors.at,nsamples=N)
    mydata.posteriors.ci <- mydata.posteriors.ci.cm + mydata.posteriors.ci.cc
    
    posterior.os.individual.scores <- rbind(
        rbindlist(lapply(1:nrow(mydata.posteriors.at),function(i){
            cbind( mydata.posteriors.at[i,],data.table(modelname = 'cr',rep=1:N, posterior=mydata.posteriors.cr[i,]))
        })),
        rbindlist(lapply(1:nrow(mydata.posteriors.at),function(i){
            cbind( mydata.posteriors.at[i,],data.table(modelname = 'ci',rep=1:N, posterior=mydata.posteriors.ci[i,]))
        }))
    )

    ttmp <- rbind(posterior.os.individual.scores,posterior.os.individual.metrics)
    ttmp2 <- ttmp[,{
        osdarwin  <- .SD[os=='Darwin',][order(rep),]
        oswindows <- .SD[os=='Windows_NT',][order(rep),]
        oslinux   <- .SD[os=='Linux',][order(rep),]
        ns <- c(nrow(osdarwin),nrow(oswindows),nrow(oslinux))
        if(!(sum(ns==0)>=2) ){
            osall <- apply(cbind(osdarwin[,posterior], oswindows[,posterior], oslinux[,posterior]),1,mean)
            if(length(osall) >0){
                .SD[, list(os='overall', nvc=0, dau_cversion=0, usage_cc_crasher_cversion =0,usage_cm_crasher_cversion=0, rep=1:length(osall), posterior=osall)]
            }
        }
    },by=list(channel, c_version, major, minor,date,model_date,modelname)]
    if(nrow(ttmp2)>0){
        ttmp2 <- ttmp2[, list(channel,os,c_version,major,minor,model_date,date,nvc,dau_cversion,usage_cc_crasher_cversion,usage_cm_crasher_cversion,modelname,rep,posterior)]
        posterior.os.all <- rbind(ttmp,ttmp2)
    }else posterior.os.all <- ttmp
    posterior.os.all
}

backfill <- FALSE
if(backfill){
    
    models <- system(glue("gsutil ls {loc}",loc = getArchiveLoc()),intern=TRUE)
    models <- models[grepl("models-", models)]
    models <- sort(models)

## Custom data simplfication for first run
## For the first run, we need posteriors for every version


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


} ## end backfill


##
## Call as Rscript process_and_save_posteriors.R --data_file=default is ./all.the.data.intermediate.Rdata --out=default is ./all.the.data.Rdata
command.line <- commandArgs(asValues=TRUE,defaults=list(data_file="./all.the.data.intermediate.Rdata",out="./all.the.data.Rdata"),unique=TRUE)
loginfo(glue("loading data file from {command.line$data_file}"))
load(command.line$data_file)



model.date <- max(dall.rel2$date)
last.model.date <- system("bq query --format=prettyjson --nouse_legacy_sql 'select max(model_date) as x from `moz-fx-data-derived-datasets`.analysis.missioncontrol_v2_posteriors'",intern=TRUE)
last.model.date <- rjson::fromJSON(paste(last.model.date,collapse="\n"))[[1]]$x

rel.list <- list(cmr = cr.cm.rel, ccr = cr.cc.rel, cmi = ci.cm.rel, cci = ci.cc.rel)
ll.rel <- make_posteriors(dall.rel2, CHAN='release', model.date = model.date,model.list=rel.list,last.model.date= last.model.date)


beta.list <- list(cmr = cr.cm.beta, ccr = cr.cc.beta, cmi = ci.cm.beta, cci = ci.cc.beta)
ll.beta <- make_posteriors(dall.beta2, CHAN='beta', model.date = model.date,model.list=beta.list,last.model.date= last.model.date)


nightly.list <- list(cmr = cr.cm.nightly, ccr = cr.cc.nightly, cmi = ci.cm.nightly, cci = ci.cc.nightly)
ll.nightly <- make_posteriors(dall.nightly2, CHAN='nightly', model.date = model.date,model.list=nightly.list,last.model.date= last.model.date)

all.posteriors <- rbindlist(list(ll.rel,ll.beta,ll.nightly))

## Now write this
atemp <- tempfile()
fwrite(all.posteriors, file=atemp,row.names=FALSE,quote=TRUE,na=0)
system(glue("bq load  --noreplace --project_id moz-fx-data-derived-datasets   --source_format=CSV --skip_leading_rows=1 --null_marker=NA",
            " analysis.missioncontrol_v2_posteriors {atemp} ./posterior_schema.json"))

## Save Currents

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


## And save this

#save(cr.cm.rel,cr.cc.rel,ci.cm.rel,ci.cc.rel,
#     cr.cm.beta,cr.cc.beta,ci.cm.beta,ci.cc.beta,
#     cr.cm.nightly,cr.cc.nightly,ci.cm.nightly,ci.cc.nightly,all.posteriors
#     dall.rel2,dall.beta2,dall.nightly2,file=command.line$out)




######################################################################
## Some Demo Queries
######################################################################
g <- bq()


## Rolout
## g$q("select date,modelname,APPROX_QUANTILES( posterior,100)[OFFSET(50)] as x from  `moz-fx-data-derived-datasets`.analysis.missioncontrol_v2_posteriors where os='Linux' and channel='release' and c_version
## in ('72.0.1') group by 1,2 order by 2,1")







################################################################################
## Release
################################################################################

chan <- 'release'

versions.compare.sql <- g$q(glue('select
c_version as Current_Version,
ncurrent as Days_on_Current,
o_version as Older_Version,
nolder as Days_on_Older,
asOf as asOf
from  analysis.missioncontrol_v2_channel_summaries
where channel="{chan}" and os="Windows_NT"
'))



usage.sql <- g$q(glue("
with a as (
select 'Windows_NT' as os, 1 as sr
union all
select 'Darwin' as os, 2 as sr
union all
select 'Linux' as os, 3 as sr
union all
select 'overall' as os, 4 as sr
),
b as (
select sr,a.os,  nvc, dau_cversion, call
from analysis.missioncontrol_v2_channel_summaries X  join a
on X.os= a.os
where channel='{chan}'
order by sr 
)
select os as OS, format('%.2f%%',nvc*100) as Adoption, format(\"%'d\",call) as `Total_Crashes`,format(\"%'d\",dau_cversion) as `DAU_on_Current` from b
"))



rate.incidence.summary.sql  <- g$q(glue("
CREATE  TEMP FUNCTION  whatColor(r float64, i float64) AS (
(select
 case  when r>i and r>0.75 then 'red'
       when r>i and r>=0.6  then 'darkorange'
       when r>i then 'grey'
       when r<=i and i>0.75 then 'limegreen'
       else 'grey'
  end
)
);
CREATE TEMP FUNCTION stringify(c float64, r float64,incidence BOOL,color string) AS(
 (
   with a as (
   select case when incidence = false then format('%.2f (%.2f%%)',c,r*100)
            else format('%.2f%% (%.2f%%)',c*100,r*100) end as X
  )
  select case when color = 'red' then format('%s ⤊ ', X)
              when color = 'darkorange' then format('%s ↑', X)
              when color = 'limegreen' then format('%s ⤋ ',X)
              else  format('%s', X) end
  from a
));
with
osorder as (
select 'Windows_NT' as os, 1 as sr
union all
select 'Darwin' as os, 2 as sr
union all
select 'Linux' as os, 3 as sr
union all
select 'overall' as os, 4 as sr
),
a0 as (select os,model_date,modelname,c_version as cv,rep,posterior as cp from `moz-fx-data-derived-datasets`.analysis.missioncontrol_v2_posteriors
     where c_version = (select max(c_version) from analysis.missioncontrol_v2_channel_summaries where os='Windows_NT' and channel='{chan}')
     and channel='{chan}'
),
a00 as ( select os, max(model_date) as max_model_date from a0 group by 1) ,
a as (select a0.* from a0 join a00 on a0.os=a00.os and a0.model_date = a00.max_model_date),
b0 as (select os,model_date,modelname,c_version as ov,rep,posterior as op from `moz-fx-data-derived-datasets`.analysis.missioncontrol_v2_posteriors
     where c_version = (select max(o_version) from analysis.missioncontrol_v2_channel_summaries where os='Windows_NT' and channel='{chan}')
     and channel='{chan}'
),
b00 as ( select os, max(model_date) as max_model_date from b0 group by 1) ,
b as (select b0.* from b0 join b00 on b0.os=b00.os and b0.model_date = b00.max_model_date),
c as (select a.os,a.modelname,cv,ov,a.rep,cp, ( cp-op)/op as rel from a join b on a.os=b.os and a.rep=b.rep and a.modelname=b.modelname),
d as (select os, modelname, cv,ov,
 APPROX_QUANTILES(cp, 100)[OFFSET(50)] as c,
 APPROX_QUANTILES( rel,100)[OFFSET(50)] as relchange,
 avg(case when rel >0.2 then 1 else 0 end) as prReg,
 avg(case when rel <-0.2 then 1 else 0 end) as prImp
from c
group by 1,2,3,4
order by 1,2,3,4),
e1  as (
select os, c as Score, relchange Score_rl, prImp as Score_Imp ,prReg as Score_Reg
from d where modelname='cr')
,e2  as (
select os, c as CM, relchange CM_rl, prImp as CM_Imp ,prReg as CM_Reg
from d where modelname='cmr')
,e3  as (
select os, c as CC, relchange CC_rl, prImp as CC_Imp ,prReg as CC_Reg
from d where modelname='ccr'),
e as (
select
sr, e1.os, 'rate' as type ,stringify( Score, Score_rl,false,whatColor(Score_Imp, Score_Reg)) as Score,
 stringify(CM, CM_rl,false, whatColor(CM_Imp, CM_Reg)) as CM,
 stringify(CC, CC_rl,false,whatColor(CC_Imp, CC_Reg)) as CC, 
from e1 join e2 on e1.os=e2.os  join e3 on e2.os=e3.os join osorder on osorder.os=e3.os order by sr ),
f1  as (
select os, c as Score, relchange Score_rl, prImp as Score_Imp ,prReg as Score_Reg
from d where modelname='ci')
,f2  as (
select os, c as CM, relchange CM_rl, prImp as CM_Imp ,prReg as CM_Reg
from d where modelname='cmi')
,f3  as (
select os, c as CC, relchange CC_rl, prImp as CC_Imp ,prReg as CC_Reg
from d where modelname='cci'),
f as (
select
sr, f1.os, 'incidence' as type , stringify(Score,Score_rl,true,whatColor(Score_Imp, Score_Reg)) as Score ,
 stringify(CM, CM_rl,true,whatColor(CM_Imp, CM_Reg)) as CM,
 stringify(CC, CC_rl,true,whatColor(CC_Imp, CC_Reg)) as CC,
from f1 join f2 on f1.os=f2.os  join f3 on f2.os=f3.os join osorder on osorder.os=f3.os order by sr ),
final as (select *   from e union all  select * from f order by type DESC,sr)
select * except(sr) from final
"),-1)


evolution.sql  <- g$q(glue("
with
a0 as (
select os,modelname,c_version as cv,date as date,model_date,
APPROX_QUANTILES(posterior, 100)[OFFSET(50)] as c,
APPROX_QUANTILES(posterior, 100)[OFFSET(5)] as lo90,
APPROX_QUANTILES(posterior, 100)[OFFSET(95)] as hi90,
from `moz-fx-data-derived-datasets`.analysis.missioncontrol_v2_posteriors
where channel='{chan}' and date>=DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
--and os != 'overall'
group by 1,2,3,4,5
order by 2,1,3,4,5
),
a1 as (select os,modelname, cv, max(model_date) as model_date_max from a0 group by 1,2,3),
a as (select a0.* from a0 join a1 on a0.os=a1.os and a0.modelname=a0.modelname and a0.cv=a1.cv  and model_date = model_date_max),
b as (select
 os,modelname,cv, date, model_date,c, lo90,hi90,
 row_number() OVER mywindow AS n_
  from a
 WINDOW mywindow AS (PARTITION BY os,cv,modelname ORDER BY date DESC )
),
c as (select * except(n_) from b where n_ = 1),
d as (
select  os, c_version as cv, major,minor,date, nvc as adoption
from `moz-fx-data-derived-datasets`.analysis.missioncontrol_v2_raw_data
where channel = '{chan}'
),
e as (
select A.os,A.date, A.cv,major,minor,adoption, modelname, c,lo90,hi90
from c A left join d
on A.os=d.os and A.date=d.date and A.cv=d.cv
order by modelname, os,major,minor
)
select * from e
"),-1)
