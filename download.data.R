source("missioncontrol.lib.R")
slack.start() ##requires prefix.R






qr <- "
with
SAMPLING as (
 SELECT 'nightly' AS chan, 'Linux' as sos, 1 as NBUCKS
 UNION ALL
 SELECT 'nightly' AS chan, 'Darwin' as sos, 1 as NBUCKS
 UNION ALL
 SELECT 'nightly' AS chan, 'Windows_NT' as sos, 1 as NBUCKS
 UNION ALL
 SELECT 'beta' AS chan, 'Linux' as sos, 1 as NBUCKS
 UNION ALL
 SELECT 'beta' AS chan, 'Darwin' as sos, 1 as NBUCKS
 UNION ALL
 SELECT 'beta' AS chan, 'Windows_NT' as sos, 1 as NBUCKS
 UNION ALL
  SELECT 'release' AS chan, 'Linux' as sos, 1 as NBUCKS
 UNION ALL
 SELECT 'release' AS chan, 'Darwin' as sos, 1 as NBUCKS
 UNION ALL
 SELECT 'release' AS chan, 'Windows_NT' as sos, 3 as NBUCKS
),
a1 as (select
--- TOTAL USAGE ON FIREFOX
submission_date_s3 as date,
os,
sum(active_hours_sum) as usage_all,
count(distinct(client_id)) as dau_all
from telemetry.clients_daily_v6  HH left join SAMPLING
on HH.os = SAMPLING.sos and HH.normalized_channel = SAMPLING.chan
where
submission_date_s3 >='{current_version_release}'
and submission_date_s3 <= DATE_ADD(DATE '{current_version_release}', INTERVAL {nday} DAY)
and os  in ('Linux','Windows_NT','Darwin')
and app_name='Firefox'
and normalized_channel = '{norm_channel}'
and  MOD(ABS(FARM_FINGERPRINT(MD5(client_id))), SAMPLING.NBUCKS)=0
group by 1,2
),
a2 as (
--- TOTAL USAGE ON FIREFOX ON LATEST VERSION
--- THIS AND THE ABOVE ARE USED FOR COMPUTING 'NVC'
select
submission_date_s3 as date,
os,
sum(active_hours_sum) as usage_cversion,
count(distinct(client_id)) as dau_cversion
from telemetry.clients_daily_v6 HH left join SAMPLING
on HH.os = SAMPLING.sos and HH.normalized_channel = SAMPLING.chan
where
submission_date_s3 >='{current_version_release}'
and submission_date_s3 <= DATE_ADD(DATE '{current_version_release}', INTERVAL {nday} DAY)
and os  in ('Linux','Windows_NT','Darwin')
and app_name='Firefox'
and normalized_channel = '{norm_channel}'
-- and profile_creation_date>=12418 and profile_creation_date<=20089 
and {app_version_field}='{current_version}'
and  MOD(ABS(FARM_FINGERPRINT(MD5(client_id))), SAMPLING.NBUCKS)=0
group by 1,2
),
A as (
select a1.date,a1.os,a1.usage_all, a1.dau_all, a2.usage_cversion, a2.dau_cversion
from a1 join a2
on a1.date =a2.date and a1.os=a2.os
),
b1 as (
--- Total Crashes on Current Version
--- NEED CLIENT_ID TO JOIN on DAILY TO GET CRASH RATE
select 
client_id,
submission_date as date,
os_name as os, 
sum(case when payload.processType IS NULL OR payload.processType = 'main' then 1 else 0 end) as cmain,
sum(case when payload.processType = 'content' and (udf.get_key(payload.metadata, 'ipc_channel_error') is null
       or (udf.get_key(payload.metadata, 'ipc_channel_error') is not null  and udf.get_key(payload.metadata, 'ipc_channel_error') !='ShutdownKill'))
     then 1 else 0 end) as ccontent
from {crash_src} JJ left join SAMPLING
on JJ.os_name = SAMPLING.sos and JJ.normalized_channel = SAMPLING.chan
where submission_date >='{current_version_release}'
and submission_date <= DATE_ADD(DATE '{current_version_release}', INTERVAL {nday} DAY)
and os_name in ('Linux','Windows_NT','Darwin')
and application='Firefox'
and normalized_channel = '{norm_channel}'
and {build_version_field} in ({current_version_crash})
and profile_created>=12418 and profile_created<=20089 
and  MOD(ABS(FARM_FINGERPRINT(MD5(client_id))), SAMPLING.NBUCKS)=0
group by 1,2,3
),
--- TOTAL HOURS FROM FOLKS WHO CRASHED
--- TO COMPUTE CRASH RATE 
b2 as (select
client_id,
submission_date_s3 as date,
os as os,
sum(active_hours_sum) as usage,
sum(coalesce(crashes_detected_plugin_sum,0)) as cplugin
from telemetry.clients_daily_v6 HH left join SAMPLING
on HH.os = SAMPLING.sos and HH.normalized_channel = SAMPLING.chan
where
submission_date_s3 >='{current_version_release}'
and submission_date_s3 <= DATE_ADD(DATE '{current_version_release}', INTERVAL {nday} DAY)
and os  in ('Linux','Windows_NT','Darwin')
and app_name='Firefox'
and normalized_channel = '{norm_channel}'
and {app_version_field}='{current_version}'
-- and profile_creation_date>=12418 and profile_creation_date<=20089 
and  MOD(ABS(FARM_FINGERPRINT(MD5(client_id))), SAMPLING.NBUCKS)=0
group by 1,2,3
),
b as (
select 
b1.date,b1.os, 
count(distinct(case when b1.cmain>0 then b1.client_id else null end)) as dau_cm_crasher_cversion,
count(distinct(case when b1.ccontent>0  then b1.client_id else null end)) as dau_cc_crasher_cversion,
count(distinct(case when b2.cplugin>0 then b1.client_id else null end)) as dau_cp_crasher_cversion,
count(distinct(case when (b1.cmain>0 or b1.ccontent>0  or b2.cplugin>0)
                        then b1.client_id else null end)) as dau_call_crasher_cversion,
sum(case when b1.cmain>0 then b2.usage else 0 end) as usage_cm_crasher_cversion,
sum(case when b1.ccontent >0  then b2.usage else 0 end) as usage_cc_crasher_cversion,
sum(case when b2.cplugin >0 then b2.usage else 0 end) as usage_cp_crasher_cversion,
sum(case when (b1.cmain>0 or b1.ccontent>0 or b2.cplugin >0) then b2.usage else 0 end) as usage_call_crasher_cversion,
sum(cmain) as cmain,
sum(ccontent)  as ccontent,
sum(cplugin) as cplugin,
sum(cmain)+sum(ccontent) + sum(cplugin) as call
from b1 join b2 
on b1.client_id = b2.client_id and b1.os=b2.os and b1.date = b2.date
where (b1.ccontent+b1.cmain)< 350 -- see https://sql.telemetry.mozilla.org/queries/64354/source
group by 1,2
),
d as (
select
A.date,A.os,A.usage_all, A.dau_all, A.usage_cversion, A.dau_cversion,
b.dau_cm_crasher_cversion,b.dau_cc_crasher_cversion,b.dau_cp_crasher_cversion,b.dau_call_crasher_cversion,
b.usage_cm_crasher_cversion,b.usage_cc_crasher_cversion,b.usage_cp_crasher_cversion,b.usage_call_crasher_cversion,
b.cmain,b.ccontent,b.cplugin,b.call
from b join A
on b.date=A.date and b.os=A.os
)
select * from d order by os, date
"


## RELEASE DATA


library(glue) 
library(data.table)
isn <- function(s,r = 'telemetry.crash_summary_v1') if(is.null(s) || length(s)==0) r else s

fxv <- local({
  r <- fromJSON(file="https://product-details.mozilla.org/1.0/firefox.json")
  rbindlist(lapply(r$releases,function(s){
    data.table(build.number=s$build_number, categ=s$category, date=as.Date(s$date),
               description = isn(s$description,NA), is.security = s$is_security_driven,
               product = s$product,version=s$version)
  }))
})

fxv.releases <- fxv[grepl("(major|stability)",categ),][date >='2019-01-01',]
fxv.releases <- fxv.releases[, ":="(major = as.numeric(sapply(version,function(s) head(strsplit(s,"\\.")[[1]],1))),
                                    minor = as.numeric(sapply(version,function(s) tail(strsplit(s,"\\.")[[1]],1))))]
fxv.releases <- fxv.releases[order(major,minor),]
release.the.current.release <- tail(fxv.releases,1)[, list(version, major,minor, date)]
release.releases.for.model <- fxv.releases[major %in% c(release.the.current.release$major,release.the.current.release$major-1,release.the.current.release$major-2),]


release.what.i.need <- fxv.releases[major %in%  release.the.current.release$major,]
release.what.i.need <- release.what.i.need[,list(v=version,d=date, till=date, c='release',ndays=72, crash_src='telemetry.crash_summary_v2',
                                                 app_version_field='app_version', build_version_field='build_version')]
release.what.i.need <- release.what.i.need[d<'2019-06-18', crash_src := 'telemetry.crash_summary_v1']
release.what.i.need$till= c(tail(release.what.i.need$d,-1),Sys.Date()+365)
g <- bq()





add_fields <- function(x, current_version, d,till ){
  x <- x[,":="(date=as.Date(date))]
  x[,":="(date=as.Date(date),c_version = current_version, c_version_rel = as.Date(d), isLatest=date<=till,t =as.numeric(date - as.Date(d)))][,]
  x<- x[, ":="(
    cmi=  (1+dau_cm_crasher_cversion)/dau_cversion,
    cmr = (1+cmain) / sapply(usage_cm_crasher_cversion,function(s) {if( s< 60/3600){ 0 } else{ s}}),
    cci = (1+dau_cc_crasher_cversion)/dau_cversion,
    ccr = (1+ccontent)/sapply(usage_cc_crasher_cversion,function(s) {if( s< 60/3600){ 0 } else{ s}}),
    nvc=usage_cversion/usage_all,
    os=factor(os, levels=c("Linux","Darwin","Windows_NT")),
    c_version_rel = as.Date(c_version_rel)
  )]
  x
}


######################################################################
## GET NEW DATA FOR CURRENT MAJOR FOR RELEASE
######################################################################

dall.release.new <-   release.what.i.need[,{
    print(.SD);print(.BY)
    qf <- glue(qr,
               current_version=.BY$v,
               current_version_crash=sprintf("'%s'",.BY$v),
               current_version_release=d,
               norm_channel = c,
               app_version_field = app_version_field,
               build_version_field=build_version_field,
               nday=ndays,
               crash_src=isn(crash_src),
               NBUCKS=1
               )
    writeLines(qf,"/tmp/t.txt")
    qff <- g$q(qf,-1)
    if(nrow(qff) > 0)
      add_fields(qff,.BY$v,d,till)
},by=v]
if(nrow(dall.release.new)==0) stop("release: No rows for this data")
dall.release.new <- dall.release.new[!c_version %in% c('66.0.4','66.0.5'),][, {
  ## Take the last day we saw the max
  index <- which.max(nvc)
  s <- (1:.N)[ isLatest]
  if(length(s) > 0){
   whenStoppedBeingLatest <- max( s )
  } else whenStoppedBeingLatest <- -1
  if(index<whenStoppedBeingLatest) index <- whenStoppedBeingLatest
  # .SD[1:whenStoppedBeingLatest,]
  .SD[1:index,]
},by=list(os,c_version)]

dall.release.new <- dall.release.new[, ":="( major = as.numeric(sapply(c_version,function(s) head(strsplit(s,"\\.")[[1]],1))),
                                            minor = as.numeric((sapply(c_version,function(s) tail(strsplit(s,"\\.")[[1]],1)))))]



## BETA: DATA

## FOR BETA NOW: GET BUILD MAPPING WHICH WILL GET REPLACED BY PYTHON CODE



buildhub <- import("buildhub_bid")
builds <- local({
 FIX <- function(f)
   paste(unlist(lapply(f,function(s) sprintf("'%s'",s))),collapse=',')
  x <-  buildhub$pull_build_id_docs()
  x <- buildhub$version2build_ids(x)
  x=rbindlist(Map(function(n,b){
    data.table(versions=n, buildid=FIX(lapply(b,"[[",2)))
  },names(x),x))[order(versions),]
})
  

######################################################################
## GET OLD DATA FROM BQ
## NEEDED FOR MODEL
######################################################################

fxv.beta <- fxv[grepl("(dev)",categ),][date >='2019-01-01',]
fxv.beta <- fxv.beta[, ":="( major=  as.numeric(sapply(version,function(s) head(strsplit(s,"\\.0b")[[1]],1))),
                            minor = as.numeric(sapply(version,function(s) tail(strsplit(s,"\\.0b")[[1]],1))))]
fxv.beta <- fxv.beta[order(major,minor),]
beta.the.current.release <- tail(fxv.beta,1)[, list(version, major,minor, date)]
#beta.the.current.release  <- fxv.beta[60,][, list(version, major,minor, date)]
beta.releases.for.model <- fxv.beta[major %in% c(beta.the.current.release$major,beta.the.current.release$major-1,beta.the.current.release$major-2
                                                 ),]

beta.what.i.need <- fxv.beta[major %in%  beta.the.current.release$major,]
beta.what.i.need <- beta.what.i.need[,list(v=version,d=date, till=date, c='beta',ndays=14, crash_src='telemetry.crash_summary_v2',
                                                                         app_version_field='app_display_version', build_version_field='build_id')]
beta.what.i.need$till= c(tail(beta.what.i.need$d,-1),Sys.Date()+7)
beta.what.i.need[v=='69.0b16', till := as.Date('2019-08-27')] ## I need this because we dont get data for RC builds
beta.what.i.need <- beta.what.i.need[d<'2019-06-18', crash_src := 'telemetry.crash_summary_v1']
beta.what.i.need <- merge(beta.what.i.need,builds[, list(v=versions, buildid)],by=c('v'),all.x=TRUE)




dall.beta.new <-   beta.what.i.need[,{
    print(.SD);print(.BY)
    qf <- glue(qr,
               current_version=.BY$v,
               current_version_crash=buildid,
               current_version_release=d,
               norm_channel = c,
               app_version_field = app_version_field,
               build_version_field=build_version_field,
               nday=ndays,
               crash_src=isn(crash_src),
               NBUCKS=1
               )
    writeLines(qf,'/tmp/x.txt') 
    qff <- g$q(qf,-1)
    if(nrow(qff) > 0)
      add_fields(qff,.BY$v,d,till)
},by=v]
if(nrow(dall.beta.new)==0) stop("beta:No rows for this data")

dall.beta.new <- dall.beta.new[!c_version %in% c('67.0b17','67.0b18'),][, {
  ## Take the last day we saw the max
  index <- which.max(nvc)
  s <- (1:.N)[ isLatest]
  if(length(s) > 0){
   whenStoppedBeingLatest <- max( s )
  } else whenStoppedBeingLatest <- -1
#  if(index<whenStoppedBeingLatest) index <- whenStoppedBeingLatest
  .SD[1:whenStoppedBeingLatest,]
  .SD[1:index,]
},by=list(os,c_version)]
dall.beta.new <- dall.beta.new[,":="( major=  as.numeric(sapply(c_version,function(s) head(strsplit(s,"\\.0b")[[1]],1))),
                                      minor = as.numeric(sapply(c_version,function(s) tail(strsplit(s,"\\.0b")[[1]],1))))]

#dall.new <- rbind(dall.beta.new[,":="(channel='beta')],dall.release.new[,":="(channel='release')])[,":="(v=NULL)]




## NIGHTLY

## Nightly is slightly different. We get the dates of a version and then each
## YYYYMMDD is a minor version. For each minor version we get 7 days of data (we'll
## lop of days later).


## This ideally ought be done via buildhub
nightly <- fxv.beta[minor==3,list(version=substr(version,1,4), date)]
nightly[, ":="(till=c(tail(date,-1),Sys.Date()),
               version = paste(as.numeric(version) +1,".0a1",sep=""))] #HACK
nightly.what.i.need <- tail(nightly,1) #tail(nightly,1)
nightly.what.i.need <- nightly.what.i.need[, data.frame(buildid = strftime(seq(from=date, to=till,by=1),'%Y%m%d')),
                    by=list(v=version)]
nightly.what.i.need[, ":="(d=as.character(strptime(buildid,'%Y%m%d')),
                           c = 'nightly',
                           ndays = 7, crash_src = 'telemetry.crash_summary_v2',
                           app_version_field='substr(app_build_id,1,8)',
                           build_version_field='substr(build_id,1,8)')][, till:=as.Date(d)+7]
nightly.what.i.need <- nightly.what.i.need[d<'2019-06-18', crash_src := 'telemetry.crash_summary_v1']
nightly.releases.for.model <- c(as.numeric(substring(nightly.what.i.need$v,1,2)))
nightly.releases.for.model <- unique(c(nightly.releases.for.model,nightly.releases.for.model-1))


dall.nightly.new <-   nightly.what.i.need[,{
  print(.SD);print(.BY)
    qf <- glue(qr,
               current_version=.BY$buildid,
               current_version_crash=sprintf("'%s'",.BY$buildid),
               current_version_release=d,
               norm_channel = c,
               app_version_field = app_version_field,
               build_version_field=build_version_field,
               nday=ndays,
               crash_src=isn(crash_src),
               NBUCKS=1
               )
  #writeLines(qf, "/tmp/xx")
    qff <- g$q(qf,-1)
    if(nrow(qff) > 0)
      add_fields(qff,.BY$buildid,d,till)
},by=list(v,buildid)]

if(nrow(dall.nightly.new)==0) stop("No rows for this data")

dall.nightly.new <- dall.nightly.new[, {
  ## Take the last day we saw the max
  index <- which.max(nvc)
  .SD[1:index,]
},by=list(os,c_version)]

dall.nightly.new <- dall.nightly.new[,":="( major=  as.numeric(sapply(v,function(s) head(strsplit(s,"\\.0a")[[1]],1))),
                                           minor =  as.numeric(as.character(c_version))
                                           ),by=v][,buildid:=NULL]

dall.new <- rbind(dall.beta.new[,":="(channel='beta')],
                  dall.release.new[,":="(channel='release')],
                  dall.nightly.new[,":="(channel='nightly')]
                  )[,":="(v=NULL)]
dall.new <- dall.new[, list(date,os,usage_all,dau_all,usage_cversion,dau_cversion,dau_cm_crasher_cversion,dau_cc_crasher_cversion,
                            dau_cp_crasher_cversion,dau_call_crasher_cversion,usage_cm_crasher_cversion,
                            usage_cc_crasher_cversion,usage_cp_crasher_cversion,usage_call_crasher_cversion,
                            cmain,ccontent,cplugin,call,c_version,c_version_rel,isLatest,t,cmi,cmr,cci,ccr,nvc,major,minor,channel)]







## What versions, channels, and os did we read today?
unq <- dall.new[, list(n=.N),by=list(major, channel)]

## delete them from BQ table
res <- unq[,{
  del.q <- glue("delete  from analysis.sguha_crash_rate_raw  where major={major} and channel='{channel}'",major=.BY$major, channel=.BY$channel)
  g$q(del.q)
},by=list(major,channel)]

## Upload new data
atemp <- tempfile()
write.csv(dall.new, file=atemp,row.names=FALSE)
system(glue("bq load --noreplace --project_id moz-fx-data-derived-datasets --source_format=CSV --skip_leading_rows=1 --null_marker=NA",
            " analysis.sguha_crash_rate_raw {atemp}"))
unique(nightly.releases.for.model)
## Created FIRST TABLE via
#system(glue("bq load  --project_id moz-fx-data-derived-datasets --autodetect --replace  --source_format=CSV --skip_leading_rows=1 --null_marker=NA",
#            " analysis.sguha_crash_rate_raw {atemp}"))




## GET MODEL DATA
## Current and last two versions:


dall.rel2 <- g$q(glue("select * from analysis.sguha_crash_rate_raw where channel = 'release' and major in ({whichv})",
         whichv = paste(unique(release.releases.for.model[,major]),collapse=",")),-1)
dall.rel2 <- dall.rel2[nvc > 0,] 

dall.beta2 <- g$q(glue("select * from analysis.sguha_crash_rate_raw where channel = 'beta' and major in ({whichv})",
         whichv = paste(unique(beta.releases.for.model[,major]),collapse=",")),-1)
dall.beta2 <- dall.beta2[nvc > ,0]

dall.nightly2 <- g$q(glue("select * from analysis.sguha_crash_rate_raw where channel = 'nightly' and major in ({whichv})",
                          whichv = paste(unique(nightly.releases.for.model),collapse=',')),-1)

dall.nightly2 <- dall.nightly2[nvc > 0,]


addOSFactor <- function(d){
    d[, osGroup := sapply(os, function(k) if(k=="Linux") "Linux" else "Win/Darwin")]
    d
}
dall.rel2 <- addOSFactor(dall.rel2)
dall.beta2 <- addOSFactor(dall.beta2)
dall.nightly2 <- addOSFactor(dall.nightly2)
##dall.rel2[, wts := 1]
##dall.beta2[, wts:=1]
##dall.nightly2[,wts := 1]


## BUILD MODELS

## Release model


slack.start("cryptojoy2")
slackr("RELEASE CHAINS STARTING")
d.rel <- dall.rel2
cr.cm.rel.f <- future({ make.a.model(d.rel,'cmr') })
cr.cc.rel.f <- future({ make.a.model(d.rel,'ccr') })
ci.cm.rel.f <- future({ make.a.model(d.rel,'cmi') })
ci.cc.rel.f <- future({ make.a.model(d.rel,'cci') })

cr.cm.rel <- label(value(cr.cm.rel.f),'cmr')
cr.cc.rel <- label(value(cr.cc.rel.f),'ccr')
ci.cm.rel <- label(value(ci.cm.rel.f),'cmi')
ci.cc.rel <- label(value(ci.cc.rel.f),'cci')
slackr("RELEASE CHAINS DONE")


## ## https://stats.stackexchange.com/questions/228800/crossed-vs-nested-random-effects-how-do-they-differ-and-how-are-they-specified
## dd <- d.beta[, ":="(major=as.character(major),minor=as.character(minor))]
## dd <- dd[cmain > 0,]
## M00 <- bf( cmain + 1  ~ offset(log(usage_cm_crasher_cversion + 1/60)) + os +
##                (1+os|major)+(1+os | major:minor) ,
##           shape ~ os)+negbinomial()
## cr.cm.beta.2 <- future(make.a.model(dd,'cmr',bff=M00, list0= list(adapt_delta = 0.999, max_treedepth=12)))
## M000 <- bf( cmain + 1 ~ offset(log(usage_cm_crasher_cversion +1/60)) + os +
##                (1+os|c_version),
##           shape ~ os)+negbinomial()
## cr.cm.beta.1 <- future(make.a.model(dd,'cmr',bff=M000, list0= list(adapt_delta = 0.999, max_treedepth=12)))

## cr.cm.beta.1 <- value(cr.cm.beta.1)
## cr.cm.beta.2 <- value(cr.cm.beta.2)

## M01 <- bf( cmain + 1  ~ offset(log(usage_cm_crasher_cversion + 1/60)) + os +
##                (1+os | major:minor) ,
##           shape ~ os)+negbinomial()
## cr.cm.beta.3 <- value(future(make.a.model(dd,'cmr',bff=M01, list0= list(adapt_delta = 0.999, max_treedepth=12))))

## Beta Model


slackr("BETA CHAINS STARTING")
d.beta <- dall.beta2
cr.cm.beta.f <- future({ make.a.model(d.beta,'cmr',channel='beta') })
cr.cc.beta.f <- future({ make.a.model(d.beta,'ccr',channel='beta') })
ci.cm.beta.f <- future({ make.a.model(d.beta,'cmi',channel='beta') })
ci.cc.beta.f <- future({ make.a.model(d.beta,'cci',channel='beta') })
cr.cm.beta <- label(value(cr.cm.beta.f),'cmr')
cr.cc.beta <- label(value(cr.cc.beta.f),'ccr')
ci.cm.beta <- label( value(ci.cm.beta.f),'cmi')
ci.cc.beta <- label(value(ci.cc.beta.f),'cci')
slackr("BETA CHAINS DONE!")




## Nightly Model


slackr("Nightly CHAINS STARTING")
d.nightly <- dall.nightly2
cr.cm.nightly.f <- future({ make.a.model(d.nightly,'cmr',channel='nightly') })
cr.cc.nightly.f <- future({ make.a.model(d.nightly,'ccr',channel='nightly') })
ci.cm.nightly.f <- future({ make.a.model(d.nightly,'cmi',channel='nightly') })
ci.cc.nightly.f <- future({ make.a.model(d.nightly,'cci',channel='nightly') })
cr.cm.nightly <- label(value(cr.cm.nightly.f),'cmr')
cr.cc.nightly <- label(value(cr.cc.nightly.f),'ccr')
ci.cm.nightly <- label(value(ci.cm.nightly.f),'cmi')
ci.cc.nightly <- label(value(ci.cc.nightly.f),'cci')
slackr("Nightly CHAINS DONE!")





### ROugh Work: To What Extent Does Sigma Depend on NVC
if(FALSE){

    ci.cc.nightly.old <-  label(make.a.model(d.nightly,'cci'
                                            ,bff= bf( log(1 + dau_cc_crasher_cversion) |weights(wts) ~ os + offset(log(dau_cversion)) + s(nvc, m = 1) + (1 + os | c_version),sigma ~ os+ nvc)
                                             ,channel='nightly'),'cci')

    ci.cc.beta.new <- label(make.a.model(d.beta,'cci',channel='beta'),'cci')
    
    f=rbind(d.nightly[, list(nvc, cmr,ccr,cci,cmi,os,ch='nightly')],
        d.beta[, list(nvc, cmr,ccr,cci,cmi,os,ch='beta')],
        d.rel[, list(nvc, cmr,ccr,cci,cmi,os,ch='rel')])[order(ch,os, nvc),]

ggplot(f[ch=='nightly',], aes(nvc, cci,color=os))+geom_point()+geom_smooth()
dev.off()


    cr.cm.beta.new2<- future({make.a.model(d.beta,'cmr',channel='beta')})
    cr.cm.rel.new<- future({make.a.model(d.rel,'cmr',channel='release')})    
    cr.cm.beta.new2<- value(cr.cm.beta.new2)
    cr.cm.rel.new <- value(cr.cm.rel.new)

    cr.cm.nightly.new <-  make.a.model(d.beta,'cmr',channel='nightly')
    
f2 <- f[cmr<=Inf,{
    g <- loess(cmi ~ nvc)
    list(r = fitted(g), nvc=nvc) #fitted(g))
},by=list(os, ch)]
    
H <- function(x) abs(x)
ggplot(f2[ch=='rel' & r<quantile(r,0.99),], aes(nvc, H(r),color=os))+geom_point()+geom_smooth(method='loess')
ggplot(f2[ch=='beta'  & r<quantile(r,0.99),], aes(nvc, H(r),color=os))+geom_point()+geom_smooth(method='loess')
ggplot(f2[ch=='nightly'  & r<quantile(r,0.99),], aes(nvc, H(r),color=os))+geom_point()+geom_smooth(method='loess')
dev.off()


}
