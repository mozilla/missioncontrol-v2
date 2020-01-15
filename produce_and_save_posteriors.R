setwd("~/missioncontrol-v2/")
source("missioncontrol.lib.R")


## From the models, generate posteriors and save to BQ
## Also create some summary tables for BQ

## Call as Rscript produce_and_save_posteriors.R --data_file=default is ./all.the.data.intermediate.Rdata --out=default is ./all.the.data.Rdata
command.line <- commandArgs(asValues=TRUE,defaults=list(data_file="./all.the.data.intermediate.Rdata",out="./all.the.data.Rdata"),unique=TRUE)
loginfo(glue("loading data file from {command.line$data_file}"))
load(command.line$data_file)


######################################################################
## Produce Posteriors
######################################################################

model.date <- max(dall.rel2$date)
last.model.date <- system("bq query --format=prettyjson --nouse_legacy_sql 'select max(model_date) as x from `moz-fx-data-derived-datasets`.analysis.missioncontrol_v2_posteriors'",intern=TRUE)
last.model.date <- rjson::fromJSON(paste(last.model.date,collapse="\n"))[[1]]$x

rel.list <- list(cmr = cr.cm.rel, ccr = cr.cc.rel, cmi = ci.cm.rel, cci = ci.cc.rel)
ll.rel <- make_posteriors(dall.rel2, CHAN='release', model.date = model.date,model.list=rel.list,last.model.date= last.model.date)

if(nrow(ll.rel)>0) loginfo(glue("Release Posteriors updated to include {model.date}"))

beta.list <- list(cmr = cr.cm.beta, ccr = cr.cc.beta, cmi = ci.cm.beta, cci = ci.cc.beta)
ll.beta <- make_posteriors(dall.beta2, CHAN='beta', model.date = model.date,model.list=beta.list,last.model.date= last.model.date)
if(nrow(ll.beta)>0) loginfo(glue("Beta Posteriors updated to include {model.date}"))

nightly.list <- list(cmr = cr.cm.nightly, ccr = cr.cc.nightly, cmi = ci.cm.nightly, cci = ci.cc.nightly)
ll.nightly <- make_posteriors(dall.nightly2, CHAN='nightly', model.date = model.date,model.list=nightly.list,last.model.date= last.model.date)
if(nrow(ll.nightly)>0) loginfo(glue("Nightly Posteriors updated to include {model.date}"))

all.posteriors <- rbindlist(list(ll.rel,ll.beta,ll.nightly))
if(nrow(all.posteriors)>0){
    ## Now write this
    atemp <- tempfile()
    fwrite(all.posteriors, file=atemp,row.names=FALSE,quote=TRUE,na=0)
    system(glue("bq load  --noreplace --project_id moz-fx-data-derived-datasets   --source_format=CSV --skip_leading_rows=1 --null_marker=NA",
            " analysis.missioncontrol_v2_posteriors {atemp} ./posterior_schema.json"))
}

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
atemp <- tempfile()
fwrite(channel.summary, file=atemp,row.names=FALSE,quote=TRUE,na=0)
system(glue("bq load --replace   --project_id moz-fx-data-derived-datasets  --source_format=CSV --skip_leading_rows=1 --null_marker=NA",
                " analysis.missioncontrol_v2_channel_summaries {atemp} ./channel_summary_schema.json"))
loginfo("Uploaded Channel Summary")
cat(channel.summary)





## SAVE DATA to all.the data.Rdata

######################################################################
## Some Demo Queries
## The can power any dashboard
######################################################################
demo.query <- FALSE
if(demo.query){
g <- bq()


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
order by modelname, os,major DESC,date DESC,minor DESC
)
select * from e
"),-1)

} # end demo.query
