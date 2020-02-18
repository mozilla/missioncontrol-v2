suppressPackageStartupMessages({
library(logging)
library(R.utils)
library(glue)
library(data.table)
library(vegawidget)
library(DT)
library(formattable)
library(parallel)
library(brms)
library(rjson)
library(future)
library(curl)
library(feather)
library(rmarkdown)
})
                                        #library(future.apply)
options(error = function() traceback(3))
options(future.globals.maxSize= 850*1024^2 )
options(width=200)
Lapply <- lapply #future_lapply

BQCREDS <- Sys.getenv("GOOGLE_APPLICATION_CREDENTIALS", "~/gcloud.json")
GCP_PROJECT_ID <- Sys.getenv("GCP_PROJECT_ID", "moz-fx-data-derived-datasets")
GCS_OUTPUT_PREFIX <- Sys.getenv("GCS_OUTPUT_PREFIX", "gs://moz-fx-data-derived-datasets-analysis/sguha/missioncontrol-v2")


if(!exists("missioncontrol.lib.R")){
    ## executed only once
    basicConfig()
    plan(multisession)
    plan(sequential)
    ##    options(future.globals.onReference = "error")
    missioncontrol.lib.R <- TRUE
}


## Based on the data the following apple-apple comparison adoptions were chose
## to estimate final rates
## e.g.  dall.rel2[, list(a=.SD[,max(nvc)]),by=list(c_version,os)][, quantile(a,0.5),by=os]
adoptionsCompare <- function(os,ch,DF=FALSE){
    f <- data.table(channel =rep(c("release","beta","nightly","esr"),each=3),
                    os  = rep(c("Linux","Windows_NT","Darwin"),4),
                    ## used predict(mgcv( log(usage_cc) ~ os+s(nvc,by=os)), f)
                    ## e.g. dall.beta2[, os:=factor(os)][, exp(predict(gam( log(dau_cversion)~ os+s(nvc, by=os)),newdata=g)) ]
                    dau = c(
                        912182.2, 17281273.0  ,2997185.1,
                        1040.123, 489639.889 ,  4818.843,
                        821.7477 ,12506.7056,  1218.0142,
                        1505.707, 401217.766 , 24594.496
                        ),
                    ## dall.beta2[, os:=factor(os)][, exp(predict(gam( log(dau_cversion)~ os+s(nvc, by=os)),newdata=g)) ]
                    usage_cm = c(
                        22172.91, 156151.10  ,11969.40,
                        10.68346, 7808.63684,   40.62532,
                        29.27896, 262.19025,  25.99481,
                        19.41971 ,12061.76251  ,  82.97752
                       ),
                    usage_cc = c(
                        18377.15 ,219896.37 , 16004.87,
                        11.88207, 13508.18048 ,   30.10713,
                        31.74024, 634.10478 , 22.34219,
                         17.38312, 12928.50682  , 110.84559
                        ),
                    ## i got these numbers based on medians of adoptions
                    adopt =  c(0.4468343,0.7819889,0.5782132,
                               0.2464732,0.4819733,0.293562,
                               0.2593343, 0.3811480,0.2736847,
                               0.01426907,0.14256519,0.19504157
                               ))
    if(!DF) f[channel==ch & os==os, adopt] else f
}

getArchiveLoc <- function(){
    return(glue("{GCS_OUTPUT_PREFIX}/archive"))
}

loadArchiveData<- function(date=NULL,path=NULL
                          ,loc=getArchiveLoc()){
    if(!is.null(path)){
        system(glue("gsutil cp {path}  /tmp/foo.Rdata"))
        e <- new.env()
        load(glue("/tmp/foo.Rdata"),envir=e)
    }
    if(!is.null(date)){
        system(glue("gsutil cp {loc}/models-{date}.Rdata /tmp/"))
        e <- new.env()
        load(glue("/tmp/models-{date}.Rdata"),envir=e)
    }
    if(is.null(path) & is.null(date)) stop("Error: need to specify at least one of date or path")
    e
}


ffunc <- function(M,D,list0=NULL,iter=4000,thin=1,chains=4,cores=4)  {
 brm(M,data=D, chains = chains,
                                       control = if(is.null(list0))
                                                     list(adapt_delta = 0.999, max_treedepth=13)
                                                 else list0
   , cores = cores,iter=iter,thin=thin)
 }

make.a.model <- function(data,wh,channel='not-nightly',debug=0,bff=NULL,list0=NULL,iter=4000,thin=1,priorSim=FALSE){
    ## See wbeards work on nightly: https://metrics.mozilla.com/protected/wbeard/mc/nightly_model.html
    ## Found this useful for comparing models
    ##  /home/sguha/crons/myrscript.sh /home/sguha/crons/rcrondriver.R /home/sguha/mz/missioncontrol/ex1/mc2/missioncontrol_v2_runner.R
    ## y[, x:=(fitted(cmr1)[,'Estimate'])/ (usage_cm_crasher_cversion+1/60)][,list(m1=mean(cmr),m2=mean(x))   ,by=list(os,c_version)][, list(mean((m1-m2)),100* mean(abs(m1-m2)/(1+m1)),sqrt(mean((m1-m2)^2)),cor(m1,m2))]
    ### y[, x:=-1+exp(fitted(cmr2)[,'Estimate'])][,list(m1=mean(cmr),m2=mean(x))   ,by=list(os,c_version)][, list(mean(abs(m1-m2)),100* mean((m1-m2)/(1+m1)), sqrt(mean((m1-m2)^2)),cor(m1,m2))]
    ## Get outpu tvia
    ## rel.list <- list(cmr = label(cmr1,'cmr'), ccr = label(ccr2,'ccr'), cmi = label(cmi2,'cmi'), cci = label(cci2,'cci'))
    ## ll.rel <- make_posteriors(y, CHAN='esr', model.date = '2020-02-01',model.list=rel.list,last.model.date= '2019-01-01')
    ## f <- ll.rel[os=='Windows_NT',list(m=mean(posterior), l = quantile(posterior,0.05), u = quantile(posterior,1-0.05)),
    ##   by=list(modelname,c_version,major,minor,os)][order(modelname,os,major,minor),][, del:=(u-l)/m][,]

    alter <- TRUE
    if(wh=="cmr"){
        M0 <- bf(  log(1+cmr)  ~  os +  s(nvc, m = 1) + (1+os|c_version),sigma~os  )
        if(channel %in% c('beta')){
            M0 <- bf(  log(1+cmr)  ~  os +  s(nvc, m = 1) + (1+os|c_version),sigma~os  )
        }
        if(channel %in% c("nightly")){
            M0 <- bf(  log(1+cmr)  ~  os +  s(nvc, m = 1) + (1+os|c_version),sigma~os  )
        }
        if(channel %in% c("esr")){
            M0 <- bf(  cmain+1   ~  os+offset(log( usage_cm_crasher_cversion+1/60)) +  s(nvc, m = 1) + (1+os|c_version)
                    ,shape~os )+negbinomial()
        }
        if(debug==1){
            M0 <-  bf(  log(1+cmr)  ~  os  )
            }
        if(!is.null(bff)) M0 <- bff
    }
    if(wh=='ccr'){
        M0 <- bf(  log(1+ccr)  ~  os +  s(nvc, m = 1) + (1+os|c_version),sigma~os  )
        if(channel %in% c('beta')){
            M0 <- bf(  log(1+ccr)  ~  os +  s(nvc, m = 1) + (1+os|c_version),sigma~os  )
        }
        if(channel %in% c("nightly")){
            M0 <- bf(  log(1+ccr)  ~  os +  s(nvc, m = 1) + (1+os|c_version),sigma~os  )
        }
        if(channel %in% "esr"){
            M0 <- bf(  log(1+ccr)  ~  os +  s(nvc, m = 1) + (1+os|c_version),sigma~os  )
        }
        if(debug==1){
                M0 <- bf(  log(1+ccr)  ~  os +  nvc )
        }
        if(!is.null(bff)) M0 <- bff
    }
    if(wh=='cmi'){
        M0 <-  bf( cmi.logit   ~   os+ s(nvc,m=1) + (1+os|c_version), sigma ~ os)
        if(channel %in% c('beta')){
            M0 <-  bf( cmi.logit   ~   os+ s(nvc,m=1) + (1+os|c_version), sigma ~ os)
        }
        if(channel %in% c('nightly')){
            M0 <-  bf( cmi.logit   ~   os+ s(nvc,m=1) + (1+os|c_version), sigma ~ os)
        }
        if(channel %in% c("esr")){
            M0 <-  bf( cmi.logit   ~   os+ s(nvc,m=1) + (1+os|c_version), sigma ~ os)
        }
        if(debug==1){
            M0 <-  bf( cmi.logit   ~   os)
        }
        if(!is.null(bff)) M0 <- bff
    }
    if(wh=='cci'){
        M0 <-  bf( cci.logit   ~   os+ s(nvc,m=1) + (1+os|c_version), sigma ~ os)
        if(channel %in% c('beta')){
            M0 <-  bf( cci.logit   ~   os+ s(nvc,m=1) + (1+os|c_version), sigma ~ os)
        }
        if(channel %in% c("nightly")){
            M0 <-  bf( cci.logit   ~   os+ s(nvc,m=1) + (1+os|c_version), sigma ~ os)
        }
        if(channel %in% c("esr")){
            M0 <-  bf( cci.logit   ~   os+ s(nvc,m=1) + (1+os|c_version), sigma ~ os)
        }
        if(debug==1){
            M0 <-  bf( cci.logit   ~   os)
        }
        if(!is.null(bff)) M0 <- bff
    }
    print(M0)
    if(priorSim){ return(M0) }
    else  ffunc(M0,data,list0=list0,thin=thin,iter=iter, chains=if(debug==1) 1 else 4, cores=if(debug==1) 1 else 4)
}


Predict <- function(M,D,ascale='response',...){
    fa <- family(M)$fam
    l <- list(...)
    if(fa=='negbinomial' & is.null(l$leaveScaleAlone)) ascale='linear'
    p <- fitted(M,newdata=D,scale=ascale,...)
    return(p)
}


getPredictions <- function(M,D, wh=NULL,givenx=NULL,summary=FALSE,ascale='response',...){
    fa <- family(M)$fam
    #if(fa=='gaussian') inv.lnk <- exp else inv.lnk <- function(x) x
    if(is.null(wh)){
        wh <- attr(M,"model.type")
        if(is.null(wh)) stop("model type is missing")
    }
    if(is.null(givenx)){
        x <-Predict(M,D,ascale=ascale,summary=summary,...)
    }else{
        x <-givenx
    }
    if(wh=='cmr'){
        if(fa == 'gaussian'){
            r <-  pmax(0,exp(t(x))-1)
        }else if(fa=='negbinomial'){
            r <- exp( t(x) - D[, log( usage_cm_crasher_cversion+1/60)])
        }else stop(glue('what family? {fa}'))
    }else if(wh=='ccr'){
        if(fa=='gaussian'){
            r <-  pmax(0,exp(t(x))-1)
        }else if(fa=='negbinomial'){
            r <- exp(t(x) -  D[, log( usage_cc_crasher_cversion+1/60)])
        }else stop(glue('what family? {fa}'))
    }else if(wh=='cmi'){
        if(fa=='binomial'){
            r <- t(x) #boot::inv.logit(t(x))
        }else{
            r <- boot::inv.logit(t(x))
            r[r>1]  <- 1
        }
    }else if(wh=='cci'){
        if(fa=='binomial'){
            r <- t(x) #boot::inv.logit(t(x))
        }else {
            r <- boot::inv.logit(t(x))
            r[r>1] <- 1
        }
    }
    r
}


label <- function(M,x){
    S <- M
    attr(S,"model.type") <- x
    S
}





makePredictions <- function(model,D){
    if(is.null(model$scope)){
        a <- getPredictions(model, D)
    }else{
        if(model$scope=='rate'){
            a1 <- getPredictions(model$mr, D)
            a2 <- getPredictions(model$cr,D)
        }else{
            a1 <- getPredictions(model$mi, D)
            a2 <- getPredictions(model$ci,D)
        }
        a <- a1+a2
    }
    a
}

getCurrentVersion <- function(D,s, channel){
    if(s=="overall") s <- "Windows_NT"
  getReleaseVersion <- function(D, s){
    DD <- D[os==s,]
    DD[major==max(major),][minor==max(minor),][, c_version][1]
  }
  getBetaVersion <- function(D, s){
    DD <- D[os==s,]
    DD[major==max(major),][minor==max(minor),][, c_version][1]
  }
  getNightlyVersion <- function(D, s){
    DD <- D[os==s,]
    DD[minor==max(minor),c_version][1]
  }
  if(channel == 'release') return(getReleaseVersion(D,s))
  if(channel == 'beta') return(getBetaVersion(D,s))
  if(channel == 'nightly') return(getNightlyVersion(D,s))
}

getMaxVersionBeforeX <- function(D,s, channel,X){
  DD <- D[!c_version %in% X,]
  return(getCurrentVersion(DD,s,channel))
}

getPreviousVersion <- function(D,s, channel){
  return(getMaxVersionBeforeX(D,s,channel, X = getCurrentVersion(D,s,channel)))
}





Nposterior <- 1000

make_posterior_newdata <- function(mydata,model_date,CHAN){
    mydata <- mydata[, list(channel=CHAN,date,os,c_version,major,minor,nvc, dau_cversion,  dau_cc_crasher_cversion, dau_cm_crasher_cversion,
                            ccontent, cmain, usage_cc_crasher_cversion,usage_cm_crasher_cversion,cmr,ccr,cmi,cci,cmi.logit,cci.logit)]
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
    #mydata.posteriors.at[, date:=as.Date(model.date)] #as.Date('2970-01-01')]
    ## Posteriors for CR(M,C) and CI(M,C)
    ## For Operating Systems
    posterior.os.individual.metrics <- rbindlist(Map(function(m, w){
        mydata.posteriors <- getPredictions(M=m,D=mydata.posteriors.at,nsamples=Nposterior)
        mydata.posteriors <- rbindlist(lapply(1:nrow(mydata.posteriors.at),function(i){
            cbind( mydata.posteriors.at[i,],data.table(rep=1:Nposterior, modelname = w,posterior=mydata.posteriors[i,]))
        }))
    }, m=model.list,w=names(model.list)))

    ## Posteriors for CR-Score and CI-Score
    ## For operating systems

    posterior.os.individual.metrics <- rbind(posterior.os.individual.metrics,
                                             posterior.os.individual.metrics[,rbind(
        data.table(modelname = 'cr', posterior = .SD[modelname=='cmr',posterior]+ .SD[modelname=='ccr',posterior]),
        data.table(modelname = 'ci', posterior = .SD[modelname=='cmi',posterior]+ .SD[modelname=='cci',posterior])),
        by=list(channel, os, c_version,major,minor,model_date,
                date,
                nvc, dau_cversion, usage_cc_crasher_cversion, usage_cm_crasher_cversion, rep)]
       )

    ## Now OVERALL SCORES
    ttmp <-  posterior.os.individual.metrics
    ttmp2 <- ttmp[,{
        if(.BY$modelname=='cmi') { X<<-.SD; Y<<-.BY}
        osdarwin  <- .SD[os=='Darwin',][order(rep),]
        oswindows <- .SD[os=='Windows_NT',][order(rep),]
        oslinux   <- .SD[os=='Linux',][order(rep),]
        ns <- c(darwin=nrow(osdarwin),windows=nrow(oswindows),linux=nrow(oslinux))
        if(any(ns==0))
            loginfo(glue("OS LEVEL SCORE: missing operating system data for {paste(c(.BY$modelname,.BY$c_version,as.character(.BY$model_date)),collapse=',')} ones absent: {paste(names(ns)[ns==0],collapse=',')}"))
        if(!(sum(ns==0)>=2) ){
            osall <- apply(cbind(osdarwin[,posterior], oswindows[,posterior], oslinux[,posterior]),1,mean)
            if(length(osall) >0){
                .SD[, list(os='overall', nvc=0, dau_cversion=0, usage_cc_crasher_cversion =0,usage_cm_crasher_cversion=0, rep=1:length(osall), posterior=osall)]
            }
        }
    },by=list(channel, c_version, major, minor,
              date,
              model_date,modelname)]
    
    if(nrow(ttmp2)>0){
        ttmp2 <- ttmp2[, list(channel,os,c_version,major,minor,model_date, date, nvc,dau_cversion,usage_cc_crasher_cversion,usage_cm_crasher_cversion,rep,modelname,posterior)]
        posterior.os.all <- rbind(ttmp,ttmp2)
    }else posterior.os.all <- ttmp
    posterior.os.all[, list(channel,os,c_version,major,minor,model_date, date, nvc,dau_cversion,usage_cc_crasher_cversion,usage_cm_crasher_cversion,modelname,rep,posterior)]
}


######################################################################
## SQL QUERIES USING THE ABOVE APPROACH
######################################################################
posterior.current.versions <- function(channel,g){
    chan <- channel
     g$q(glue('select
c_version as Current_Version,
ncurrent as Days_on_Current,
o_version as Older_Version,
nolder as Days_on_Older,
asOf as asOf
from  analysis.missioncontrol_v2_channel_summaries
where channel="{chan}" and os="Windows_NT"
'))
}

model.summary <- function(g){
    g$q("
select
JSON_EXTRACT_SCALAR(smry,'$.w')  as allmods
from  analysis.missioncontrol_v2_channel_summaries
limit 1
")
}

posterior.usage <- function(channel,g){
    chan <- channel
 g$q(glue("
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
}

posterior.rate.incidence.summary <- function(channel,g){
    chan <- channel
    g$q(glue("
CREATE  TEMP FUNCTION  whatColor(i float64, r float64) AS (
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
  select case when color = 'red' then format('%s ▲', X)
              when color = 'darkorange' then format('%s △', X)
              when color = 'limegreen' then format('%s ▼',X)
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
}

posterior.evolution <- function(channel, nmonth=3,g){
    nmonth <- as.integer(nmonth)
    chan=channel
    x <-  g$q(glue("
with
a0 as (
select os,modelname,c_version as cv,date as date,model_date,
APPROX_QUANTILES(posterior, 100)[OFFSET(50)] as c,
APPROX_QUANTILES(posterior, 100)[OFFSET(5)] as lo90,
APPROX_QUANTILES(posterior, 100)[OFFSET(95)] as hi90,
from `moz-fx-data-derived-datasets`.analysis.missioncontrol_v2_posteriors
where channel='{chan}' and date>=DATE_SUB(CURRENT_DATE(), INTERVAL {nmonth} MONTH)
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
select  os, c_version as cv, major,minor,date, nvc as adoption, cmr,ccr,cmi,cci
from `moz-fx-data-derived-datasets`.analysis.missioncontrol_v2_raw_data
where channel = '{chan}'
),
e as (
select A.os,A.date, A.cv,major,minor,adoption, modelname, c,lo90,hi90,cmr,ccr,cmi,cci
from c A left join d
on A.os=d.os and A.date=d.date and A.cv=d.cv
order by modelname, os,major DESC,date DESC,minor DESC
),
f as (
select
os,date,cv,major,minor,adoption,modelname,
case when modelname ='cci' then cci
    when modelname ='cmi' then cmi
    when modelname ='ccr' then ccr
    when modelname ='cmr' then cmr
    else -1 end as orig,
c,lo90,hi90
from e
)
select * from f
"),-1)
    x[, channel := channel]
    x
}

posterior.version.change <- function(channel, nmonth){
    ## this query is for studying how the model estimates a version as
    ## new data comes in. The difference between this and the above is
    ## that the above takes value of a crash metric for the latest
    ## model for that version(so only one estimate per version) this
    ## is longitudinal estimates per version.
    ## Useful for long running versions such as Release/ESR to see if a
    ## change of environment caused a crash spike wrt yesterday/
    ## This should be one more graph.
    nmonth <- as.integer(nmonth)
    chan  <- channel
    x <- g$q(glue("
with
a0 as (
select os,modelname,c_version as cv,date as date,model_date,
APPROX_QUANTILES(posterior, 100)[OFFSET(50)] as c,
APPROX_QUANTILES(posterior, 100)[OFFSET(5)] as lo90,
APPROX_QUANTILES(posterior, 100)[OFFSET(95)] as hi90,
from `moz-fx-data-derived-datasets`.analysis.missioncontrol_v2_posteriors
where channel='{chan}' and date>=DATE_SUB(CURRENT_DATE(), INTERVAL {nmonth} MONTH)
--and os != 'overall'
group by 1,2,3,4,5
order by 2,1,3,4,5
),
b as (select
 os,modelname,cv, date, model_date,c, lo90,hi90,
 row_number() OVER mywindow AS n_
  from a0
 WINDOW mywindow AS (PARTITION BY os,cv,modelname,model_date ORDER BY date DESC )
),
c as (select * except(n_) from b where n_ = 1),
d as (
select  os, c_version as cv, major,minor,date, cmr,ccr,cmi,cci,nvc as adoption
from `moz-fx-data-derived-datasets`.analysis.missioncontrol_v2_raw_data
where channel = '{chan}'
),
e as (
select A.os,A.date, A.cv,major,minor,adoption, modelname, model_date,c,lo90,hi90,cmr,ccr,cmi,cci
from c A left join d
on A.os=d.os and A.date=d.date and A.cv=d.cv
order by modelname,model_date, os,major DESC,date DESC,minor DESC
),
f as (
select
os,date,cv,major,minor,adoption,modelname,model_date,
case when modelname ='cci' then cci
    when modelname ='cmi' then cmi
    when modelname ='ccr' then ccr
    when modelname ='cmr' then cmr
    else -1 end as orig,
c,lo90,hi90
from e
),
g as (
select os, cv,model_date,c,modelname, lo90,hi90,adoption from f
where os!='overall' and  modelname in ('cr','ci') and cv=(select c_version from  analysis.missioncontrol_v2_channel_summaries where os='Windows_NT' and channel='{chan}')
order by os,model_date
)
select * from g
"))
}
