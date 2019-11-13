library(logging)
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
#library(future.apply)
options(future.globals.maxSize= 850*1024^2 )
Lapply <- lapply #future_lapply

BQCREDS <- "~/gcloud.json"

if(!exists("missioncontrol.lib.R")){
    ## executed only once
    basicConfig()
    plan(multicore)
    missioncontrol.lib.R <- TRUE
}


## Based on the data the following apple-apple comparison adoptions were chose
## to estimate final rates
## e.g.  dall.rel2[, list(a=.SD[,max(nvc)]),by=list(c_version,os)][, quantile(a,0.5),by=os]
adoptionsCompare <- function(os,ch,DF=FALSE){
    f <- data.table(channel =rep(c("release","beta","nightly"),each=3),
               os  = rep(c("Linux","Windows_NT","Darwin"),3),
               adopt =  c(0.4468343,0.7819889,0.5782132,
                          0.2690685,0.5137107,0.3553570,
                          0.2593343, 0.3811480,0.2736847))
    if(!DF) f[channel==ch & os==os, adopt] else f
}

getArchiveLoc <- function(){
    return("gs://moz-fx-data-derived-datasets-analysis/sguha/missioncontrol-v2/archive")
}

loadArchiveData<- function(date
           ,loc=getArchiveLoc()){
    system(glue("gsutil cp {loc}/models-{date}.Rdata /tmp/"))
    e <- new.env()
    load(glue("/tmp/models-{date}.Rdata"),envir=e)
    e
}

fitFromModel <- function(date,ch,newdata,normalizeNVC=TRUE,loc=getArchiveLoc(),bindData=NULL){
    if(normalizeNVC == TRUE){
        pp=adoptionsCompare(DF=TRUE)
        newdata2 <- merge(newdata,pp,by=c("os","channel"))
        newdata2[, nvc := adopt];
    }else{
        newdata2 <- newdata
    }
    rbindlist(lapply(date,function(d){
        x <- tryCatch({
            loadArchiveData(d,loc)
        }, error=function(e){
            warning(glue("Could not find {d} in {loc}, ignoring"))
            NULL
        })
        if(is.null(x)) return(NULL)
        M <- if(ch=='release') {
                 list( mr=x$cr.cm.rel,cr=x$cr.cc.rel,mi=x$ci.cm.rel,ci=x$ci.cc.rel)
             }else if(ch=='beta'){
                 list( mr=x$cr.cm.beta,cr=x$cr.cc.beta,mi=x$ci.cm.beta,ci=x$ci.cc.beta)
             }else if(ch=='nightly'){
                 list( mr=x$cr.cm.nightly,cr=x$cr.cc.nightly,mi=x$ci.cm.nightly,ci=x$ci.cc.nightly)
             }else stop("bad channel")
        y <- cbind(data.table(model_date = d,channel=ch),getCredibleIntervals(newdata2,M))
        if (is(bindData,"data.frame")){
           y <-  cbind(bindData,y)
        }
        y
    }))
}


fittedTableForBQ <- function(thedata,models){
    pp=adoptionsCompare(DF=TRUE)
    newdata2 <- merge(thedata,pp,by=c("os","channel"))
    newdata2[, originalnvc := nvc]
    newdata2[, nvc := adopt];
    newdata2 <- newdata2[,{
        tail(.SD[order(date),],1)
        },by=list(channel,os, c_version,major,minor)]
    y <- cbind(newdata2[,list(date,channel,os,c_version,major,minor,nvc=originalnvc, nvc_baseline=nvc,cmr,ccr,cmi,cci)],getCredibleIntervals(newdata2,models))
    colnames(y) <- gsub("\\.","_",colnames(y))
    y
}
    
## Example:
## fitFromModel(c("2019-10-30","2019-10-31","2019-11-01"), ch='nightly', newdata=dnew,bindData=dnew[,list(os,c_version,date,nvc,cmr,ccr,cmi,cci)])
        

ffunc <- function(M,D,list0=NULL,iter=4000,thin=1)  brm(M,data=D, chains = 4,
                                       control = if(is.null(list0))
                                                     list(adapt_delta = 0.999, max_treedepth=13)
                                                 else list0
                                     , cores = 4,iter=iter,thin=thin)
make.a.model <- function(data,wh,channel='not-nightly',bff=NULL,list0=NULL,iter=4000,thin=1,priorSim=FALSE){
  ## See wbeards work on nightly: https://metrics.mozilla.com/protected/wbeard/mc/nightly_model.html
  alter <- TRUE
    if(wh=="cmr"){
        M0 <- bf( cmain+1   ~  os+offset(log( usage_cm_crasher_cversion+1/60))  + s(nvc,m=1,by=os)+(1+os|c_version), shape ~ os*log(nvc))+negbinomial()
        if(channel %in% c('beta')){
            M0 <- bf( cmain + 1 ~ offset(log(usage_cm_crasher_cversion + 1/60)) + os + (1+os | c_version) + os*log(nvc) ,
                     shape ~ log(nvc)*os)+negbinomial()
        }
        if(channel %in% c("nightly")){
            M0 <- bf( cmain + 1  ~ offset(log(usage_cm_crasher_cversion + 1/60)) + os + (1+os | c_version) +  log(nvc)*os,
                     shape ~ os)+negbinomial()
        }
        if(!is.null(bff)) M0 <- bff
    }
    if(wh=='ccr'){
        M0 <- bf( ccontent+1  ~  os+offset(log( usage_cc_crasher_cversion+1/60))  + s(nvc,m=1,by=os) + (1+os|c_version),
                 shape ~  os*log(nvc)) +negbinomial() # os+s(nvc,1)
        if(channel %in% c('beta')){
            M0 <- bf( ccontent + 1 ~ os + offset(log(usage_cc_crasher_cversion + 1/60)) +  s(nvc, m = 1, by = os) + (1 + os | c_version),
                     shape ~ os*nvc) + negbinomial()  #log(dau_cversion + 1))
        }
        if(channel %in% c("nightly")){
            M0 <- bf( ccontent + 1 ~ os + offset(log(usage_cc_crasher_cversion + 1/60)) +  s(nvc, m = 1, by = os) + (1 + os | c_version),
                     shape ~ os)+negbinomial()
        }
        if(!is.null(bff)) M0 <- bff
    }
    if(wh=='cmi'){
        M0<- bf( log(1+dau_cm_crasher_cversion)   ~   os+ offset(log( dau_cversion)) + s(nvc,m=1,by=os) + (1+os|c_version), sigma ~ os*nvc) #+s(nvc,m=1))
        if(channel %in% c('beta')){
            M0 <- bf(log(1 + dau_cm_crasher_cversion) ~ os + offset(log(dau_cversion)) + s(nvc, m = 1,by=os) + (1 + os | c_version) ,sigma ~ os*nvc)
        }
        if(channel %in% c('nightly')){
            M0 <- bf(log(1 + dau_cm_crasher_cversion) ~ os + offset(log(dau_cversion)) + os*log(1+nvc) + (1 + os | c_version) ,sigma ~ os)
        }
        if(!is.null(bff)) M0 <- bff
    }
    if(wh=='cci'){
        M0<- bf( log(1+dau_cc_crasher_cversion)   ~   os+ offset(log( dau_cversion))  + s(nvc,m=1,by=os) + (1+os|c_version), sigma ~ os*nvc) #+s(nvc,m=1))
        if(channel %in% c('beta')){
            M0 <- bf( log(1 + dau_cc_crasher_cversion)  ~ os + offset(log(dau_cversion)) + s(nvc, m = 1,by=os) + (1 + os | c_version),sigma ~ os*nvc) 
        }
        if(channel %in% c("nightly")){
            M0 <- bf( log(1 + dau_cc_crasher_cversion) ~ os + offset(log(dau_cversion)) + s(nvc, m = 1,by=os) + (1 + os | c_version),sigma ~ os)
        }
        if(!is.null(bff)) M0 <- bff
    }
    if(priorSim){ return(M0) }
    else  ffunc(M0,data,list0=list0,thin=thin,iter=iter)
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
        r <- exp( t(x) - D[, log( usage_cm_crasher_cversion+1/60)])
    }
    if(wh=='ccr'){
        r <- exp(t(x) -  D[, log( usage_cc_crasher_cversion+1/60)])
    }
    if(wh=='cmi'){
        if(fa=='binomial'){
            r <- t(x) #boot::inv.logit(t(x))
        }else{
            r <- exp(t(x) -  D[, log( dau_cversion)])
            r[r>1]  <- 1
        }
    }
    if(wh=='cci'){
        if(fa=='binomial'){
            r <- t(x) #boot::inv.logit(t(x))
        }else {
            r <- exp(t(x) -  D[, log( dau_cversion)])
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








## Experimentation

getCredibleIntervals <- function(D,model,TR=1/100/2){
  CENT <- function(s)  median(s,na.rm=TRUE) #mean(s, trim=TR)
  getLowHi <- function(x,names,trans){
    x <- rbindlist(apply(x,1,function(s){
      data.table(lo90 = quantile(trans(s),0.05), mean = CENT(trans(s)), hi90=quantile(trans(s),0.95))
    }))
    setnames(x,names)
  }
  pm <- getPredictions(model$mr, D)
  pc <- getPredictions(model$cr, D)
  cmrpreds <- getLowHi(pm,trans=function(s) s,names=c("cmr.lo90","cmr.mean","cmr.hi90"))
  ccrpreds <- getLowHi(pc,trans=function(s) s,names=c("ccr.lo90","ccr.mean","ccr.hi90"))
  ctotrpreds <- getLowHi((pm+pc),trans=function(s) s,names=c("ctotr.lo90","ctotr.mean","ctotr.hi90"))
  
  pm <- getPredictions(model$mi, D)
  pc <- getPredictions(model$ci, D)
  cmipreds <- getLowHi(pm,trans=function(s) s,names=c("cmi.lo90","cmi.mean","cmi.hi90"))
  ccipreds <- getLowHi(pc,trans=function(s) s,names=c("cci.lo90","cci.mean","cci.hi90"))
  ctotipreds <- getLowHi((pm+pc),trans=function(s) s,names=c("ctoti.lo90","ctoti.mean","ctoti.hi90"))
  cbind(cmrpreds,ccrpreds,ctotrpreds, cmipreds, ccipreds, ctotipreds)
}



get.evolution <- function(model, dataset){
    endingPoint <- dataset[, tail(.SD[order(date),],1), by=list(c_version,os)]
    pp=adoptionsCompare(DF=TRUE)
    endingPoint <- merge(endingPoint,pp,by=c("os","channel"))
    endingPoint[, nvcActual := nvc]; endingPoint[, nvc := adopt];
    ci.from.model <- getCredibleIntervals(endingPoint, model)
    ## Replace the nvc above with the median value per OS/Channel combination
    cbind(endingPoint[, list(c_version, os,major, minor, date, nvc=nvcActual)], ci.from.model)[order(os,major,minor,date),]
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

compare.two.versions.2 <- function(versiona,versionb,oschoice,
                             dataset,model, doLatest=TRUE,normalizeNVC=TRUE){
    ## oschoice is one of Windows_NT,Linux, Darwin
    ## 'overall' is handled differently
    smz_fits <- function(m,D,oschoice,predsOnly=FALSE){
        Mean <- function(s) exp(mean(log(s+1/100)))-1/100
        predictions <- makePredictions(m,D)
        if(oschoice=="overall"){
            ab <- cbind(D[,list(date=date)],predictions)[, { as.list(apply(.SD[,-1],2,Mean)) },
                                                         by=date]
            predictions <- as.matrix(ab[, -c(1)])
        }
        if(predsOnly) return(predictions)
        label <-  ifelse(is.null(m$scope), attr(m, 'model.type'),m$scope)
        data.table(label=label,
                   lo = apply(predictions,1, quantile, 0.05, na.rm = TRUE),
                   me = apply(predictions,1, quantile, 0.5,  na.rm = TRUE),
                   hi = apply(predictions,1, quantile, 0.90, na.rm = TRUE))
    }
    prep_data <- function(versiona.data,oschoice,normalizeNVC){
        ## when nvc is normalized no point in using all rows, it will be same predictions,so use last
        versiona.data <- versiona.data[, tail(.SD[order(date),],if(normalizeNVC) 1 else Inf),by=os]
        ## to prevent weirdness in 'overall' we dont want only dates were windows exists
        ## which means there might be some dates where the overall score is not based on Linux
        ## because Linux is weeird and someetimes we dont get data for Linux
        droppedOS <- FALSE
        if(oschoice=='overall'){
            n1 <- nrow(versiona.data)
            versiona.data <- versiona.data[date %in% versiona.data[os=='Windows_NT',date],]
            if(nrow(versiona.data)<n1) {
                droppedOS <- TRUE
                }
        }
        
         versiona.fit.data <- versiona.data[, list(
             date,channel,cmain, ccontent, dau=dau_all,dau_c=dau_cversion,
             dau_cm_crasher_cversion,dau_cc_crasher_cversion,
            usage_cm_crasher_cversion,usage_cc_crasher_cversion,usage_all,usage_cversion,
            dau_cversion, nvcOriginal=nvc,
            os, c_version)]
        
        if(normalizeNVC) {
            versiona.fit.data <- merge(versiona.fit.data,adoptionsCompare(DF=TRUE)[,list(channel,os,nvc=adopt)],by=c("channel","os"))
        }else {
            versiona.fit.data[, nvc:=nvcOriginal]
        }

        return(list(droppedOS=droppedOS, versiona.data=versiona.data, versiona.fit.data=versiona.fit.data))
    }
        
    make_cmp_dt <- function(versiona.data, dmodel, versiona,versionb,oschoice, normalizeNVC){
        ## Same functions a make_smz_dt
        da <- prep_data(versiona.data,oschoice, normalizeNVC)
        va.data <- da$versiona.data
        va.fit.data <- da$versiona.fit.data
        da.fits <- smz_fits(dmodel, va.fit.data,oschoice,predsOnly=TRUE)
        db.fits <- smz_fits(dmodel, copy(va.fit.data)[, c_version:=versionb][,],oschoice,predsOnly=TRUE)
        reldifferences <- rbindlist(apply((da.fits - db.fits)/db.fits,1,function(s){
            data.table(lo=quantile(s,0.05,na.rm=TRUE),
                       me = median(s,na.rm=TRUE), 
                       hi = quantile(s,0.95,na.rm=TRUE),
                       prReg = mean(s > 0.20,na.rm=TRUE),
                       prImp = mean(s < -0.20,na.rm=TRUE)
                       )
        }))
        if(oschoice!='overall'){
            cbind(va.fit.data[,list(date,dau,dau_cversion, nvc=nvcOriginal)],reldifferences,
                  data.table(label=ifelse(is.null(dmodel$scope), attr(dmodel, 'model.type'),dmodel$scope),
                             versiona=versiona,versionb=versionb, os=oschoice))
        }else{
            va.fit.data.agg <- va.fit.data[,list(dau=sum(dau),dau_cversion=sum(dau_cversion),nvc=sum(usage_cversion)/sum(usage_all)),by=date]
            cbind(va.fit.data.agg,reldifferences,
                  data.table(label=ifelse(is.null(dmodel$scope), attr(dmodel, 'model.type'),dmodel$scope),
                             versiona=versiona,versionb=versionb, os=oschoice))
        }
    }
    make_smz_dt <- function(versiona.data, dmodel, oschoice, normalizeNVC){
        ## The estimates of crash rate are made at certain NVC values
        ## (median) So that we can compare apples to apples. For example a
        ## cversion might have been adopted by only 10% and another by
        ## 25%, how can we compare the 'true' crash rate taking into
        ## account the adoption effect?  By comparing at same nvc.
        ## Let's get the different type of crash rates
        
        ## when nvc is normalized no point in using all rows, it will be same predictions,so use last
        x <- prep_data(versiona.data, oschoice, normalizeNVC)
        droppedOS <- x$droppedOS
        versiona.data <- x$versiona.data
        versiona.fit.data <- x$versiona.fit.data
        if(droppedOS){
            loginfo(glue("In computation of {oschoice} for {ch},version:{v}  an OS might have been dropped during mean calcs",ch=versiona.data$channel[1]
                         ,v = versiona.data$c_version[1]
                         ))
        }
        if(normalizeNVC) {
            versiona.fit.data <- merge(versiona.fit.data,adoptionsCompare(DF=TRUE)[,list(channel,os,nvc=adopt)],by=c("channel","os"))[, nvc:=nvc.y]
        }else {
            versiona.fit.data[, nvc:=nvcOriginal]
        }
        res <- if(oschoice!="overall"){
                   do.call(cbind, list(versiona.fit.data[,list(date,nvc=nvcOriginal, dau, dau_c)],  smz_fits(dmodel, versiona.fit.data,oschoice )))
               }else{
                   versiona.fit.data.agg <- versiona.fit.data[,list(nvc=sum(usage_cversion)/sum(usage_all),dau=sum(dau),dau_c=sum(dau_c)),by=date]
                   fits <- smz_fits(dmodel, versiona.fit.data,oschoice )
                   cbind(versiona.fit.data.agg,fits)
               }
        res[, droppedOS:=droppedOS]
        res
    }
                                 
                                 
                                 
    make_usage <- function(da, va, osc){
        if(osc!="overall"){
            tail(da[c_version==va & os==osc, ][order(date),][,list(nvc=nvc, call=call,dau_cversion=dau_cversion)],1)
        }else{
            da[c_version==va,][date==max(date),][,list(nvc = sum(usage_cversion)/sum(usage_all),call=sum(call),dau_cversion=sum(dau_cversion))]
        }
    }
                                 
        
    r <- list()
    r$usgTable <- make_usage(dataset, versiona,oschoice)
    r$versiona <- versiona
    r$versionb <- versionb
    r$daysSinceRelease <- dataset[c_version==versiona & os==oschoice, .N]

    ## ##########################################################
    ## Summaries
    ## ##########################################################

    ## Version A summary
    versiona.data <-if(oschoice=='overall') {
                        dataset[c_version==versiona , ][order(date), ]
                    }else{
                        dataset[c_version==versiona & os==oschoice, ][order(date), ]
                    }
    versiona.smzs <- do.call(rbind, list(
                                        make_smz_dt(versiona.data, model$mr, oschoice, normalizeNVC),
                                        make_smz_dt(versiona.data, model$cr, oschoice, normalizeNVC),
                                        make_smz_dt(versiona.data, list(scope='rate',cr=model$cr,mr=model$mr), oschoice, normalizeNVC),
                                        make_smz_dt(versiona.data, model$mi, oschoice, normalizeNVC),
                                        make_smz_dt(versiona.data, model$ci, oschoice, normalizeNVC),
                                        make_smz_dt(versiona.data, list(scope='incidence',ci=model$ci,mi=model$mi), oschoice, normalizeNVC)))

    ## Version B Summary
    versionb.data <-if(oschoice=='overall') {
                        dataset[c_version==versionb , ][order(date), ]
                    }else{
                        dataset[c_version==versionb & os==oschoice, ][order(date), ]
                    }
    versionb.smzs <- do.call(rbind, list(
                                        make_smz_dt(versionb.data, model$mr, oschoice, normalizeNVC),
                                        make_smz_dt(versionb.data, model$cr, oschoice, normalizeNVC),
                                        make_smz_dt(versionb.data, list(scope='rate',cr=model$cr,mr=model$mr), oschoice, normalizeNVC),
                                        make_smz_dt(versionb.data, model$mi, oschoice, normalizeNVC),
                                        make_smz_dt(versionb.data, model$ci, oschoice, normalizeNVC),
                                        make_smz_dt(versionb.data, list(scope='incidence',ci=model$ci,mi=model$mi), oschoice, normalizeNVC)))

    ## Combine Version A and Version B summaries
    r$summary  <- rbind(versiona.smzs[, "versions":='a'], versionb.smzs[, "versions":='b'])

    ## ##########################################################
    ## Relative Comparisons
    ## Produce key results for dates of comparison
    ## lo,me,hi of Relative Difference
    ## Prob of Regression and Prob of Improvement
    ## We assume Version B has the same adoption as Version A
    ## and compare them 
    ## ##########################################################
    ab.cmp <- do.call(rbind, list(
                                 make_cmp_dt(versiona.data, model$mr,versiona,versionb, oschoice, normalizeNVC),
                                 make_cmp_dt(versiona.data, model$cr, versiona,versionb,oschoice, normalizeNVC),
                                 make_cmp_dt(versiona.data, list(scope='rate',cr=model$cr,mr=model$mr), versiona,versionb,oschoice, normalizeNVC),
                                 make_cmp_dt(versiona.data, model$mi, versiona,versionb,oschoice, normalizeNVC),
                                 make_cmp_dt(versiona.data, model$ci,versiona,versionb, oschoice, normalizeNVC),
                                 make_cmp_dt(versiona.data, list(scope='incidence',ci=model$ci,mi=model$mi), versiona,versionb,oschoice, normalizeNVC)))
    r$comparison=ab.cmp
    r
    
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



####################################################################################################
## Save the comparisons for 'current' version vs 'previous' version so that the default page loads
## fast. Note the user can easily change the current and previous but this will obviosuly result
## in some running time. We assume the user comes to see 'current' vs 'previous'
####################################################################################################

## A Sample Dashboard Output For Beta Looks like
whatColor <- function(r,i){
  if(r > i) type='reg'  else type='imp'
  if(type=='reg'){
    if(r > 0.75) 'red'
    else if(r %between% c(0.6,0.75)) 'darkorange'
    else 'grey'
  }else{
    if(i > 0.75) 'limegreen'
    else 'grey'
  }
}




makeUsageSummary <- function(b){
  current.version <- b$Windows_NT$versiona 
  days.since.release <- b$Windows_NT$daysSinceRelease
  usage.summary <- rbindlist(Map(function(oss,d){
    dd  <- d$usgTable
    data.table(OS=oss,
               Adoption = dd[,percent(nvc,digits=1)],
               "No. of Crashers" = comma(dd[, call],digits=0)
              ,"DAU on Current" = comma(dd[, dau_cversion],digits=0)
               )
                                        #      DAU = comma(dd[label=='cmr' & versions=='a' & date==max(date), dau],digits=0))
  },names(b), b))
  list(cu=current.version, days=days.since.release, usage=usage.summary)
}


makeUsageTable <- function(usage.summary,dest='moco'){
    if(dest=='public'){
        x <- usage.summary[, list(OS,Adoption,"No. of Crashers"= get("No. of Crashers"))]
        usage.ft = formattable(x,align=c('r','r','r'))
    }else{
        usage.ft = formattable(usage.summary,align=c('r','r','r','r'))
    }
    usage.ft
}

## Color Coding:
## if prob relative change > 70 % --> red
## if prob relative change 50-70 % orange
## if prob relative change Improvemtn > 70% --> green,
## else no color

makeSummaryTable <- function(b){
  beta.rate.summary <- rbindlist(Map(function(oss,d){
    summ <- d$summary[versions=='a' & date==max(date),]
    compa <- d$comparison[ date==max(date),]
    getEntries <- function(summ,compa, lab){
      a <- summ[ label==lab,me]
      a.relchange <- compa[label==lab, me]
      a.color <- compa[label==lab, whatColor(prReg,prImp)]
      list(me=a,rel=a.relchange,col=a.color)
    }
    cmr <- getEntries(summ,compa, 'cmr')
    ccr <- getEntries(summ,compa, 'ccr')
    cr <-  getEntries(summ,compa, 'rate')
    cmi <- getEntries(summ,compa, 'cmi')
    cci <- getEntries(summ,compa, 'cci')
    ci <-  getEntries(summ,compa, 'incidence')

   rbind( data.table(OS=oss,
                     type='rate',
                     Score = glue("{a} ({b})",a=comma(cr$me,digits=2),b=percent( cr$rel, digits=1)),
                     sccolor = cr$col,
                     Browser=glue("{a} ({b})",a=comma(cmr$me,digits=2),b=percent( cmr$rel, digits=1)),
                     mcrcolor=cmr$col,
                     Content = glue("{a} ({b})",a=comma(ccr$me,digits=2),b=percent(ccr$rel,digits=1)),
                     ccrcolor=ccr$col),
         data.table(OS=oss,
                    type='incidence',
                    Score = glue("{a} ({b})",a=percent(ci$me,digits=2),b=percent( ci$rel, digits=1)),
                    sccolor = ci$col,
                    Browser=glue("{a} ({b})",a=percent(cmi$me,digits=2),b=percent( cmi$rel, digits=1)),
                    mcrcolor=cmi$col,
                    Content = glue("{a} ({b})",a=percent(cci$me,digits=2),b=percent(cci$rel,digits=1)),
                    ccrcolor=cci$col)
         )
  },names(b), b))
  list(rate = formattable(beta.rate.summary[type=='rate',],
            list(
              Score = formatter("span", style = ~ style(color = sccolor, font.weight=ifelse(grepl("grey",sccolor), "normal","bold"))),
              Browser = formatter("span", style = ~ style(color = mcrcolor, font.weight=ifelse(grepl("grey",mcrcolor), "normal","bold"))),
              Content = formatter("span", style = ~ style(color = ccrcolor, font.weight=ifelse(grepl("grey",ccrcolor), "normal","bold"))),
              sccolor=FALSE,ccrcolor=FALSE, mcrcolor=FALSE,type=FALSE),
            align=c("c","r","r","r")),
       incidence = formattable(beta.rate.summary[type=='incidence',],
            list(
              Score = formatter("span", style = ~ style(color = sccolor, font.weight=ifelse(grepl("grey",sccolor), "normal","bold"))),
              Browser = formatter("span", style = ~ style(color = mcrcolor, font.weight=ifelse(grepl("grey",mcrcolor), "normal","bold"))),
              Content = formatter("span", style = ~ style(color = ccrcolor, font.weight=ifelse(grepl("grey",ccrcolor), "normal","bold"))),
              sccolor=FALSE,ccrcolor=FALSE, mcrcolor=FALSE,type=FALSE),
            align=c("r","r","r","r")))
}


################################################################################
## Plots
################################################################################



plotEvolution <- function(f, title, xtip,width=NULL,height=NULL,shiny=NULL,showlegend=TRUE,pointSize=65
                         ,cversionType='ordinal'){
    if(cversionType=='temporal')
        f[, c_version := as.character(as.Date(c_version, '%Y%m%d'))]
    ## see https://github.com/vegawidget/vegawidget/blob/master/inst/shiny-demo/data-set-gapminder/server.R
    ## for tooltip example
    u3 <- list(
        `$schema` = vega_schema(),
        data = if(is.null(shiny)) list(values =f) else list(name=shiny),
        title = list(text=glue(title), anchor='start',fontWeight = 'normal'),
        width = if(is.null(width)) 1200 else width,
        height = if(is.null(height)) 300 else height,
        autosize = list(type='fit',contains='content'),
        layer = list(
            list(
                mark = list(type="point",filled=TRUE),
                selection = list(
                    grid = list(type= "interval", bind="scales")
                ),
                transform = list(list(calculate="format(datum.x,'.2f')+' ('+format(datum.lo,'.2f')+','+format(datum.hi,'.2f')+')'", as=xtip),
                                 list(calculate='format(datum.nvc*100,".2f")+"%"',as='Adoption')),
                encoding = list(
                    size = list(value=pointSize),
                    x = list(field="c_version", type=cversionType, axis=list(title="",labelOverlap='parity'), sort=list(field="ordering",op='count')),
                    y = list(field="x",type="quantitative",axis=list(title="",titleFontSize=11)),
                    color = list(field = "major", type = "nominal",scale=list(scheme="set1")),
                    tooltip=list(list(field = "Date",type="ordinal"),
                                 list(field='c_version', type=cversionType,title='Version'),
                                 list(field = "Adoption",type="ordinal"),
                                 list(field = xtip,type="ordinal")
                                 )
                )),
            list(
                mark = list(type="rule"),
                encoding = list(
                    size = list(value=1),
                    x = list(field="c_version", type=cversionType,sort=if(cversionType=='ordinal') list(field="ordering",op='count') else NULL ),
                    y = list(field="lo",type="quantitative",  grid=FALSE),
                    y2 = list(field="hi",type="quantitative",  grid=FALSE),
                    color = list(field = "major", type = "nominal",scale=list(scheme="set1")),
                    tooltip =  NULL
                ))
        )
    )
    u3
}

Plural <- function(n,w){
    n <- as.numeric(n)
    if(n > 1) glue("{n} {w}s") else glue("{n} {w}")
}

makeActiveMenuBar <- function(channel, generatedText){
    require("glue")
    isreleaseActive <- isbetaActive <- isnightlyActive  <- isfaqActive <- ""
    assign(glue("is{channel}Active"),"class='active'")
    knitr::asis_output(htmltools::htmlPreserve(
glue('
<nav class="navbar navbar-inverse navbar-fixed-top " role="navigation">
  <div class="container">
    <div class="navbar-header">
      <a class="navbar-brand nohover">MissionControl: Proof of Concept</a>
    </div>
    <ul class="nav navbar-nav">
      <li {isreleaseActive}><a href="release.html">Release</a></li>
      <li {isbetaActive}><a href="beta.html">Beta</a></li>
      <li {isnightlyActive}><a href="nightly.html">Nightly</a></li>
      <li {isfaqActive}><a href="faq.html">About</a></li>
    </ul>
    <ul class="nav navbar-nav navbar-right">
      <li><a class="nohover">{generatedText}</a></li>
    </ul>
  </div>
</nav>
',generatedText=generatedText)))
}

makeEvolutionFigure <- function(evo,oss,text,xtip,whi,width=600,height=300,showAction=FALSE,...){
    xm <- glue("{whi}.mean")
    lo = glue("{whi}.lo90")
    hi = glue("{whi}.hi90")
    mul <- if(whi %in% c("cmi","cci")) 100 else 1
    f <- evo[os==oss,list(Date=date,major,minor,c_version,nvc, x=get(xm)*mul, lo=get(lo)*mul,hi=get(hi)*mul
                          )][order(major,minor),]
    l=list(...)
    f <- f[, "ordering" := 1:nrow(f)]
    mdl <- plotEvolution(f,text,xtip,width=width,height=height,...)
    vegawidget(as_vegaspec(mdl),embed=vega_embed(actions=showAction))
}



plotRolloutFigure <- function(cmp, oss,title,whi,xtip,prc=0.75,height=200,width=300){
    aa <- cmp[[oss]]
    ac = aa$summary
    mul <- if(whi %in% c("cmi","cci")) 100 else 1
    v1 <- ac[label==whi, list(versions,lo=lo*mul,me=me*mul,hi=hi*mul,nvc,date)]
    v1 <- v1[, versions := factor(versions, label=c(aa$versiona,aa$versionb))]
    v2 <- aa$comp[label==whi,][, "pr" := pmax(prReg,prImp)]
    v2 <-v2[, "wha" := unlist(Map(function(x,y){
        if(x > y){
            "Regression"
        }else "Improvement"
    },prReg,prImp))]
    v2 <- v2[pr > prc,list(date, versions=aa$versiona, pr, r=me,wha)]
    v3 <- merge(v1,v2,by=c("date",'versions'),all.x=TRUE)
    v3$versions <- as.character(v3$versions)
    v3 <- v3[order(versions,nvc),nvc := 100*nvc]
    u3 <- list(
        `$schema` = vega_schema(),
        title = list(text=glue(title), anchor='start',fontWeight = 'normal'),
        data = list(values=v3),
        width=width,height=height,
        autosize = list(type='fit',contains='content'),
        layer = list(
            list(
                mark = list(type="point",strokeDash=list(1,1)),
                transform = list(
                    list(filter= 'datum.pr>0 & datum.wha=="Regression"')
                ),
                encoding = list(
                    size = list(value=300),
                    shape=list(value='triangle-up'),
                    x = list(field="nvc",type='quantitative'),
                    y = list(field="me",type="quantitative"),
                    color=list(value='black'),
                    tooltip =  NULL
                )),
            list(
                mark = list(type="point",strokeDash=list(1,1)),
                transform = list(
                    list(filter= 'datum.pr>0 & datum.wha=="Improvement"')
                ),
                encoding = list(
                    size = list(value=300),
                    shape=list(value='triangle-down'),
                    x = list(field="nvc",type='quantitative',axis=list(title="Adoption")),
                    y = list(field="me",type="quantitative"),
                    color=list(value='black'),
                    tooltip =  NULL
                )),
            list(
                selection = list(
                    grid = list(type= "interval", bind="scales","encodings"=list("y"))
                ),
                mark = list(type="line",point=TRUE),
                transform = list(list(calculate="format(datum.me,'.2f')+' ('+format(datum.lo,'.2f')+','+format(datum.hi,'.2f')+')'", as=xtip),
                                 list(calculate='format(datum.nvc,".2f")+"%"',as='Adoption'),
                                 list(calculate='if(isNaN(datum.r),"-",format(datum.r*100,".1f")+"%")',as='RelDiff')
                                 ),
                encoding = list(
                    x = list(field="nvc",type='quantitative'),
                    y = list(field="me",type="quantitative"),
                    color = list(field = "versions", type = "nominal",scale=list(scheme="set1")),
                    tooltip=list(list(field = "date",type="ordinal",title="Date"),
                                 list(field = "Adoption",type="ordinal"),
                                 list(field = "versions",type="ordinal",title="Version"),
                                 list(field = xtip,type="ordinal"),
                                 list(field = "RelDiff",type="ordinal")
                                 )
            ))
        )
    )
    vegawidget(as_vegaspec(u3),embed=vega_embed(actions=FALSE))
}


#
