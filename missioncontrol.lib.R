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

if(!exists("missioncontrol.lib.R")){
    ## executed only once
    basicConfig()
    plan(multicore)
    missioncontrol.lib.R <- TRUE
}


ffunc <- function(M,D,list0=NULL,iter=4000,thin=1)  brm(M,data=D, chains = 4,
                                       control = if(is.null(list0))
                                                     list(adapt_delta = 0.999, max_treedepth=13)
                                                 else list0
                                     , cores = 4,iter=iter,thin=thin)
make.a.model <- function(data,wh,channel='not-nightly',bff=NULL,list0=NULL,iter=4000,thin=1){
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
            M0 <- bf(log(1 + dau_cm_crasher_cversion) ~ os + offset(log(dau_cversion)) + s(nvc, m = 1,by=os) + (1 + os | c_version) ,sigma ~ os)
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
  ffunc(M0,data,list0=list0,thin=thin,iter=iter)
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
  attr(M,"model.type") <- x
  M
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
  ci.from.model <- getCredibleIntervals(endingPoint, model)
  cbind(endingPoint[, list(c_version, os,major, minor, date, nvc)], ci.from.model)[order(os,major,minor,date),]
}


posteriorsTransForModel <- function(pc,nr){
    ## the fact that prior version _might_ not have as many rows as current version
    ## means nr wont be same for previous and current and this throws an error
    ## basically dont do doLatest=FALSE for beta and nightly
  ## Get Posterior Distribution of Relative Difference
    versiona.post <- pc[1:nr,,drop=FALSE] 
    versionb.post <- pc[(nr+1):(nr+nr),,drop=FALSE]
    versionb.orig <- pc[(nr+nr+1):(nr+nr+nr),,drop=FALSE]
    
    reldifferences <- rbindlist(apply((versiona.post - versionb.post)/versionb.post,1,function(s){
        data.table(lo=quantile(s,0.05,na.rm=TRUE),
                   me = median(s,na.rm=TRUE), 
                   hi = quantile(s,0.95,na.rm=TRUE),
                   prReg = mean(s > 0.20,na.rm=TRUE),
                   prImp = mean(s < -0.20,na.rm=TRUE)
                   )
    }))
    ## Get Prob(VersionA>VersionB)
    prob.a.gt.b <-as.numeric(sapply(1:nr,function(k){
        x1 <- versiona.post[k,]
        x2 <- versionb.post[k,]
        prob <- mean(x1 > x2) # wilcox.test(x1,x2)
        #prob$stat/(length(x1)*length(x2))
    }))
    
    ## Summary means
    means <-rbindlist(lapply(1:nr,function(k){
        x1 <- versiona.post[k,]
        x2 <- versionb.orig[k,]
        data.table(versions=c('a','b'),
                   lo = c(quantile(x1,0.05,na.rm=TRUE),quantile(x2,0.05,na.rm=TRUE)),
                   me = c(quantile(x1,0.5,na.rm=TRUE),quantile(x2,0.5,na.rm=TRUE)),
                   hi = c(quantile(x1,0.95,na.rm=TRUE),quantile(x2,0.95,na.rm=TRUE)))
    }))
    
    reldifferences[, "probagtb" := prob.a.gt.b]
    list(comparisons = reldifferences, summary= means)
}

compare.two.versions <- function(versiona, versionb,oschoice, dataset,model,doLatest=TRUE){
  ## compare versionb to versiona(base)
  ## using model, labelled as crashtype
  ## Take data corresponding to versiona
  ## Take data correspnding to versionb and set its nvc to the value found in versiona
  ## (in other words, how would versionb performed at versiona's adoption levels)
  ## Fit these two data sets for every day present for them
  ## and based on posterior estimates of rates and incidences
  ## If we want the adoption figure (release yes, beta no , neightly no (doLatest))
  ## a) Compute posterior distribution of relative difference for every day since release
  ## For all
  ## b) As of latest day, compute posterior distribution of relative difference
  ## c) As of latest day, compute Prob(VersionA > Versionb) for differet incidences
    versiona.data <- if(oschoice=="overall"){
                         dataset[c_version %in% versiona ,]
                     }else{
                         dataset[c_version %in% versiona & os %in% oschoice,]
                     }
    versiona.data[, "since" := as.numeric(date - min(date)), by=list(os,c_version),]
    if(any(nrow(versiona.data)==0)) stop(glue("Version A: {versiona} has no data"))
    versionb.data <- if(oschoice=="overall") {
                         dataset[c_version %in% versionb ,][order(date),]
                     }else{
                         dataset[c_version %in% versionb & os %in% oschoice,][order(date),]
                     }
    if(oschoice=="overall"){
        af <- versiona.data[, list(n=.N),by=os]
        versionb.data <- versionb.data[, head(.SD[order(date),],af[af$os==.BY$os,n]),by=os]
    }else{
        versionb.data <- head(versionb.data,nrow(versiona.data))
    }
    versionb.data[, "since" := as.numeric(date - min(date)), by=list(os,c_version),]
    if(doLatest || oschoice=="overall"){
        versiona.data <- versiona.data[,
                                       tail(.SD[order(date),],1), by=list(os,c_version)][order(os,c_version,date),]
        versionb.data <- versionb.data[,
                                       tail(.SD[order(date),],1), by=list(os,c_version)][order(os,c_version,date),]
    }
    if(nrow(versiona.data) > nrow(versionb.data)){
        g <- versionb.data[,  {
            AS <- since
            A1 <- versiona.data[os==.BY$os,]
            A1 <- A1[!A1$since %in% AS,]            
            A1$c_version = versionb
            A1$os <- NULL
            A1
        },by=os]
        versionb.data <- versionb.data[, .SD, by=os]
        versionb.data <- rbind(versionb.data,g)[order(os,c_version,since),]
    }
    versionb.data.orig <- copy(versionb.data)
    versionb.data <- versionb.data[order(os,date),];versiona.data <- versiona.data[order(os,date),]
    versionb.data[, nvc :=  head(versiona.data$nvc,nrow(versionb.data))]
    D <- rbind(versiona.data[,clz := "A"], versionb.data[,clz:='B'],versionb.data.orig[, clz := 'BO'])
    if(oschoice=='overall'){
        D <- D[, mw :=  usage_cversion/sum(usage_cversion)  ,by=list(clz,date)]
    }

    callAndEdit <- function(model,D, vaData,vbData,squashOS=FALSE){
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
      Mean <- function(s) exp(mean(log(s+1/100)))-1/100
      if(squashOS==TRUE ){
          osInfo <- D[,  list(os, date,c_version,clz)]
          x <- cbind(osInfo, a)
          ab <- x[,{
              as.list(apply(.SD[,-1],2,Mean))
          },by=list(date,c_version,clz)]
          a <- as.matrix(ab[, -c(1,2,3)])
      }
    b <- posteriorsTransForModel(a,if(squashOS) nrow(vaData[, 1,by=list(date,c_version)]) else nrow(vaData))
    comparisons <- b$comparison; summa <- b$summary
    comparisons[,'label' :=  ifelse(is.null(model$scope),attr(model, 'model.type'),model$scope)]
    summa[,'label' :=  ifelse(is.null(model$scope), attr(model, 'model.type'),model$scope)]
    list(comparisons=comparisons, summary = summa,squashOS=squashOS)
  }

  
  j <- list(callAndEdit(model$mr,D,versiona.data,squashOS=oschoice=="overall"),
            callAndEdit(model$cr,D,versiona.data,squashOS=oschoice=="overall"),
            callAndEdit(list(scope='rate',cr=model$cr,mr=model$mr),D,versiona.data,squashOS=oschoice=="overall"),
            callAndEdit(model$mi,D,versiona.data,squashOS=oschoice=="overall"),
            callAndEdit(model$ci,D,versiona.data,squashOS=oschoice=="overall"),
            callAndEdit(list(scope='incidence',ci=model$ci,mi=model$mi),D,versiona.data,squashOS=oschoice=="overall")
            )
            
  
    j0 <- rbindlist(lapply(j, function(s) s$comparisons))
    j0[, ":="(versiona = paste(as.character(versiona),collapse=","),
              versionb=paste(as.character(versionb),collapse=","),
              os=paste(oschoice,collapse=","))]
    if(doLatest || oschoice=="overall"){
        ## it is frustrating that linux makes things messy
        ## in oschoice ='overall', there should be the same date for all os
        ## given a version, yet Linux shows a different one
        ## setting it to the same as Windows
        fixMuls <- function(s){
            if(length(unique(s$date)) > 1) {
                s[os=='Linux', date := s[os=='Windows_NT',date]]
            }
            return(s)
        }
        versiona.data <- fixMuls(versiona.data)
        versionb.data <- fixMuls(versionb.data)
        versionb.data.orig <- fixMuls(versionb.data.orig)
    }
    
    nvc  <- c(rbind(versiona.data[, list(nvc=sum(nvc)),by=list(date)]$nvc,
                    versionb.data.orig[,list(nvc=sum(nvc)),by=list(date)]$nvc ))
    dates  <- as.Date(c(rbind( versiona.data[, list(date=min(date)), by=date]$date,
                              versionb.data.orig[,list(date=min(date)), by=date]$date)),
                      origin='1970-01-01')
    dau  <- c(rbind(versiona.data[, list(dau_all=sum(dau_all)),by=date]$dau_all,
                    versionb.data.orig[, list(dau_all=sum(dau_all)),by=date]$dau_all))
    dau_c  <- c(rbind(versiona.data[, list(dau_cversion=sum(dau_cversion)),by=date]$dau_cversion,
                    versionb.data.orig[, list(dau_cversion=sum(dau_cversion)),by=date]$dau_cversion))
    j1 <- rbindlist(lapply(j, function(s){
        a <- s$summary
        a[,":="(date= dates, nvc = nvc,dau=dau,dau_c=dau_c)]
        a
    }))
    if(oschoice=="overall"){
        usgTable <- dataset[c_version  %in% versiona,][date==max(date),]
        usgTable <- usgTable[, list(nvc=sum(usage_cversion)/sum(usage_all),call=sum(call), dau_cversion=sum(dau_cversion))]
    }else{
        usgTable <- dataset[c_version %in% versiona &  os %in% oschoice,][order(date),]
        usgTable <- tail(usgTable,1)[, list(nvc,call,dau_cversion) ]
    }
    list(comparison =   cbind(
             versiona.data[, list(dau=sum(dau),dau_cversion=sum(dau_cversion), nvc=sum(usage_cversion)/sum(usage_all)),by=date ],
             j0)[order(date),]
       , summary = j1[order(versions,date),],usgTable = usgTable,
         daysSinceRelease=as.numeric(Sys.Date()-min(versiona.data$date)),
         versiona = versiona,versionb=versionb
         )
}
#
#compare.two.versions('69.0','68.0.2','Darwin', dall.rel2,model,FALSE)

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
