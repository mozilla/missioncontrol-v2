
library(data.table)
library(jsonlite)
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


## Now get some data
esr.data <-  fread("bq query -n 500000000 --format=csv --nouse_legacy_sql 'select *  from analysis.missioncontrol_v2_raw_data_test_bh_mast'")

esr.data <- merge(esr.data,uesr[, list(c_version, rdate)], by="c_version",keep.x=TRUE)
## And this is how  we remove data beyond the 'end' date of a major/minor
esr.data <- esr.data[date<=rdate][, rdate:=NULL]


## Fix major and minor
esr.data <- esr.data[, ":="(major=NULL, minor=NULL)]
esr.data <- cbind(esr.data,rbindlist(Map(function(s) {
    f <- strsplit(s,".",fixed=TRUE)[[1]]
    data.table(major = f[1], minor=paste(f[-1],collapse="."))
    },esr.data$c_version)))

## Restrict data to some cuttoff date (i dont think we need so much data anyways)
## the cutoff date is a bit arbitrary
esr.data <- esr.data[major>=68,]

################################################################################
##
## Lets build models!!
##
################################################################################


## Is there a relationship with adoption?
## cmr

y <- esr.data
pdf("Rplots.pdf",width=12)
ggplot(y[os=='Windows_NT',], aes(nvc, cmr2,color=c_version))+geom_line(alpha=0.5)
ggplot(y[os=='Linux',], aes(nvc, cmr2,color=c_version))+geom_line(alpha=0.5)
ggplot(y[os=='Darwin',], aes(nvc, cmr2,color=c_version))+geom_line(alpha=0.5)
y2 <- y[,list(cmr=median(cmr2),s=sd(cmr)), by=list(os, nvc=cut(nvc*100, 100*unique(quantile(nvc, seq(0,1,length=10))),
                                                            include.lowest=TRUE,ordered=TRUE))]
y2 <- y2[order(os,nvc),]
ggplot(y2[os=='Windows_NT',], aes(nvc, cmr))+geom_bar(stat="identity")+theme(axis.text.x = element_text(angle = 90))
ggplot(y2[os=='Linux',], aes(nvc, cmr))+geom_bar(stat="identity")+theme(axis.text.x = element_text(angle = 90))
ggplot(y2[os=='Darwin',], aes(nvc, cmr))+geom_bar(stat="identity")+theme(axis.text.x = element_text(angle = 90))
ggplot(y2[os=='Windows_NT',], aes(nvc, s))+geom_bar(stat="identity")+theme(axis.text.x = element_text(angle = 90))
ggplot(y2[os=='Linux',], aes(nvc, s))+geom_bar(stat="identity")+theme(axis.text.x = element_text(angle = 90))
ggplot(y2[os=='Darwin',], aes(nvc, s))+geom_bar(stat="identity")+theme(axis.text.x = element_text(angle = 90))
dev.off()


library(brms)
y <- esr.data
y[ ,os:=factor(os)]
y[, cmr:=cmain/(usage_cm_crasher_cversion+1/60)]
y[, ccr:=ccontent/(usage_cc_crasher_cversion+1/60)]


cmr1=cr.cm.nightly;ccr1=cr.cc.nightly

cor <- function(a,b) stats::cor(a,b,method='pearson')

cmr1 <- make.a.model(data=y,wh='cmr', channel='esr',
                   bff = bf(  cmain+1   ~  os+offset(log( usage_cm_crasher_cversion+1/60)) +  s(nvc, m = 1) + (1+os|c_version)
                           ,shape~os )+negbinomial()
                   )

cmr2 <- make.a.model(data=y,wh='cmr', channel='esr',
                   bff = bf(  log(cmr+1)  ~  os +  s(nvc, m = 1) + (1+os|c_version)
                           ,sigma~os  )
                   )

y[, x:=(fitted(cmr1)[,'Estimate'])/ (usage_cm_crasher_cversion+1/60)][,list(m1=mean(cmr),m2=mean(x))   ,by=list(os,c_version)][, list(mean((m1-m2)),100* mean(abs(m1-m2)/(1+m1)),sqrt(mean((m1-m2)^2)),cor(m1,m2))]
y[, x:=-1+exp(fitted(cmr2)[,'Estimate'])][,list(m1=mean(cmr),m2=mean(x))   ,by=list(os,c_version)][, list(mean(abs(m1-m2)),100* mean((m1-m2)/(1+m1)), sqrt(mean((m1-m2)^2)),cor(m1,m2))]



ccr2 <- make.a.model(data=y,wh='ccr', channel='esr',
                   bff = bf(  log(ccr+1)  ~  os +  s(nvc, m = 1) + (1+os|c_version)
                           ,sigma~os  )
                   )

y[, x:=(fitted(ccr1)[,'Estimate'])/ (usage_cc_crasher_cversion+1/60)][,list(m1=mean(ccr),m2=mean(x))   ,by=list(os,c_version)][, list(mean((m1-m2)),100* mean(abs(m1-m2)/(1+m1)),sqrt(mean((m1-m2)^2)),cor(m1,m2))]
y[, x:=-1+exp(fitted(ccr2)[,'Estimate'])][,list(m1=mean(ccr),m2=mean(x))   ,by=list(os,c_version)][, list(mean(abs(m1-m2)),100* mean((m1-m2)/(1+m1)), sqrt(mean((m1-m2)^2)),cor(m1,m2))]



###############################
cmi2 <-  make.a.model(data=y,wh='cmi', channel='esr',
                   bff = bf( log(1+dau_cm_crasher_cversion)   ~   os+ offset(log( dau_cversion)) + s(nvc,m=1) + (1+os|c_version), sigma ~ os)
                   )
cci2 <-  make.a.model(data=y,wh='cci', channel='esr',
                   bff = bf( log(1+dau_cc_crasher_cversion)   ~   os+ offset(log( dau_cversion)) + s(nvc,m=1) + (1+os|c_version), sigma ~ os)
                   )
y[, x:=(-1+exp(fitted(cmi2)[,'Estimate']))/ (dau_cversion)][,list(m1=mean(cmi),m2=mean(x))   ,by=list(os,c_version)][, list(mean(abs(m1-m2)),100* mean(abs(m1-m2)/(1+m1)), sqrt(mean((m1-m2)^2)),cor(m1,m2))]
y[, x:=(-1+exp(fitted(cci2)[,'Estimate']))/ (dau_cversion)][,list(m1=mean(cci),m2=mean(x))   ,by=list(os,c_version)][, list(mean(abs(m1-m2)),100* mean(abs(m1-m2)/(1+m1)), sqrt(mean((m1-m2)^2)),cor(m1,m2))]

y[, cmi.logit:=boot::logit(cmi)]
y[, cci.logit:=boot::logit(cci)]


cmi3 <-  make.a.model(data=y,wh='cmi', channel='esr',
                   bff = bf( cmi.logit   ~   os+ s(nvc,m=1) + (1+os|c_version), sigma ~ os)
                   )
cci3 <-  make.a.model(data=y,wh='cci', channel='esr',
                   bff = bf( cci.logit   ~   os+ s(nvc,m=1) + (1+os|c_version), sigma ~ os)
                   )

y[, x:=(boot::inv.logit(fitted(cmi3)[,'Estimate']))][,list(m1=mean(cmi),m2=mean(x))   ,by=list(os,c_version)][, list(mean(abs(m1-m2)),100* mean(abs(m1-m2)/(1+m1)), sqrt(mean((m1-m2)^2)),cor(m1,m2))]
y[, x:=(boot::inv.logit(fitted(cci3)[,'Estimate']))][,list(m1=mean(cci),m2=mean(x))   ,by=list(os,c_version)][, list(mean(abs(m1-m2)),100* mean(abs(m1-m2)/(1+m1)), sqrt(mean((m1-m2)^2)),cor(m1,m2))]


rel.list <- list(cmr = label(cmr1,'cmr'), ccr = label(ccr2,'ccr'), cmi = label(cmi2,'cmi'), cci = label(cci2,'cci'))
ll.rel <- make_posteriors(y, CHAN='esr', model.date = '2020-02-01',model.list=rel.list,last.model.date= '2019-01-01')

f <- ll.rel[os=='Windows_NT',list(m=mean(posterior), l = quantile(posterior,0.05), u = quantile(posterior,1-0.05)),
       by=list(modelname,c_version,major,minor,os)][order(modelname,os,major,minor),][, del:=(u-l)/m][,]
f[modelname=='cmi',]
y[os=='Windows_NT', list(m = cmi[date==max(date)]),by=list(os,major,c_version,minor)][order(major,minor),]



f <- ll.rel[os=='Windows_NT',list(m=mean(posterior), l = quantile(posterior,0.05), u = quantile(posterior,1-0.05)),
       by=list(modelname,c_version,major,minor,os)][order(modelname,os,major,minor),][, del:=(u-l)/m][,]
f[modelname=='cmi',]
y[os=='Windows_NT', list(m = cmi[date==max(date)]),by=list(os,major,c_version,minor)][order(major,minor),]
