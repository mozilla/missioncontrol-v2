
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
## Did this for building models
## But i think the API would be same as others i.e. get N last versions data
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


cr.cc.nightly2 <- make.a.model(dall.nightly2,'ccr',channel='nightly',debug=debug.mode,iter=4000)

pp_check(cr.cm.nightly)
pp_check(cr.cm.nightly,type='scatter_avg',nsam=100)
pp_check(cr.cm.nightly, type='stat_grouped',group='os')
pp_check(cr.cc.nightly2)
pp_check(cr.cc.nightly2,type='scatter_avg',nsam=100)
pp_check(cr.cc.nightly2, type='stat_grouped',group='os')
pp_check(ci.cm.nightly)
pp_check(ci.cm.nightly,type='scatter_avg',nsam=100)
pp_check(ci.cm.nightly, type='stat_grouped',group='os')
pp_check(ci.cc.nightly)
pp_check(ci.cc.nightly,type='scatter_avg',nsam=100)
pp_check(ci.cc.nightly, type='stat_grouped',group='os')
dev.off()

D <- dall.rel2[os=='Windows_NT',]
pp_check(cr.cm.rel,newdata=D)
pp_check(cr.cc.rel,newdata=)
pp_check(ci.cm.rel,newdata=)
pp_check(ci.cc.rel,newdata=)
dev.off()



