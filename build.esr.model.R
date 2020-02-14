
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
y[, ":="(cmi.logit=boot::logit(cmi), cci.logit=boot::logit(cci))]
y2 <- y[ major<=68 & minor <="4.1",]
cmr <- make.a.model(y,'ccr',channel='esr',debug=debug.mode,iter=4000)

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

y2 <- y[ major<=68 & minor <="4.1",][order(os,c_version,date),]
cmr <- make.a.model(y2,'ccr',channel='esr')

y3=copy(y2)
y3[os=="Windows_NT" & date==max(date), ccr:=4*ccr]
cmr2 <- make.a.model(y3,'ccr',channel='esr')

rel.list2 <- list(cmr = label(cmr,'cmr'), ccr = label(cmr,'ccr'), cmi = label(cmr,'cmi'), cci = label(cmr,'cci'))
ll.rel2<- make_posteriors(y2, CHAN='esr', model.date = '2020-20-01',model.list=rel.list2,last.model.date= '2019-01-01')

f1 <- ll.rel2[os=='Windows_NT' & modelname=="ccr",list(m=mean(posterior), l = quantile(posterior,0.05), u = quantile(posterior,1-0.05)),
              by=list(modelname,c_version,major,minor,os)][order(os,major,minor),][,list(c_version, m,l,u)]


rel.list3 <- list(cmr = label(cmr2,'cmr'), ccr = label(cmr2,'ccr'), cmi = label(cmr2,'cmi'), cci = label(cmr2,'cci'))
ll.rel3 <- make_posteriors(y3, CHAN='esr', model.date = '2020-20-01',model.list=rel.list3,last.model.date= '2019-01-01')

f2 <- ll.rel3[os=='Windows_NT' & modelname=="ccr",list(m=mean(posterior), l = quantile(posterior,0.05), u = quantile(posterior,1-0.05)),
              by=list(modelname,c_version,major,minor,os)][order(os,major,minor),][,list(c_version, m,l,u)]
f1
f2

cmr2 <- make.a.model(dall.rel2, 'cmr',bff = bf(log(cmr+1)  ~  0+ os + Intercept+ nvc),channel='release')








## Parameter Passing or Posterior Passing Estimate the model on some
## data (here d1) (and we could have used priors for this too) and
## then take coefficient estimates and use normal family with these as
## mean and pass them to smaller data set If we pass in our own sigma
## (not sigma of paramaters estimated) we can ciontrol how much the
## priors influence the data smaller /tighter sigma will result in
## less influence from the data. Using the estimate 'Esr.Error' might
## be a good way to prevent new data from speaking too much


d1 <- dall.rel2[date<=(max(date)-7),]
d2 <- dall.rel2[date>(max(date)-7),]

cmr2 <- brm(formula=bf(  log(cmr+1)  ~  0+ Intercept + os + nvc),
            data = d1,cores=2,chains=2)

## COmpare model estimates by changging 0.3 to something else
cmr2a <- brm(formula=bf(  log(cmr+1)  ~  0+ Intercept + os + nvc),
            prior=c(
                prior(normal(  0.88,0.03)  , class = b, coef = Intercept),
                prior(normal( -0.19,0.03)  , coef='osLinux',class = "b"),
                prior(normal(  0.11,0.03)  , coef='osWindows_NT',class = "b"),
                prior(normal( -0.52,0.05)  , coef='nvc',class = "b")
            )
            ,data =d2,cores=2,chains=2)

## Lets expand the model to include grouping effect
## One useful way to get a prior for these is to use lme4
## Generally adding priors can reduce model fit time
## and number of divergences
library(lme4)
summary(lmer(log(cmr+1)  ~  0+os + nvc + (1|c_version), data = d1))
cmr2 <- brm(formula=bf(  log(cmr+1)  ~  0+ Intercept + os + nvc + (1+os|c_version)),
            prior=c(
                prior(normal(  0.96,1)  , class = b, coef = Intercept),
                prior(normal( 0.79,1)  , coef='osLinux',class = "b"),
                prior(normal(  1.06,1)  , coef='osWindows_NT',class = "b"),
                prior(normal( -0.427,1)  , coef='nvc',class = "b"),
                prior(normal( 0.51,1)  , coef='Intercept',group='c_version',class = "sd"), ## these came from actually running the model
                prior(normal( 0.26,1)  , coef='osLinux',group='c_version',class = "sd"),
                prior(normal( 0.35,1)  , coef='osWindows_NT',group='c_version',class = "sd"),
                prior(lkj(0.9) ,group='c_version',class = "cor")
            ),
            data = d1,cores=2,chains=2)

cmr2 <- brm(formula=bf(  log(cmr+1)  ~ os + nvc + (1+os|c_version),sigma~os),
            data = d1,cores=2,chains=2)

## LEts try on data set d2

cmr2a <- brm(formula=bf(  log(cmr+1)  ~  0+ Intercept + os + nvc + (1+os|c_version)),
            prior=c(
                prior(normal(  0.96,0.3)  , class = b, coef = Intercept),
                prior(normal( -0.13,0.15)  , coef='osLinux',class = "b"),
                prior(normal(  0.03,0.25)  , coef='osWindows_NT',class = "b"),
                prior(normal( -0.44,0.1)  , coef='nvc',class = "b"),
                prior(normal( 0.56,0.3)  , coef='Intercept',group='c_version',class = "sd"), ## these came from actually running the model
                prior(normal( 0.24,0.3)  , coef='osLinux',group='c_version',class = "sd"),
                prior(normal( 0.37,0.3)  , coef='osWindows_NT',group='c_version',class = "sd"),
                prior(lkj(1.1) ,group='c_version',class = "cor")
            ),
            data = d2,cores=2,chains=2)
## Compare to this
cmr2aa <- brm(formula=bf(  log(cmr+1)  ~  0+ Intercept + os + nvc + (1+os|c_version)),
            data = d2,cores=2,chains=2)


### Now Complicatting More
d1 <- dall.rel2[date<=(max(date)-7),]
cmr2 <- brm(formula=bf(  log(cmr+1)  ~   0+Intercept+os + s(nvc,m=1) + (1+os|c_version),sigma~os),
            prior=c(
                prior(normal( 0.79,1)  , coef='osLinux',class = "b"),
                prior(normal(  1.06,1)  , coef='osWindows_NT',class = "b"),
                prior(normal( 0.51,1)  , coef='Intercept',group='c_version',class = "sd"), ## these came from actually running the model
                prior(normal( 0.26,1)  , coef='osLinux',group='c_version',class = "sd"),
                prior(normal( 0.35,1)  , coef='osWindows_NT',group='c_version',class = "sd"),
                prior(normal( 0.42,0.2)  , class='b',coef='osLinux',dpar = "sigma"),
                prior(normal( -0.76,.2)  , class='b',coef='osWindows_NT',dpar = "sigma"),
                prior(lkj(0.9) ,group='c_version',class = "cor")
            ),
            data = d1,cores=2,chains=2,control=list(adapt_delta = 0.999, max_treedepth=13))


d21 <- d2[date<='2020-01-28',]
cmr2a <- brm(formula=bf(  log(cmr+1)  ~   0+Intercept+os + s(nvc,m=1) + (1+os|c_version),sigma~os),
             prior=c(
                 prior(normal( 0.24,0.1)  , class='sds',coef='s(nvc,m=1)'),
                 prior(normal( 0.52,0.3)  , coef='Intercept',group='c_version',class = "sd"), ## these came from actually running the model
                 prior(normal( 0.23,0.3)  , coef='osLinux',group='c_version',class = "sd"),
                 prior(normal( 0.36,0.25) , coef='osWindows_NT',group='c_version',class = "sd"),
                 prior(normal( 0.95,.35)  , coef='Intercept',class = "b"),
                 prior(normal( -0.10,.3)  , coef='osLinux',class = "b"),
                 prior(normal( 0.03,0.25) , coef='osWindows_NT',class = "b"),
                 prior(normal( 0.53,.18)  , class='b',coef='osLinux',dpar = "sigma"),
                 prior(normal( -0.63,.18) , class='b',coef='osWindows_NT',dpar = "sigma"),
                 prior(lkj(1.1) ,group='c_version',class = "cor")
             ),
            data = d21,cores=2,chains=2,control=list(adapt_delta = 0.999, max_treedepth=13))
## Trythe above without priors. miserable fits
V <- 'sd_c_version__Intercept'
c( mean(posterior_samples(cmr2a,par=V)[,1]),  sd(posterior_samples(cmr2a,par=V)[,1]))

#dall.rel2[, x:=-1+exp(fitted(cmr2)[,'Estimate'])][,list(m1=mean(cmr),m2=mean(x))   ,by=list(os,c_version)][, list(mean(abs(m1-m2)),100* mean((m1-m2)/(1+m1)), sqrt(mean((m1-m2)^2)),cor(m1,m2))]


cmr <- cr.cm.rel
y2 <- dall.rel2
rel.list2 <- list(cmr = label(cmr,'cmr'), ccr = label(cmr,'ccr'), cmi = label(cmr,'cmi'), cci = label(cmr,'cci'))
ll.rel2<- make_posteriors(y2, CHAN='esr', model.date = '2020-20-01',model.list=rel.list2,last.model.date= '2019-01-01')

f1 <- ll.rel2[os=='Windows_NT' & modelname=="cmr",list(m=mean(posterior), l = quantile(posterior,0.05), u = quantile(posterior,1-0.05)),
              by=list(modelname,c_version,major,minor,os)][order(os,major,minor),][,list(c_version, m,l,u)]

f1[c_version %in% c('72.0.1', '72.0.2'),]


cmr <- cmr2a
y2 <- d2
rel.list2 <- list(cmr = label(cmr,'cmr'), ccr = label(cmr,'ccr'), cmi = label(cmr,'cmi'), cci = label(cmr,'cci'))
ll.rel2<- make_posteriors(y2, CHAN='esr', model.date = '2020-20-01',model.list=rel.list2,last.model.date= '2019-01-01')

f1 <- ll.rel2[os=='Windows_NT' & modelname=="cmr",list(m=mean(posterior), l = quantile(posterior,0.05), u = quantile(posterior,1-0.05)),
              by=list(modelname,c_version,major,minor,os)][order(os,major,minor),][,list(c_version, m,l,u)]

f1[c_version %in% c('72.0.1', '72.0.2'),]


dx <- dall.rel2[date<="2020-01-13",]
cmr2 <- brm(formula=bf(  log(cmr+1)  ~   0+Intercept+os + s(nvc,m=1,by=os) + (1+os|c_version),sigma~os),
            prior=c(
                prior(normal( 0.79,1)  , coef='osLinux',class = "b"),
                prior(normal( 1.06,1)  , coef='osWindows_NT',class = "b"),
                prior(normal( 0.51,1)  , coef='Intercept',group='c_version',class = "sd"), ## these came from actually running the model
                prior(normal( 0.26,1)  , coef='osLinux',group='c_version',class = "sd"),
                prior(normal( 0.35,1)  , coef='osWindows_NT',group='c_version',class = "sd"),
                prior(normal( 0.42,0.2)  , class='b',coef='osLinux',dpar = "sigma"),
                prior(normal( -0.76,.2)  , class='b',coef='osWindows_NT',dpar = "sigma"),
                prior(lkj(0.9) ,group='c_version',class = "cor")
            ),
            data = dx,cores=2,chains=2,control=list(adapt_delta = 0.999, max_treedepth=13))



getPriorValues <- function(model=NULL,sf=1.5){
    fillIn <- function(p, m,ee,SF=sf){
        po <- posterior_samples(model,par=p)[,1]
        assign(glue("{m}_m"), mean(po),env=ee)
        assign(glue("{m}_s"), SF*sd(po),env=ee)
    }
   
    if(is.null(model)){
        nvc_m <- 0.25                       ; nvc_s <- 0.1
        Intercept_c_version_sd_m <- 0.52    ; Intercept_c_version_sd_s <- 0.3
        osLinux_c_version_sd_m <- 0.23      ; osLinux_c_version_sd_s <- 0.3;
        osWindows_NT_c_version_sd_m <- 0.36 ; osWindows_NT_c_version_sd_s <- 0.25;
        Intercept_b_m <- 0.95               ; Intercept_b_s <- 0.35;
        osLinux_b_m <- -0.1                 ; osLinux_b_s <- 0.3
        osWindows_NT_b_m <- 0.03            ; osWindows_NT_b_s <- 0.25;
        osLinux_sigma_m <- 0.53             ; osLinux_sigma_s <- 0.18;
        osWindows_NT_sigma_m <- -0.63       ; osWindows_NT_sigma_s <- 0.18
        Intercept_sigma_m <- -2.38          ; Intercept_sigma_s <- 0.3
    }else{
        ee <- environment()
        fillIn("sds_snvc_1","nvc",ee)
        fillIn("sd_c_version__Intercept","Intercept_c_version_sd",ee)
        fillIn("sd_c_version__osLinux","osLinux_c_version_sd",ee)
        fillIn("sd_c_version__osWindows_NT","osWindows_NT_c_version_sd",ee)
        fillIn("b_Intercept","Intercept_b",ee)
        fillIn("b_osLinux","osLinux_b",ee)
        fillIn("b_osWindows_NT","osWindows_NT_b",ee)
        fillIn("b_sigma_osLinux","osLinux_sigma",ee)
        fillIn("b_sigma_osWindows_NT","osWindows_NT_sigma",ee)
        fillIn("b_sigma_Intercept","Intercept_sigma",ee)
    }
    l <- c(
        prior_string(glue("normal({nvc_m},{nvc_s})"),class='sds',coef='s(nvc,m=1)'),
        prior_string(glue("normal({Intercept_c_version_sd_m},{Intercept_c_version_sd_s})"), coef='Intercept',group='c_version',class = "sd"), 
        prior_string(glue("normal( {osLinux_c_version_sd_m}, {osLinux_c_version_sd_s})")  , coef='osLinux',group='c_version',class = "sd"),
        prior_string(glue("normal( {osWindows_NT_c_version_sd_m}, {osWindows_NT_c_version_sd_s})")  , coef='osWindows_NT',group='c_version',class = "sd"),
        prior_string(glue("normal( {Intercept_b_m}, {Intercept_b_s})")  , coef='Intercept',class='b'),
        prior_string(glue("normal( {osLinux_b_m}, {osLinux_b_s})")  , coef='osLinux',class='b'),
        prior_string(glue("normal( {osWindows_NT_b_m}, {osWindows_NT_b_s})")  , coef='osWindows_NT',class='b'),
        prior_string(glue("normal( {Intercept_sigma_m}, {Intercept_sigma_s})")  , class='Intercept',dpar = "sigma"),
        prior_string(glue("normal( {osLinux_sigma_m}, {osLinux_sigma_s})")  , class='b',coef='osLinux',dpar = "sigma"),
        prior_string(glue("normal( {osWindows_NT_sigma_m}, {osWindows_NT_sigma_s})")  , class='b',coef='osWindows_NT',dpar = "sigma"),
        prior(lkj(1.1) ,group='c_version',class = "cor"))
    l
}

        
iter.magic <- function(dall.rel2, nmodels=9,fac=1.5){
    ds <- tail(dall.rel2[, sort(unique(date))] ,nmodels)
    ds1 <- ds[1]
    cc <- list()
    for(i in seq_along(ds)){
        print(i)
        ds1 <- ds[i]
        D <- dall.rel2[date %between% c(ds1-6,ds1),]
        if(i==1) model <- NULL else model <- prior.model
        M <- brm(formula=bf(  log(cmr+1)  ~   0+Intercept+os + s(nvc,m=1) + (1+os|c_version),sigma~os),
                 data = D,cores=2,chains=2,control=list(adapt_delta = 0.999, max_treedepth=13),
                 prior=getPriorValues(model,sf=fac))
        prior.model <- M
        cc[[ length(cc)+1]] <- M
    }
    l0 <- rbindlist(lapply(seq_along(ds),function(i){
        ds1 <- ds[i]
        D <- dall.rel2[date %between% c(ds1-6,ds1),][, nvc:=0.6]
        D[, "f":=exp(predict(cc[[i]],newdata=D )[,'Estimate'])-1]
        D2 <- D[, list(m=mean(cmr),n=max(nvc), f=mean(f)),by=list(os, c_version)]
        D2$k <- max(D$date)
        D2
    }))[order(os ,c_version,k),]
    list(cc=cc,l0=l0)
}


l10 = iter.magic(dall.rel2, nmodels=9,fac=1.0)
l20 = iter.magic(dall.rel2, nmodels=9,fac=2.0)
l30 = iter.magic(dall.rel2, nmodels=9,SF=3.0)























dx <- dall.rel2[date>='2020-02-10',]
cmr2 <- brm(formula=bf(  log(cmr)  ~   0+Intercept+os + s(nvc,m=1,bs='cs',by=os) + (1+os|c_version),sigma~os),
            prior=c(
                prior(normal( 0.08,0.3)  , coef='Intercept',group='c_version',class = "sd"), ## these came from actually running the model
                prior(normal( 0.42,1)  , coef='osLinux',group='c_version',class = "sd"),
                prior(normal( 0.15,0.6)  , coef='osWindows_NT',group='c_version',class = "sd"),
                prior(normal( -0.18,0.3), coef="Intercept"),
                prior(normal( -0.51,0.9)  , coef='osLinux',class = "b"),
                prior(normal( 0.09,0.45)  , coef='osWindows_NT',class = "b"),
                prior(normal( 0.65,0.4)  , class='b',coef='osLinux',dpar = "sigma"),
                prior(normal( -0.76,0.4)  , class='b',coef='osWindows_NT',dpar = "sigma"),
                prior(lkj(0.9) ,group='c_version',class = "cor")
            ),
            data = dx,cores=2,chains=2,control=list(adapt_delta = 0.999, max_treedepth=13))
cmr2 <- label(cmr2,'cmr')


x72 <- make_posterior_newdata(d.rel ,model_date='2020-02-10', CHAN='release')
x72 <- x72[date>'2020-02-09' & os=='Linux',]
oo72=getPredictions(M=cr.cm.rel, D=x72, nsamples=1000)

x73 <- make_posterior_newdata(dx,model_date='2020-02-12', CHAN='release')
x73 <- x73[date>'2020-02-11' & os=='Linux',]
oo73=getPredictions(M=cmr2, D=x73, nsamples=1000)
quantile(oo72,c(0.1,0.5,0.9))
quantile(oo73,c(0.1,0.5,0.9))
quantile( (oo73-oo72)/oo72*100,c(0.1,0.5,0.75,0.9))
mean( (oo73-oo72)/oo72*100 > 20)



y73 <- fread("https://sql.telemetry.mozilla.org/api/queries/68289/results.csv?api_key=d4VL1jBjwCz4khKk5ARWJLasNCYGGS8N2UEjVGqa")[order(date),][,lab:='73']
y72 <- fread("https://sql.telemetry.mozilla.org/api/queries/68290/results.csv?api_key=dVc0rlUK3cjyzJyRPSgspckI1PJhGnKxnZdGSocp")[order(date),][,lab:='72.0.2']
y722 <- y72[nvc<max(y73$nvc),]

y <- rbind(y73,y722)
ggplot(y,aes(nvc, cmr,color=lab,fill=lab))+geom_point()+geom_smooth()
ggplot(y,aes(nvc, cmi*100,color=lab,fill=lab))+geom_point()+geom_smooth()
dev.off()

m1 <- gam( cmi ~ lab+s(nvc,by=lab),data=y)


