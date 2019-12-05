setwd("~/missioncontrol-v2/")
source("missioncontrol.lib.R")


## Call as Rscript process.model.firefox.desktop.R --data_file=default is ./all.the.data.intermediate.Rdata --out=default is ./all.the.data.Rdata
command.line <- commandArgs(asValues=TRUE,defaults=list(data_file="./all.the.data.intermediate.Rdata",out="./all.the.data.Rdata"),unique=TRUE)
loginfo(glue("loading data file from {command.line$data_file}"))
load(command.line$data_file)


processDownloadsWorked <- FALSE
operating.systems <- c("Windows_NT","Darwin","Linux","overall")
loginfo("Starting posteriors")

## The %<% is a future, see https://cran.r-project.org/web/packages/future/future.pdf
## search for 'future' function.

release.current.vs.previous %<-% Lapply(operating.systems,function(os){
    compare.two.versions.2(versiona = getCurrentVersion(dall.rel2,os,'release'),
                         versionb = getPreviousVersion(dall.rel2,os,'release'),
                         oschoice = os,
                         dataset  = dall.rel2,
                         model    = list( mr=cr.cm.rel,cr=cr.cc.rel,mi=ci.cm.rel,ci=ci.cc.rel),
                         doLatest = FALSE)
})

release.current.vs.previous.realNVC %<-% Lapply(operating.systems,function(os){
    compare.two.versions.2(versiona = getCurrentVersion(dall.rel2,os,'release'),
                         versionb = getPreviousVersion(dall.rel2,os,'release'),
                         oschoice = os,
                         dataset  = dall.rel2,
                         model    = list( mr=cr.cm.rel,cr=cr.cc.rel,mi=ci.cm.rel,ci=ci.cc.rel),
                         doLatest = FALSE,
                         normalizeNVC = FALSE)
})


beta.current.vs.previous  %<-% Lapply(operating.systems,function(os){
    compare.two.versions.2(versiona = getCurrentVersion(dall.beta2,os,'beta'),
                         versionb = getPreviousVersion(dall.beta2,os,'beta'),
                         oschoice = os,
                         dataset  = dall.beta2,
                         model    = list( mr=cr.cm.beta,cr=cr.cc.beta,mi=ci.cm.beta,ci=ci.cc.beta),
                         doLatest = TRUE)
  })


## It doesn't make sense to set the previous version something that is yesterdays
## There is so little data. Might as well set it to some version with at least 

nightly.current.vs.previous  %<-% Lapply(operating.systems,function(os){
    compare.two.versions.2(versiona = getPreviousVersion(dall.nightly2,os,'nightly'),
                         versionb = getMaxVersionBeforeX(dall.nightly2, os,'nightly',
                                                         c(getCurrentVersion(dall.nightly2,os,'beta'),getPreviousVersion(dall.nightly2,os,'nightly'))),
                         oschoice = os,
                         dataset  = dall.nightly2,
                         model    = list( mr=cr.cm.nightly,cr=cr.cc.nightly,mi=ci.cm.nightly,ci=ci.cc.nightly),
                         doLatest = TRUE)
  })


names(release.current.vs.previous) <- operating.systems
names(release.current.vs.previous.realNVC) <- operating.systems
names(beta.current.vs.previous) <- operating.systems
names(nightly.current.vs.previous) <- operating.systems
loginfo(" Posteriors Complete")


loginfo("Starting Summaries")


release.usage <- makeUsageSummary(release.current.vs.previous)
release.summary <- makeSummaryTable(release.current.vs.previous)


beta.usage <- makeUsageSummary(beta.current.vs.previous)
beta.summary <- makeSummaryTable(beta.current.vs.previous)



nightly.usage <- makeUsageSummary(nightly.current.vs.previous)
nightly.summary <- makeSummaryTable(nightly.current.vs.previous)


###############################################################################################
## Evolution Figures
## which indicates the estimate crash rates and incidences of different versions (major/minor)
## Doesn't compare two versions!
###############################################################################################

loginfo("Starting Evolution")
release.evolution <- get.evolution(model=list( mr=cr.cm.rel,cr=cr.cc.rel,mi=ci.cm.rel,ci=ci.cc.rel),
                                dataset = dall.rel2)
beta.evolution <- get.evolution(model=list( mr=cr.cm.beta,cr=cr.cc.beta,mi=ci.cm.beta,ci=ci.cc.beta),
                                dataset = dall.beta2)
nightly.evolution <- get.evolution(model=list( mr=cr.cm.nightly,cr=cr.cc.nightly,mi=ci.cm.nightly,ci=ci.cc.nightly),
                                dataset = dall.nightly2[c_version<=getCurrentVersion(dall.nightly2,"Linux",'nightly'),])


##################################################
## Make BigQuery Tables for Model Output
##################################################

n <- as.character(Sys.Date())
n <- as.character(dall.rel2[,max(date)])


allversions <- list(
    release=list(v=dall.rel2[,unique(c_version)],c=(release.current.vs.previous)$Darwin$versiona,p=(release.current.vs.previous)$Darwin$versionb),
    beta=list(v=dall.beta2[,unique(c_version)],c=(beta.current.vs.previous)$Darwin$versiona,p=(beta.current.vs.previous)$Darwin$versionb),
    nightly=list(v=dall.nightly2[,unique(c_version)],c=(nightly.current.vs.previous)$Darwin$versiona,p=(nightly.current.vs.previous)$Darwin$versionb)
)


gen.time <- Sys.time()
processDownloadsWorked <- TRUE
save.list <- list(
    "processDownloadsWorked","n",
    "allversions","gen.time",
    "cr.cm.rel","cr.cc.rel","ci.cm.rel","ci.cc.rel",
    "cr.cm.beta","cr.cc.beta","ci.cm.beta","ci.cc.beta",
    "cr.cm.nightly","cr.cc.nightly","ci.cm.nightly","ci.cc.nightly",
    "dall.rel2","dall.beta2","dall.nightly2",
    "release.current.vs.previous","release.current.vs.previous.realNVC","release.usage","release.summary","release.evolution",
    "beta.current.vs.previous","beta.usage","beta.summary","beta.evolution",
    "nightly.current.vs.previous","nightly.usage","nightly.summary","nightly.evolution"
)
save(list=unlist(save.list),file=command.line$out)
loginfo("Saved data to {command.line$out}")
