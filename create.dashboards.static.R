setwd("~/missioncontrol-v2/")
source("missioncontrol.lib.R")

## Call as Rscript backup.firefox.desktop.R  --data_file=default is ./all.the.data.Rdata --backup=0
## if backup = false, it wont backup default is false
command.line <- commandArgs(asValues=TRUE,defaults=list(quiet=1,backup=0,data_file="./all.the.data.Rdata"),unique=TRUE)
loginfo(glue("loading data file from {command.line$data_file}"))
backup.mode <- command.line$backup
load(command.line$data_file)
renderQuiet <- if(command.line$quiet==0) FALSE else TRUE


system("rm -rf ~/html")
dir.create("~/html")
dir.create("~/html/public/")
dir.create("~/html/private")
dir.create("~/html/archive/")
loginfo("Please look inside ~/html/{public/private/} for dashboards")


genf <- function(D,ch=c("release beta nightly faq")){
    if(D=="public"){
        if(grepl("release",ch)) render("mc2/release.Rmd",quiet = renderQuiet,params=list(dest=D));
        if(grepl("beta",ch)) render("mc2/beta.Rmd",quiet = renderQuiet,params=list(dest=D));
        if(grepl("nightly",ch)) render("mc2/nightly.Rmd",quiet = renderQuiet,params=list(dest=D))
        if(grepl("faq",ch)) render("mc2/faq.Rmd",quiet = renderQuiet,params=list(dest=D))
        system("rsync  --exclude '*py' --exclude '*cache' --exclude 'data' --exclude 'tests'  -az mc2/ ~/html/public/")
        if(backup.mode==1){
            system(glue("gsutil -q -m rsync -d -r  ~/html/public/ {GCS_OUTPUT_PREFIX}/html/public/"))
        }
    }else if(D=="moco"){
        if(grepl("release",ch)) render("mc2/release.Rmd",quiet = renderQuiet,params=list(dest=D));
        if(grepl("beta",ch)) render("mc2/beta.Rmd",quiet = renderQuiet,params=list(dest=D));
        if(grepl("nightly",ch)) render("mc2/nightly.Rmd",quiet = renderQuiet,params=list(dest=D))
        if(grepl("faq",ch)) render("mc2/faq.Rmd",quiet = renderQuiet,params=list(dest=D))
        system("rsync  --exclude '*py' --exclude '*cache' --exclude 'data' --exclude 'tests'  -az mc2/ ~/html/private/")
        system(glue("mkdir ~/html//archive/{loc}; rsync -az ~/html/private/  ~/html/archive/{loc}/",
                    loc=dall.rel2[,max(date)]))
        if(backup.mode==1){
            system(glue("gsutil -q -m rsync -d -r  ~/html/private/ {GCS_OUTPUT_PREFIX}/html/private/"))
            system(glue("gsutil -q -m rsync -d  -r ~/html/archive/{loc}/ {GCS_OUTPUT_PREFIX}/html/archive/{loc}/"
                      , loc=dall.rel2[,max(date)]))
        }
    }
}


if(processDownloadsWorked){
    loginfo("Creating Dashboards")
    genf("public")
    genf("moco")
    loginfo(glue("Public: {GCS_OUTPUT_PREFIX}/html/public/"))
    loginfo(glue("Private: {GCS_OUTPUT_PREFIX}/html/private/"))
    loginfo(glue("Archive: {GCS_OUTPUT_PREFIX}/html/archive/"))
    system(glue("gsutil ls {GCS_OUTPUT_PREFIX}/html/archive/"))
}else{
    logerror("Something went wrong processing downloads, stopping")
    stop("Something went wrong processing downloads, stopping")
}

