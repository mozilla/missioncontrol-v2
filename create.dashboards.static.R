setwd("~/missioncontrol-v2/")
source("missioncontrol.lib.R")
load("all.the.data.Rdata")

system("rm -rf ~/html")
dir.create("~/html")
dir.create("~/html/public/")
dir.create("~/html/private")
dir.create("~/html/archive/")

renderQuiet <- TRUE
genf <- function(D,ch=c("release beta nightly faq")){
    if(D=="public"){
        if(grepl("release",ch)) render("mc2/release.Rmd",quiet = renderQuiet,params=list(dest=D));
        if(grepl("beta",ch)) render("mc2/beta.Rmd",quiet = renderQuiet,params=list(dest=D));
        if(grepl("nightly",ch)) render("mc2/nightly.Rmd",quiet = renderQuiet,params=list(dest=D))
        if(grepl("faq",ch)) render("mc2/faq.Rmd",quiet = renderQuiet,params=list(dest=D))
        system("rsync  --exclude '*py' --exclude '*cache' --exclude 'data' --exclude 'tests'  -az mc2/ ~/html/public/")
        if(!exists("debugg")){
            system(glue("gsutil -q -m rsync -d -r  ~/html/public/  gs://moz-fx-data-derived-datasets-analysis/sguha/missioncontrol-v2/html/public/"))
        }
    }else if(D=="moco"){
        if(grepl("release",ch)) render("mc2/release.Rmd",quiet = renderQuiet,params=list(dest=D));
        if(grepl("beta",ch)) render("mc2/beta.Rmd",quiet = renderQuiet,params=list(dest=D));
        if(grepl("nightly",ch)) render("mc2/nightly.Rmd",quiet = renderQuiet,params=list(dest=D))
        if(grepl("faq",ch)) render("mc2/faq.Rmd",quiet = renderQuiet,params=list(dest=D))
        system("rsync  --exclude '*py' --exclude '*cache' --exclude 'data' --exclude 'tests'  -az mc2/ ~/html/private/")
        system(glue("mkdir ~/html//archive/{loc}; rsync -az ~/html/private/  ~/html/archive/{loc}/",
                    loc=dall.rel2[,max(date)]))
        if(!exists("debugg")){
            system(glue("gsutil -q -m rsync -d -r  ~/html/private/  gs://moz-fx-data-derived-datasets-analysis/sguha/missioncontrol-v2/html/private/"))
            system(glue("gsutil -q -m rsync -d  -r ~/html/archive/{loc}/  gs://moz-fx-data-derived-datasets-analysis/sguha/missioncontrol-v2/html/archive/{loc}/"
                      , loc=dall.rel2[,max(date)]))
        }
    }
}


if(processDownloadsWorked){
    loginfo("Creating Dashboards")
    genf("public")
    genf("moco")
    loginfo("Public: gs://moz-fx-data-derived-datasets-analysis/sguha/missioncontrol-v2/html/public/")
    loginfo("Private: gs://moz-fx-data-derived-datasets-analysis/sguha/missioncontrol-v2/html/private/")
    loginfo("Archive: gs://moz-fx-data-derived-datasets-analysis/sguha/missioncontrol-v2/html/archive/")
    system("gsutil ls gs://moz-fx-data-derived-datasets-analysis/sguha/missioncontrol-v2/html/archive/")
}else{
    logerror("Something went wrong processing downloads, stopping")
    stop("Something went wrong processing downloads, stopping")
}

