

genf <- function(D,ch=c("release beta nightly")){
    if(D=="public"){
        if(grepl("release",ch)) render("mc2/release.Rmd",params=list(dest=D));
        if(grepl("beta",ch)) render("mc2/beta.Rmd",params=list(dest=D));
        if(grepl("nightly",ch)) render("mc2/nightly.Rmd",params=list(dest=D))
        if(grepl("faq",ch)) render("mc2/faq.Rmd",params=list(dest=D))
        system("rsync  -avz mc2/ ~/pubsguha/mc2")
    }else if(D=="moco"){
        if(grepl("release",ch)) render("mc2/release.Rmd",params=list(dest=D));
        if(grepl("beta",ch)) render("mc2/beta.Rmd",params=list(dest=D));
        if(grepl("nightly",ch)) render("mc2/nightly.Rmd",params=list(dest=D))
        if(grepl("faq",ch)) render("mc2/faq.Rmd",params=list(dest=D))
        system(glue("mkdir mc2/archive/{loc}; rsync --exclude archive -avz mc2/ mc2/archive/{loc}/",loc=dall.rel2[,max(date)]))
    }
}

setwd("~/mz/missioncontrol/ex1/")
source("~/mz/core.load.R")
library(rmarkdown)
#options(mc.cores = para]llel::detectCores())
#library(rstan)
#rstan_options(auto_write = TRUE)
library(brms)
source("download.data.R")
source("process.downloads.R")

if(processDownloadsWorked){
    genf("public")
    genf("moco")
}else{
    slackr("Error in running processdownloads")
    stop()
}

#print("Running Code on GS Server")
#system('gcloud compute instances start "sguha-datascience" --zone=us-west1-b')
#system('gcloud beta compute --project "moz-fx-dev-sguha-rwasm" ssh --zone "us-west1-b" "sguha-datascience" --comand=./runMCModels.R')
#print("Downloading current data")
#system("gsutil cp  gs://moz-fx-data-derived-datasets-analysis/sguha/missioncontrol/archive/all.the.data.Rdata ./")
#load("all.the.data.Rdata")
#print("RENDERING")
#genf("public")
#genf("moco")

# system("gsutil ls  gs://moz-fx-data-derived-datasets-analysis/sguha/missioncontrol/archive/")
