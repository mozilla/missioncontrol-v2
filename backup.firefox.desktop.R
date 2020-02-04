setwd("~/missioncontrol-v2/")
source("missioncontrol.lib.R")


## Call as Rscript backup.firefox.desktop.R --backup=0  --data_file=default is ./all.the.data.Rdata
command.line <- commandArgs(asValues=TRUE,defaults=list(backup=0,data_file="./all.the.data.Rdata"),unique=TRUE)
loginfo(glue("loading data file from {command.line$data_file}"))
load(command.line$data_file)
backup.mode <- command.line$backup



data.file <- glue("/tmp/models-{n}.Rdata",n=n)
system(glue("cp {command.line$data_file} {data.file}"))
loginfo(glue("Saving Data to temp file: {data.file}"))

if(backup.mode == 1){
    system(glue("gsutil cp {data.file} {GCS_OUTPUT_PREFIX}/archive/"))
    system(glue("gsutil cp {command.line$data_file} {GCS_OUTPUT_PREFIX}/archive/"))
    loginfo(glue("Data file saved at {GCS_OUTPUT_PREFIX}/archive/{data.file}. Download using gsutil cp"))
}else{
    loginfo("Not running, just showing what would be run")
    loginfo(glue("gsutil cp {data.file} {GCS_OUTPUT_PREFIX}/archive/"))
    loginfo(glue("gsutil cp {command.line$data_file} {GCS_OUTPUT_PREFIX}/archive/"))
    loginfo(glue("Data file saved at {GCS_OUTPUT_PREFIX}/archive/{data.file}. Download using gsutil cp"))
}
