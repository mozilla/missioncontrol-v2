source("missioncontrol.lib.R")



loginfo("Starting Downloading Data")

runner <- glue("#!/bin/sh
cd mc2
python data/crud.py main --project_id '{GCP_PROJECT_ID}' --table_name '{RAW_OUTPUT_TABLE}'")
writeLines(runner,con="./runner.sh")
res  <- system2("sh", "./runner.sh",stderr=TRUE,stdout=TRUE)
loginfo(paste(res, collapse="\n"))
if(any(grepl("(Traceback|(E|e)xception)",res))|| any(grepl("(f|F)ailed",res))){
    logerror("Problem with Creating Raw Data")

    stop("Problem with Creating Raw Data")
}

loginfo("Finished Downloading Data")

