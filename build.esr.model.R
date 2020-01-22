
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

## Now for major/minor dates=='2040-01-01'  fill in with next most one which will
## correspond to end date of last minor on previous major
uesr[rdate=='2040-01-01', rdate:=uesr[ pmin(which((rdate=='2040-01-01'))+1,nrow(uesr)) ,rdate]]


## Now get some data
esr.data <-  fread("bq query -n 500000000 --format=csv --nouse_legacy_sql 'select *  from analysis.missioncontrol_v2_raw_data_test_bh_mast'")

## Restrict data to some cuttoff date (i dont think we need so much data anyways)
## the cutoff date is a bit arbitrary
esr.data <- esr.data[date>='2019-07-01',]
esr.data <- merge(esr.data,uesr[, list(c_version, rdate)], by="c_version",keep.x=TRUE)
esr.data <- esr.data[date<=rdate][, rdate:=NULL]






    
