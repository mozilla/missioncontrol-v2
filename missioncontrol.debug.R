source("~/prefix.R")
g <- bq()


######################################################################
## Here is an example
## converting text to table: https://github.com/jrblevin/markdown-mode
## DATA AS OF (asOfdate) = 2020-03-04
## Current version is beta 74.0 Compared to 74.0b9

## Crash Incidence Summary
## | OS         | Score            | Main Crash       | Content Crash    |
## |------------|------------------|------------------|------------------|
## | Windows_NT | 1.49% (-0.75%)   | 0.62% (3.24%)    | 0.87% (-3.35%)   |
## | Darwin     | 0.77% (-1.83%)   | 0.48% (1.72%)    | 0.29% (-7.50%)   |
## | Linux      | 2.15% (-5.68%)   | 0.93% (12.27%)   | 1.20% (-17.07%)  |
## | overall    | 1.47% (29.52%) ▲ | 0.68% (27.25%) △ | 0.79% (30.07%) ▲ |

## See how in Content Crash column, the operating systems have negative
## change yet overall has positive?!
## Something less egregeious happens for Main Crash Incidence

######################################################################


## GET ALL (or nearly all) THE DAMN DATA

channel <- "beta"
al <- g$q(glue("select * from
`moz-fx-data-derived-datasets`.analysis.missioncontrol_v2_posteriors
where channel = '{channel}' and  model_date>='{Sys.Date()-30}'"),-1)

al[, max(model_date),by=list(os,c_version)][order(os,V1),]

al2 <- al[c_version=='74.0' & model_date=='2020-03-04' & modelname=='cci',]
al2[, quantile(posterior, 0.5)*100,by=os]



al3 <- merge(al[c_version=='74.0b9',], al[c_version=='74.0b9',][, list(model_date = max(model_date)) ,by=os], by=c("os","model_date"))
al3 <- al3[modelname=='cci',]
al3[,quantile(posterior, 0.5)*100, by=os]
