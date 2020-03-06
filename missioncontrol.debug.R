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
al2[, max(model_date),by=os]
(x <- al2[, quantile(posterior, 0.5)*100,by=os])

## First of we see that 74.0b9 does not have same model date for all OS
## chiefly , Linux has something from a different day

al3 <- merge(al[c_version=='74.0b9',], al[c_version=='74.0b9',][, list(model_date = max(model_date)) ,by=os], by=c("os","model_date"))
al3 <- al3[modelname=='cci',]
(y <- al3[,quantile(posterior, 0.5)*100, by=os])

## hence overall is actually based on _two_ operating systems
## something like
quantile((al3[os=='Darwin', posterior]+al3[os=='Windows_NT', posterior])/2,0.5)*100

## but if we included Linux then ...
quantile((al3[os=='Linux',posterior]+al3[os=='Darwin', posterior]+al3[os=='Windows_NT', posterior])/3,0.5)*100

## So _roughly_ what are the percent changes?
## Well for OS level it's
(merge(x,y, by='os')[, relChange := (V1.x-V1.y)/V1.y*100][,])

## it's sooo ODD ^^^!!

## So operating system chanes match, but overall doesn't match at all.
## well overall for 'x' is based on Linx, Windows, and Darwin
## but for 'y' its based on Windows and Darwin _only_

## Consider Case 1: based x's overall  on Windows and Darwin
quantile((al2[os=='Darwin', posterior]+al2[os=='Windows_NT', posterior])/2,0.5)*100
## compare to y's overall (which is based on Windows and Darwin)
y
## RelChange is (0.5807047  - 0.6069826)/0.6069826*100
## which is -4.329 % much more in line with the table above

## Consider Case 2: basing y on all three OS (but then they wont have same model_dates, but
## the most recent model dates for each OS)
## then we use x since its based on all three OS
x
al2[, max(model_date),by=os]
## and for y we use all three operating systems even though we have different model dates
al[c_version=='74.0b9',][, list(model_date = max(model_date)) ,by=os]

quantile((al3[os=='Linux',posterior]+al3[os=='Darwin', posterior]+al3[os=='Windows_NT', posterior])/3,0.5)*100
## and in this case relative change is now
(0.7876342  - 0.8842188)/0.8842188*100
## which is again reasonable with the above table

## so the summary is that the Current and Reference overalls have DIFFRENT component operating systems
## and when the different component operating systems are not the same the weirdness appears.


## dedup on channel,os,c_version, model_date, date
##al <- g$q(glue("select channel,os, c_version, model_date, date, nvc, modelname, rep, posterior from
##`moz-fx-data-derived-datasets`.analysis.missioncontrol_v2_posteriors
##where  model_date>='{Sys.Date()-7}'"),-1)
##fwrite(al,"/tmp/goog")
## remember to gzip this file

