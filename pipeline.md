## Introduction

This version to MissionControl takes a different approach to measuring stability. We consider stability comprised of two measures: how many profiles crash and at what rate they crash. The proportion of profiles crashing on a day is called the crash incidence  and the rate at which these profiles crash is called their crash rate. Taking these two numbers together we get a picture of :

- How many profiles are being affected
- How severe the effect is for these profiles

 ## Pipeline

- Download and transform stability and usage data.
 - This stage consits of downloading the data for different versions and channels, and appending new data to a BigQuery table.
 - This needs to be run daily.
- Run the statistical models that estimate the crash rates and incidences for different channels and versions at various stages of release
 - These modesl, run daily, read the data stored in the above BigQuery table, producing lots of model output which drives the dashboards
- Given model output produce the dashboard
  - We have a dashboard for Release,Beta and Nightly which is created once a day.

## Details of Pipeline Steps

### Columns
The data download step downloads usage information such as 

- How many users active today
- How many users active on the latest version today
- How many active hours today
- How many active hours on the latest version today

These columns provide information on the adoption of the current version.  We have seen crash rates generally fluctuate a lot during the initial stages of rollout and then assuming no change in underlying environment, as more profiles adopt, the crash metrics stabilize. Also by measuring stability as a function of adoption, we automatically adjust for differing release rollout schedules and seasonality. 

Other columns are 

- How many crashed on current version (main and content)
- How many hours experienced by those that crashed
- These together tell us the crash incidence and the crash rate for the crashers.

### What Data is Kept

Before the data is uploaded to BigQuery(BQ), we truncate some of the data. We
have seen for Release and Beta (where a current version stays current for long
periods of time), that adoption on the current version drops dramatically when a
new version is released.

For release, we keep data for a channel/version combination for the period it is
current. During this period we see adoption increased, reach a peak and
oscillate around this peak. When a new version is released, we stop using data
(usage/stability) from the now older version.

In its current form, we would have applied the same procedure to Beta, however
because RC builds are not listed in a suitable manner in buildhub this approach
becomes very cumbersome. Hence for Beta, we use data till the version reaches
its peak adoption and stop using data thereafter. Admittedly we are dropping
some data here since beta builds can be in user longer than the time it takes to
reach peak adoption. 

For Nightly, we use data for a build (aggregated to YYYYMMDD) to the point
adoption reaches peak. Nightly builds last a few days before profiles move to a
newer one. If and when Beta moves to a Nightly frequency we will settle on this
approach for Beta (which are currently inappropriately using).


### Frequency 
Once the data is trimmed it is  uploaded to BigQuery. The original data queries
pull data for all days for the current major version and different channels. The
data is then deduped with respect to what’s in BQ and then uploaded. Thus if the
download is run twice, we do not get duplicates. If we forget to run one day, it
is no matter, since the next day will pick all the data. Problems occur when the
next day coincides with a major version change. This ought be rare enough we
need not handle it (and we expect data downloads to occur smoothly every day :)



### Modeling Steps

We don’t compute raw numbers which would be susceptible much volatility for initial rollout periods when the adoption numbers are small and not necessarily representative. Instead we assume that the crash incidence/rate for a version/channel is parameter that we would like to estimate. If there were 100% instantaneous rollout, then we could estimate this parameter easily for every os/channel combination. However since this is not the case, our estimate is confounded by the effect of adoption. In other words, we believe the statistical model (e..g for main crash rate) is

```
cmain   ~  offset(log( usage_cm_crasher_cversion+1/60))  + os + (1+os|version)
shape   ~  os
```

Where cmain follows a negative binomial distribution (typical for count data). However , because adoption can change things, we adjust the model like so

```
cmain   ~ offset(log( usage_cm_crasher_cversion+1/60))  + os +  (1+os|version) + s(adoption,by=os)
shape   ~ os + s(adoption)
```

That is we assume the the count of crashes is affected by current adoption. Note
that adoption reaches a peak/steady state so it does not have unbounded effect
on crash rates/incidences. For nightly models, the shape (which is a measure of
dispersion) does not take into  account adoption.


We use the R package brms to model the 2 models for Crash Rates and Incidences
respectively. 4 for each channel, and 12 models across channels. These can be
executed in parallel (and we do).  The brms package allows the analyst to model
data in the Bayes framework and the two reasons for using this are

- Modelling flexibility: brms has a wealth of statistical families and a flexible modeling language
- Posterior distributions: makes it very easy to provide uncertainty estimates and probabilities around estimated quantities which becomes useful for signalling alerts

Using the model we produce all the quantities required for the dashboard:
- Top level numbers regarding current version:
 - OS specific and Overall crash rate, crash incidence (main/content/all) and relative changes wrt to previous version and alerts 
  -A regression is defined as more than 20% change. We provide color coded
  alerts based on how confident we are the estimated change is indeed more than
  20%. This often depends on how large the change is and how much adoption
- Evolution of rates/incidences across versions telling us how things have changed over time
- For release, day by day comparisons with previous version

The data is saved in Rdata files. 

### The Dashboard
Currently, the code that drives the models also drives the dashboard, using the
Rmarkdown format. This can be changed (and will be). The dashboard lives at
https://metrics.mozilla.com/~sguha/mz/missioncontrol/ex1/mc2/nightly.html

And archives live at
https://metrics.mozilla.com/~sguha/mz/missioncontrol/ex1/mc2/archive

