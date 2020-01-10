# missioncontrol-v2

An alternate view of crash and stability

## Installation instructions

This code is designed to be 'easy' to install and repeatable. That is if the underlying data doesn't change the output should not either.

This code should either work inside GCP or on a local computer. In either case, we recommend
a powerful one with at least 4 cores and a decent amount (8Gb+) memory. If using Docker, you will 
want to increase the amount of resources available to containers if you haven't already: the 
defaults are likely to be insufficient.

You will also want a [GCP service account](https://docs.telemetry.mozilla.org/cookbooks/bigquery.html#gcp-bigquery-api-access) with permission to read from the datasets in
`fx-data-shared-prod` and write access to a cloud storage bucket. Typically one would
do this using a sandbox project. Talk to data operations to  set this up if you don't have 
one already.

After getting a service setup, download the credentials into a file called `gcloud.json` in
the root of your checkout.

## Development instructions

The ETL pipeline is based on running a number of scripts in succession, performing
the following operations:

* Download the latest crash and usage data for a recent set of versions, and upload
  the results to a temporary table in BigQuery.
* Build a statistical model based on the above data downloaded as well as historical
  data that we have seen before.
* Generate an Rmarkdown-based report based on the output of the above model and upload
  it to google cloud storage.

### Option 1: Use the Docker container

This is the most deterministic approach and closest to what we are using in production, 
though it is likely to be slower on non-Linux hosts. These instructions assume that you have 
[Docker](https://www.docker.com/) and a basic set of developer tools installed on your machine.

First, build the container:

```bash
make build
```

Then, create a shell session inside it:

```bash
make shell
```

Skip to the next section to run the code.

### Option 2: Use Conda

This should run on the bare metal of your machine, and should be much faster on Mac. These instructions assume you have either [conda](https://docs.conda.io/projects/conda/en/latest/) 
or [miniconda](https://docs.conda.io/en/latest/miniconda.html) installed, as well as the 
[Google Cloud SDK](https://cloud.google.com/sdk/).

From the root checkout, creating and activating a conda environment is a two step process:

```bash
conda env create -n mc2 -f environment.yml
conda activate mc2
```

### Running

Once you have a shell (either in the docker container or activated conda environment), 
set some environment variables corresponding to your GCP settings:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=$PWD/gcloud.json
export GCP_PROJECT_ID=my-gcp-project-id
export GCS_OUTPUT_PREFIX=gs://my-cloud-storage-bucket
```

Then run the model:

```bash
./complete.runner.sh
```

If running on an underpowered machine, or you just want to get results more quickly, you can also 
enable debug mode, which simplifies the model generation significantly:

```bash
DEBUG=1 ./complete.runner.sh
```

### Gotchas

If you run the data pulling code shortly after a new release, and did not pull data in the
previous days, then those days' data could be missing for the previous major release versions.

To avoid this problem, you can copy the bigquery table used in production (`moz-fx-data-derived-datasets.analysis.missioncontrol_v2_raw_data`) to your own GCP project.