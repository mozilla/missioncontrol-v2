FROM ubuntu:18.04
ENV PYTHONDONTWRITEBYTECODE 1
ENV DEBIAN_FRONTEND noninteractive
ENV RENV_VERSION 0.9.2
ENV RENV_PATHS_CACHE /renv/cache
ENV GOOGLE_APPLICATION_CREDENTIALS /mc-etl/gcloud.json

# install necessary base dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    apt-transport-https \
    ca-certificates \
    curl \
    gcc \
    gnupg \
    llvm \
    python3-dev \
    software-properties-common \
    libcurl4-openssl-dev \
    libssl-dev \
    pandoc \
    rsync \
    vim

# set python3 as default
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 10

# install google cloud sdk (for debugging)
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -
RUN apt-get update && apt-get install -y google-cloud-sdk

# install R
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9
RUN add-apt-repository 'deb https://cloud.r-project.org/bin/linux/ubuntu bionic-cran35/'
RUN apt-get update && apt-get install -y r-base

# Install pip + requirements
COPY requirements.txt /tmp
RUN apt-get update && apt-get install -y curl
RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && python get-pip.py
RUN pip install -r /tmp/requirements.txt

# Install R packages
COPY renv.lock /tmp
WORKDIR /tmp
RUN R -e "install.packages('remotes', repos = c(CRAN = 'https://cloud.r-project.org'))"
RUN R -e "remotes::install_github('rstudio/renv@${RENV_VERSION}')"
RUN R -e "renv::restore()"

# clean up
RUN apt-get autoremove -y && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

WORKDIR /mc-etl

# for development purposes, we mount the current directory into /mc-etl, but
# can't count on that for production deployments
COPY . /mc-etl

# for backwards-compatibility purposes only
RUN ln -s /mc-etl /root/missioncontrol-v2

# So we don't get a bunch of annoying first run prompts
RUN touch ~/.bigqueryrc

# Placeholder command so container can be kept running if desired
CMD tail -f /dev/null
