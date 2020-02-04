FROM continuumio/miniconda3
ENV DEBIAN_FRONTEND noninteractive

# install necessary base dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    rsync \
    vim

# install google cloud sdk
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -
RUN apt-get update && apt-get install -y google-cloud-sdk

# clean up now-unnecessary packages + apt-get cruft
RUN apt-get remove -y gnupg curl 
RUN apt-get autoremove -y && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Add our deps to the base conda environment
COPY environment.yml /tmp
RUN conda config --add channels conda-forge
RUN conda env update -n base -f /tmp/environment.yml

WORKDIR /mc-etl

# for development purposes, we mount the current directory into /mc-etl, but
# can't count on that for production deployments
COPY . /mc-etl

# So we don't get a bunch of annoying first run prompts
RUN touch ~/.bigqueryrc

# Placeholder command so container can be kept running if desired
CMD tail -f /dev/null
