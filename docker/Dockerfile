FROM rocker/tidyverse:4.4.2 as base
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -y
RUN apt-get install -y apt-utils software-properties-common gpg curl apt-utils
RUN curl -sS https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor | tee /etc/apt/trusted.gpg.d/cloud-sdk-bullseye.gpg
RUN apt-get update && apt-get install -y \
    libssl-dev \
    libcurl4-openssl-dev \
    libxml2-dev \
    libfontconfig1-dev \
    libharfbuzz-dev \
    libfribidi-dev \
    libfreetype6-dev \
    libpng-dev \
    libtiff5-dev \
    libjpeg-dev \
    libgdal-dev \
    libgeos-dev \
    libproj-dev \
    libudunits2-dev \
    gcc g++\
    git \
    && apt-get clean

RUN install2.r --repos 'http://cran.rstudio.com/' renv
RUN mkdir /src/
WORKDIR /src/

FROM base AS development-user

RUN chmod -R g=u /root/ /src/
RUN apt-get install -y sudo
RUN apt-get install -y sl tree

ARG USER
ARG UID
ARG GID
RUN set -x && groupadd -g ${GID} -o ${USER}
RUN useradd -m -d /home/developer -u ${UID} -g ${GID} -o -s /bin/bash ${USER}

RUN usermod -aG root,sudo ${USER}
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
ENV PATH="${HOME}/.local/bin:${PATH}:/usr/games"

USER ${USER}

FROM development-user as development
ENV RENV_PATHS_LIBRARY=/home/developer/library
RUN mkdir ${RENV_PATHS_LIBRARY}
RUN sudo chmod -R 777 /usr/local/lib/R/site-library

COPY renv.lock renv.lock
RUN Rscript -e 'renv::restore()'
RUN rm renv.lock

ENTRYPOINT /bin/bash
