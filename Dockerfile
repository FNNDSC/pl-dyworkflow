# Python version can be changed, e.g.
# FROM python:3.8
# FROM docker.io/fnndsc/conda:python3.10.2-cuda11.6.0
FROM docker.io/python:3.11.3-slim-bullseye

LABEL org.opencontainers.image.authors="FNNDSC <dev@babyMRI.org>" \
      org.opencontainers.image.title="Dynamic Workflow Controller" \
      org.opencontainers.image.description="A ChRIS plugin that "splits" a parent into multiple children and adds a specified workflow to each child"

WORKDIR /usr/local/src/pl-dyworkflow

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
ARG extras_require=none
RUN pip install ".[${extras_require}]"

CMD ["dyworkflow"]
