# Dynamic Workflow Controller

[![Version](https://img.shields.io/docker/v/fnndsc/pl-dyworkflow?sort=semver)](https://hub.docker.com/r/fnndsc/pl-dyworkflow)
[![MIT License](https://img.shields.io/github/license/fnndsc/pl-dyworkflow)](https://github.com/FNNDSC/pl-dyworkflow/blob/main/LICENSE)
[![ci](https://github.com/FNNDSC/pl-dyworkflow/actions/workflows/ci.yml/badge.svg)](https://github.com/FNNDSC/pl-dyworkflow/actions/workflows/ci.yml)

`pl-dyworkflow` is a [_ChRIS_](https://chrisproject.org/) _ds_ plugin which connects to a parent node and then, based on some filtering criterion, creates a set of children based off data in this parent. Each child in turn becomes the base of a new workflow.

## Abstract

The `pl-dyworkflow` is an example of a "reflective" plugin, i.e. a plugin that itself speaks to CUBE to create dynamic compute graphs. In this specific case, `pl-dyworkflow` separates data in a parent node into _N_ child nodes, and then attaches a workflow to each child.

## Installation

`pl-dyworkflow` is a _[ChRIS](https://chrisproject.org/) plugin_ meaning it can run from either within _ChRIS_ or the command-line.

## Local Usage

To get started with local command-line usage, use [Apptainer](https://apptainer.org/) (a.k.a. Singularity) to run `pl-dyworkflow` as a container:

```shell
apptainer exec docker://fnndsc/pl-dyworkflow dyworkflow \
    [--args values...] input/ output/
```

To print its available options, run:

```shell
apptainer exec docker://fnndsc/pl-dyworkflow dyworkflow --help
```

## Examples

`dyworkflow` requires two positional arguments: a directory containing input data, and a directory where output data is saved. First, create the input directory, move input data into it, and then apply the filter/workflow:

```shell
mkdir incoming/ outgoing/
mv some.dat other.dat incoming/
apptainer exec docker://fnndsc/pl-dyworkflow:latest dyworkflow \
    [--args] incoming/ outgoing/
```

### More concretely

Let's assume there is workflow called "Leg Length Discrepancy". We can apply to this each `dcm` file in a parent node (specified with `--pluginInstanceID 1234`) with:

```shell
docker run --rm -it --userns=host  \
    -v $PWD/in:/incoming:ro -v $PWD/out:/outgoing:rw -w /outgoing       \
    localhost/fnndsc/pl-dyworkflow dyworkflow                           \
    --CUBEurl http://10.0.0.230:8000/api/v1/                            \
    --CUBEuser chris --CUBEpassword chris1234                           \
    --orthancURL http://10.0.0.230:8042                                 \
    --orthancuser orthanc --orthancpassword orthanc                     \
    --pattern **/*dcm --inNode --thread                                 \
    --pftelDB https://pftel-chris-public.apps.ocp-prod.massopen.cloud/api/v1/dyworkflow/%timestamp/analysis \
    --pipeline "Leg Length Discrepency Full Workflow v20230425"         \
    --pluginInstanceID 1234                                             \
    /incoming /outgoing
```

The above volume mounts allow for source side debugging.


## Development

Instructions for developers.

### Building

Build a local container image:

```shell
docker build -t localhost/fnndsc/pl-dyworkflow .
```

### Running

Mount the source code `dyworkflow.py` into a container to try out changes without rebuild.

```shell
docker run --rm -it --userns=host                                       \
    -v $PWD/dyworkflow.py:/usr/local/lib/python3.11/site-packages/dyworkflow.py:ro \
    -v $PWD/control:/usr/local/lib/python3.11/site-packages/control:ro  \
    -v $PWD/logic:/usr/local/lib/python3.11/site-packages/logic:ro      \
    -v $PWD/state:/usr/local/lib/python3.11/site-packages/state:ro      \
    -v $PWD/in:/incoming:ro -v $PWD/out:/outgoing:rw -w /outgoing       \
    localhost/fnndsc/pl-dyworkflow dyworkflow                           \
    --CUBEurl http://10.0.0.230:8000/api/v1/                            \
    --CUBEuser chris --CUBEpassword chris1234                           \
    --orthancURL http://10.0.0.230:8042                                 \
    --orthancuser orthanc --orthancpassword orthanc                     \
    --pattern **/*dcm --inNode --thread                                 \
    --pftelDB https://pftel-chris-public.apps.ocp-prod.massopen.cloud/api/v1/dyworkflow/%timestamp/analysis \
    --pipeline "Leg Length Discrepency Full Workflow v20230425"         \
    --pluginInstanceID 1234                                             \
    /incoming /outgoing
```

### Testing

Run unit tests using `pytest`.
It's recommended to rebuild the image to ensure that sources are up-to-date. Use the option `--build-arg extras_require=dev` to install extra dependencies for testing.

```shell
docker build -t localhost/fnndsc/pl-dyworkflow:dev --build-arg extras_require=dev .
docker run --rm -it localhost/fnndsc/pl-dyworkflow:dev pytest
```

## Release

Steps for release can be automated by [Github Actions](.github/workflows/ci.yml). This section is about how to do those steps manually.

### Increase Version Number

Increase the version number in `setup.py` and commit this file.

### Push Container Image

Build and push an image tagged by the version. For example, for version `1.2.3`:

```
docker build -t docker.io/fnndsc/pl-dyworkflow:1.2.3 .
docker push docker.io/fnndsc/pl-dyworkflow:1.2.3
```

### Get JSON Representation

Run [`chris_plugin_info`](https://github.com/FNNDSC/chris_plugin#usage)
to produce a JSON description of this plugin, which can be uploaded to a _ChRIS Store_.

```shell
docker run --rm localhost/fnndsc/pl-dyworkflow:dev chris_plugin_info > chris_plugin_info.json
```

