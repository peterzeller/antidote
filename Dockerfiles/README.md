# Getting started locally

## Prerequisites

- Working recent version of Docker (1.12 and up recommended)

## Starting a local node

Start a local node with the command

```
docker run --rm -it -p "8087:8087" antidotedb/antidote
```

or if you prefer to run Antidote in detached mode:

```
docker run -d --name antidote -p "8087:8087" antidotedb/antidote
```

This should fetch the Antidote image automatically. For updating to the latest version use the command `docker pull antidotedb/antidote`.

Wait until Antidote is ready. The current log can be inspected with `docker logs antidote`. Wait until the log message `Application antidote started on node 'antidote@127.0.0.1'` appears.

Antidote should now be running on port 8087 on localhost.

## Building the image locally

For building the Docker image on your local machine, use the following command

```
docker build -f Dockerfiles/Dockerfile -t antidotedb/antidote Dockerfiles
```

## Running with Docker Toolbox (Windows)

Please note that Docker Toolbox is deprecated in favor of "Docker for Windows", for which these steps are not necessary.

To run this image with Docker Toolbox two changes in the Docker VirtualBox configuration are necessary.
Open VirtualBox and

- change the number of cores for your VM to at least two
- edit the network settings of your VM and enable port forwarding from port 8087 to 8087