[![Build Status](https://travis-ci.org/HSLdevcom/transitdata-rata-digitraffic-source.svg?branch=master)](https://travis-ci.org/HSLdevcom/Transitdata-rata-digitraffic-source)

# Transitdata-rata-digitraffic-source

## Description

Application for listening to the train data from mqtt and create trips for trains.
Writes output to another Pulsar topic.

## Building

### Dependencies

This project depends on [transitdata-common](https://github.com/HSLdevcom/transitdata-common) project.

### Locally


### Docker image

- Run [this script](build-image.sh) to build the Docker image


## Tests:

We're separating our unit & integration tests using [this pattern](https://www.petrikainulainen.net/programming/maven/integration-testing-with-maven/).

Unit tests:

- add test classes under ./src/test with suffix *Test.java
- `mvn clean test -P unit-test`   

Integration tests:

- add test classes under ./src/integration-test with prefix IT*.java
- `mvn clean verify -P integration-test`   


## Running

Requirements:
- Local Pulsar Cluster
  - By default uses localhost, override host in PULSAR_HOST if needed.
    - Tip: f.ex if running inside Docker in OSX set `PULSAR_HOST=host.docker.internal` to connect to the parent machine
  - You can use [this script](https://github.com/HSLdevcom/transitdata/blob/master/bin/pulsar/pulsar-up.sh) to launch it as Docker container

Launch Docker container with

```docker-compose -f compose-config-file.yml up <service-name>```   
