#!/bin/sh

./gradlew logstorageunit:build
tar -xf logstorageunit/build/distributions/logstorageunit.tar -C logstorageunit/build/distributions
logstorageunit/build/distributions/logstorageunit/bin/logstorageunit -p 7777 --page-size 4096 --page-count 4096
