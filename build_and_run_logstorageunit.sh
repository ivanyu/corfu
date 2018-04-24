#!/bin/sh

./gradlew storageunit:build
tar -xf storageunit/build/distributions/storageunit.tar -C storageunit/build/distributions
storageunit/build/distributions/storageunit/bin/storageunit -p 7777 --page-size 4096 --page-count 4096
