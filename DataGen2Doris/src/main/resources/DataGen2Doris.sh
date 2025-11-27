#!/bin/sh

app_name=$(basename "$0")
prop_name=$(basename "$0" .sh)

flink run-application  \
  -t yarn-application  \
  -Dyarn.application.name=$app_name \
  -Dyarn.ship-files=$prop_name.properties  \
  -Dyarn.classpath.include-user-jar=DISABLED \
  ./DataGen2Doris-1.0.jar \
  ./$prop_name.properties