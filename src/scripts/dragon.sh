#!/usr/bin/env bash

DIR=$(dirname $0)
DRAGON_HOME=$DIR/..
export RUN_JAVA_OPTS=-javaagent:$DRAGON_HOME/lib/dragon.jar
$DIR/run-java.sh dragon.Run "$@"