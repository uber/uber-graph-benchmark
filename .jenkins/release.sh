#!/usr/bin/env bash

./gradlew clean :core:release --stacktrace ${release_opts} -x updateVersion -x commitNewVersion
