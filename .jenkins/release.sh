#!/usr/bin/env bash

release_opts="-Prelease.useAutomaticVersion=true"
if [ -n "${RELEASE_VERSION}" ]; then
        release_opts="${release_opts} -Prelease.releaseVersion=${RELEASE_VERSION}"
fi
if [ -n "${NEXT_VERSION}" ]; then
        release_opts="${release_opts} -Prelease.newVersion=${NEXT_VERSION}"
fi

# ./gradlew clean :core:release --stacktrace ${release_opts} -x updateVersion -x commitNewVersion
./gradlew clean release --stacktrace ${release_opts} -x updateVersion -x commitNewVersion
