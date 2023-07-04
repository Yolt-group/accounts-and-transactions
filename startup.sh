#!/bin/sh

# --- validate docker environment ---

if [ ! -f "/home/yolt/MainClassName" ]; then
  echo "/home/yolt/MainClassName not found. Make sure this file is present in the base image."
  exit 1
fi

# shellcheck disable=SC2143
if [ "$(
  find /app/lib -mindepth 1 -print -quit 2>/dev/null | grep -q .
  echo $?
)" != "0" ]; then
  echo "/app/lib is empty. Nothing to put on the jvm classpath."
  exit 1
fi

# shellcheck disable=SC2046
if [ "${ENVIRONMENT}" = "app-acc" ] || [ "${ENVIRONMENT}" = "yfb-acc" ] || [ $(echo "${ENVIRONMENT}" | cut -c -4) = "team" ]; then
  echo "debugging enabled"
  DEBUG_OPTS="-XX:NativeMemoryTracking=summary"
else
  echo "debugging disabled"
fi

# --- start jvm ---

# shellcheck disable=SC2046
# shellcheck disable=SC2086

exec java \
  ${DEBUG_OPTS} \
  -XX:+UnlockDiagnosticVMOptions \
  -XX:+UnlockExperimentalVMOptions \
  -XX:MaxRAMPercentage=75.0 \
  -XX:-OmitStackTraceInFastThrow \
  -Djava.security.egd=file:/dev/./urandom \
  -cp app:app/lib/* $(cat /home/yolt/MainClassName)
