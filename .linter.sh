#!/bin/bash
cd /home/kavia/workspace/code-generation/fastqueueapi-107750-bdde5977/fastqueueapi
flake8 .
LINT_EXIT_CODE=$?
if [ $LINT_EXIT_CODE -ne 0 ]; then
  exit 1
fi

