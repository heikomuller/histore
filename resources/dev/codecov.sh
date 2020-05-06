#!/bin/bash

export CODECOV_TOKEN=$1
bash <(curl -s https://codecov.io/bash)
