#!/bin/bash

script_dir=~/Documents/Projects/vacancy_watcher_requests
cd $script_dir
source ./venv/bin/activate
python vacancy_watcher_async.py $@
