#!/bin/bash

execpath="$0"
scriptpath="$neurocaasrootdir/ncap_utils"

source "$scriptpath/workflow.sh"

## Import functions for data transfer 
source "$scriptpath/transfer.sh"

## Set up error logging.
errorlog

python3 "../AWSLabeljobCreation/createLabelJobGeneral.py" "$bucketname" "$dataname" "$inputpath" "$configname" "$configpath" "$processdir" "$groupdir"