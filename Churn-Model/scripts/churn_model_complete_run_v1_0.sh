#!/bin/bash

# Linux script to run the complete churn model - from attribute table to complete writing of results back to Redshift  
# v1.0 10/2/16

# set the working directory
WORKING_DIR="/home/ubuntu/churn_model/Production/scripts/"
DATE=$(date +%d/%m/%y)
START_TIME=$(date +%s)

# Checking for correct argument

if [ "$1" != "train_and_apply" ] && [ "$1" != "apply_only" ] 
then 
	echo "incorrect argument supplied. Provide train_and_apply or apply_only"
	exit 1
fi

# get the argument & convert to uppercase
ARG="${1^^}"
echo

# 1. Running the customer attributes update script
##################################################
$WORKING_DIR"./customer_attributes_update_v1_0.sh"

echo
# 2. Running the algorithm - using R
####################################
# paases the command line to the script
$WORKING_DIR"./churn_model_run_v1_0.sh" $1


echo
# 3. Update the results of the churn model attributes and the predictions to redshift
$WORKIN_DIR"./churn_model_redshift_update_v1_0.sh" 


RUN_TIME=$((END_TIME-START_TIME))

echo
echo "Churn model automation completed in $RUN_TIME seconds"

