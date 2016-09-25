#!/bin/bash

# Linux script to run the complete share of wallet model - from attribute table to complete writing of results back to Redshift  
# v1.0 03-05-16

# set the working directory
WORKING_DIR="/home/ubuntu/datascience-share-of-wallet/Development/scripts/"
DATE=$(date +%d/%m/%y)
START_TIME=$(date +%s)

# 1. Running the customer attributes update script
##################################################
$WORKING_DIR"./customer_attributes_update.sh"

echo
# 2. Running the algorithm - using R
####################################
# paases the command line to the script
$WORKING_DIR"./share_of_wallet_run.sh" 


echo
# 3. Update the results of the churn model attributes and the predictions to redshift
$WORKIN_DIR"./share_of_wallet_redshift_update.sh" 


RUN_TIME=$((END_TIME-START_TIME))

echo
echo "Share of Wallet automation completed in $RUN_TIME seconds"

