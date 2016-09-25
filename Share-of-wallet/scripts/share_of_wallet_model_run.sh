#!/bin/bash

# Linux script to run the Share-of-Wallet R scripts.
# v1.0 10/1/16

# set the working directory
WORKING_DIR="/home/ubuntu/datascience-share-of-wallet/Development/src/"
DATE=$(date +%d/%m/%y)
START_TIME=$(date +%s)

# running the main batch script
echo "Share of Wallet Model Automation v1.0 at $DATE"
echo "##########################################################"
Rscript $WORKING_DIR"Share_of_Wallet_Init.R"

echo
# running the S3 connection script
echo "1. Running S3 connect.R"
echo "#############################"
Rscript $WORKING_DIR"S3_Connect.R"

echo
#Runing the the account level feature extraction
echo "2. Running account-level feature extraction"
echo "###########################################"
Rscript ${WORKING_DIR}"Feature_Extraction_account_level.R"
#Rscript ${WORKING_DIR}"test.R"

echo
#Running the customer level feature extraction
echo "3. Running customer-level feature extraction"
echo "############################################"
Rscript ${WORKING_DIR}"Feature_Extraction_customer_level.R"


echo
#Reducing the feature set
echo "4. Feature redcution exercise"
echo "#############################"
Rscript ${WORKING_DIR}"Feature_Reduction.R"

echo
# Apply the rule base
echo "5. Applying the Rule base"
echo "#########################"
Rscript ${WORKING_DIR}"Rule_Base.R"


echo
# Run the output model to the data
echo "6. Output predicted model"
echo "#########################"
Rscript ${WORKING_DIR}"Share_of_Wallet_Output.R"

# calculate the run time in minutes
END_TIME=$(date +%s)
RUN_TIME=$((END_TIME-START_TIME))

echo
echo "Share-of-Wallet Model completed in $RUN_TIME seconds"

