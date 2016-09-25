#!/bin/bash

# Linux script to run the Churn Model R scripts.
# v1.0 10/1/16

# set the working directory
WORKING_DIR="/home/ubuntu/churn_model/Production/src/"
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

# running the main batch script
echo "Churn Model Automation v1.0 in $ARG at $DATE"
echo "##########################################################"
Rscript $WORKING_DIR"Churn_Model_Init_Batch_File_v2.0.R"

echo
# running the S3 connection script
echo "1. Running S3 connect.R"
echo "#############################"
Rscript $WORKING_DIR"S3_Connect_Prod_v1.1.R"

echo
#Runing the the account level feature extraction
echo "2. Running account-level feature extraction"
echo "###########################################"
Rscript ${WORKING_DIR}"Feature_Extraction_account_level_Prod_v2.0.R"
#Rscript ${WORKING_DIR}"test.R"

echo
#Running the customer level feature extraction
echo "3. Running customer-level feature extraction"
echo "############################################"
Rscript ${WORKING_DIR}"Feature_Extraction_customer_level_Prod_v2.1.R"

echo
# Check for whether to retrain the complete algorithm or just apply the algorithm to the data 

if [ "$ARG" ==  "TRAIN_AND_APPLY" ] 
then
	echo "4. Running algoritm preparation"
	echo "###############################" 
	Rscript ${WORKING_DIR}"Algorithm_Preparation_Prod_v2.0.R"
	
	echo
	echo "5. Running the C5.0 algorithm"
	echo "#############################"
	Rscript ${WORKING_DIR}"C5.0_Algorithm_Prod_v2.0.R"
fi 

echo
# Run the output model to the data
echo "6. Output predicted model"
echo "#########################"
Rscript ${WORKING_DIR}"Churn_Model_Output_Prod_v2.1.R"

# calculate the run time in minutes
END_TIME=$(date +%s)
RUN_TIME=$((END_TIME-START_TIME))

echo
echo "Churn model automation completed in $RUN_TIME seconds"

