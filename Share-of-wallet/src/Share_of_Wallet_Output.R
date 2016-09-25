#R script to write results of the Churn model(s) output to the master data model table
#v2.1 07/03/16
workingDir <- "/home/ubuntu/datascience-share-of-wallet/Development/"

if(file.exists(paste(workingDir,"results/R_workspace/share_of_wallet_workspace.RData", sep="")))
    load(paste(workingDir,"results/R_workspace/share_of_wallet_workspace.RData", sep=""))


###########################################
# OUTPUT RESULTS
###########################################

# 1. Write results to the Linux EC2
###################################
cat('writing the final results & table to AWS Linux EC2 & S3 tabcorp-share-of-wallet-automation bucket...\n')

# write the SoW classfied table to local dir on EC2
write.table(customer_SoW_attributes_classified, paste(workingDir,"results/customer_SoW_attributes_classified.csv", sep=""), quote=FALSE,sep=",",row.names=FALSE,col.names=TRUE,append=FALSE,na="")


# 2. Write results to the S3 tabcorp-share-of-wallet-automation bucket
######################################################################
# The attributes table
S3_write_string <- sprintf("aws s3 cp %s s3://tabcorp-share-of-wallet-automation/output/customer_SoW_attributes_classified.csv --region ap-southeast-2",paste(workingDir,"results/customer_SoW_attributes_classified.csv", sep=""))

#now linux system call 
system(S3_write_string, ignore.stdout = TRUE )

#remove some tables and unwanted temprary variables
rm(S3_write_string)


# save image
save.image(paste(workingDir,"results/R_workspace/share_of_wallet_workspace.RData", sep=""))  

#save(c5.0_fit3, c5.0_fit3_predict_summary, file = paste(workingDir,"results/R_workspace/churn_model.RData", sep=""))
