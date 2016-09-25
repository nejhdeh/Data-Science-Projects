# R script to perform the low-level acount -level (account_number) feature extraction 
# v1.0 04_11_15
#load necessary libraries
workingDir <- "/home/ubuntu/datascience-share-of-wallet/Development/"

# check for the R workspacce and load
if(file.exists(paste(workingDir,"results/R_workspace/share_of_wallet_workspace.RData", sep="")))
    load(paste(workingDir,"results/R_workspace/share_of_wallet_workspace.RData", sep=""))

suppressMessages(library(plyr))
suppressMessages(library(data.table))
suppressMessages(library(e1071))

#temprarily turn off warnings, since we may get infinity calculations
options(warn = -1)

#perform some date manupilation

################################
#set a cut-off date & other date constants
latest_date             <- as.Date(max(account_transactions$transaction_date, na.rm = TRUE))
last_7_days             <- 7
last_14_days            <- 14
last_28_days            <- 28
last_3_months           <- 90
last_6_months           <- 180
last_12_months          <- 365

# subset the data into deposits & withdrawals
#############################################
acc_trans_deposits      <- subset(account_transactions, transaction_type_id == 4, select = c(account_number, transaction_date, transaction_amount))
acc_trans_withdrawals   <- subset(account_transactions, transaction_type_id ==29, select = c(account_number, transaction_date, transaction_amount))

#set a keys for account_number aggregation
setkey(acc_trans_deposits,      "account_number")
setkey(acc_trans_withdrawals,   "account_number")

# get the latest date available


cat("calculating account level attributes...\n")
#now calculate the required attributes:
#deposit attributes
###########################################################
acc_trans_deposits_attributes <- acc_trans_deposits[, list( 
                                  deposits_last_28_days           = length(transaction_amount[transaction_date >= (latest_date - last_28_days)]),
                                  deposits_last_3_months          = length(transaction_amount[transaction_date >= (latest_date - last_3_months)]),
                                  deposits_last_6_months          = length(transaction_amount[transaction_date >= (latest_date - last_6_months)]),

                                  
                                  dep_sum_last_28_days            = sum(transaction_amount[transaction_date >= (latest_date - last_28_days)], na.rm=TRUE), 
                                  dep_sum_last_3_months           = sum(transaction_amount[transaction_date >= (latest_date - last_3_months)], na.rm=TRUE), 
                                  dep_sum_last_6_months           = sum(transaction_amount[transaction_date >= (latest_date - last_6_months)], na.rm=TRUE),                            
                                  
                                  dep_median_last_28_days         = median(transaction_amount[transaction_date >= (latest_date - last_28_days)], na.rm=TRUE),
                                  dep_median_last_3_months        = median(transaction_amount[transaction_date >= (latest_date - last_3_months)], na.rm=TRUE),
                                  dep_median_last_6_months        = median(transaction_amount[transaction_date >= (latest_date - last_6_months)], na.rm=TRUE),
                                  

                                  dep_gradient_last_28_days       = if(length(transaction_date[transaction_date >= (latest_date - last_28_days)]) == 0)
                                                                        {as.numeric(NA)}
                                                                    else
                                                                        {lm_gradient(transaction_amount[transaction_date >= (latest_date - last_28_days)], transaction_date[transaction_date >= (latest_date - last_28_days)])},
                                  
                                  dep_gradient_last_3_months      = if(length(transaction_date[transaction_date >= (latest_date - last_3_months)]) == 0)
                                                                        {as.numeric(NA)}
                                                                    else
                                                                        {lm_gradient(transaction_amount[transaction_date >= (latest_date - last_3_months)], transaction_date[transaction_date >= (latest_date - last_3_months)])},
                                  
                                  dep_gradient_last_6_months      = if(length(transaction_date[transaction_date >= (latest_date - last_6_months)]) == 0)
                                                                        {as.numeric(NA)}
                                                                    else
                                                                        {lm_gradient(transaction_amount[transaction_date >= (latest_date - last_6_months)], transaction_date[transaction_date >= (latest_date - last_6_months)])},
                                                                        
                                  
                                  days_between_dep_last_28_days   = if(length(transaction_date[transaction_date >= (latest_date - last_28_days)]) == 0)
                                                                        {as.numeric(NA)}
                                                                    else
                                                                        {as.numeric(median(diff(sort(transaction_date[transaction_date >= (latest_date - last_28_days)]),1), na.rm=TRUE))},
                                  
                                  days_between_dep_last_3_months  = if(length(transaction_date[transaction_date >= (latest_date - last_3_months)]) == 0)
                                                                    {as.numeric(NA)}
                                                                        else
                                                                    {as.numeric(median(diff(sort(transaction_date[transaction_date >= (latest_date - last_3_months)]),1), na.rm=TRUE))},
                                  
                                  days_between_dep_last_6_months  = if(length(transaction_date[transaction_date >= (latest_date - last_6_months)]) == 0)
                                                                        {as.numeric(NA)}
                                                                    else
                                                                        {as.numeric(median(diff(sort(transaction_date[transaction_date >= (latest_date - last_6_months)]),1), na.rm=TRUE))},    
                                  
                                                                   
                                  days_between_dep_variance       = CV_normalised(as.numeric(diff(sort(transaction_date),1))), 
                                  
                                   
                                  days_since_last_dep             = if(length(transaction_date[transaction_date >= (latest_date - last_12_months)]) == 0)
                                                                        {as.numeric(NA)}
                                                                    else
                                                                        {as.numeric(latest_date - max(transaction_date, na.rm=TRUE))}),
                                  
                                  by=key(acc_trans_deposits)]

cat("calculating withdrawal profile features...\n")
#withdrawals attributes
###########################################################
acc_trans_withdrawals_attributes <- acc_trans_withdrawals[, list(
                               
                                withdrawals_last_28_days        = length(transaction_amount[transaction_date >= (latest_date - last_28_days)]),
                                withdrawals_last_3_months       = length(transaction_amount[transaction_date >= (latest_date - last_3_months)]),
                                withdrawals_last_6_months       = length(transaction_amount[transaction_date >= (latest_date - last_6_months)]),
                                
                                with_sum_last_28_days           = sum(transaction_amount[transaction_date >= (latest_date - last_28_days)], na.rm=TRUE), 
                                with_sum_last_3_months          = sum(transaction_amount[transaction_date >= (latest_date - last_3_months)], na.rm=TRUE), 
                                with_sum_last_6_months          = sum(transaction_amount[transaction_date >= (latest_date - last_6_months)], na.rm=TRUE), 
                                
                                with_median_last_28_days        = median(transaction_amount[transaction_date >= (latest_date - last_28_days)], na.rm=TRUE),
                                with_median_last_3_months       = median(transaction_amount[transaction_date >= (latest_date - last_3_months)], na.rm=TRUE),
                                with_median_last_6_months       = median(transaction_amount[transaction_date >= (latest_date - last_6_months)], na.rm=TRUE),
                                                                
                                
                                with_gradient_last_28_days      = if(length(transaction_date[transaction_date >= (latest_date - last_28_days)]) == 0)
                                                                      {as.numeric(NA)}
                                                                  else
                                                                      {lm_gradient(transaction_amount[transaction_date >= (latest_date - last_28_days)], transaction_date[transaction_date >= (latest_date - last_28_days)])},
                                
                                with_gradient_last_3_months     = if(length(transaction_date[transaction_date >= (latest_date - last_3_months)]) == 0)
                                                                      {as.numeric(NA)}
                                                                  else
                                                                      {lm_gradient(transaction_amount[transaction_date >= (latest_date - last_3_months)], transaction_date[transaction_date >= (latest_date - last_3_months)])},
                                
                                with_gradient_last_6_months     = if(length(transaction_date[transaction_date >= (latest_date - last_6_months)]) == 0)
                                                                      {as.numeric(NA)}
                                                                  else
                                                                      {lm_gradient(transaction_amount[transaction_date >= (latest_date - last_6_months)], transaction_date[transaction_date >= (latest_date - last_6_months)])},
                                
                                                
                             
                                days_between_with_last_28_days  = if(length(transaction_date[transaction_date >= (latest_date - last_28_days)]) == 0)
                                                                      {as.numeric(NA)}
                                                                  else
                                                                      {as.numeric(median(diff(sort(transaction_date[transaction_date >= (latest_date - last_28_days)]),1), na.rm=TRUE))},
                                
                                days_between_with_last_3_months = if(length(transaction_date[transaction_date >= (latest_date - last_3_months)]) == 0)
                                                                      {as.numeric(NA)}
                                                                  else
                                                                      {as.numeric(median(diff(sort(transaction_date[transaction_date >= (latest_date - last_3_months)]),1), na.rm=TRUE))},
                                
                                days_between_with_last_6_months = if(length(transaction_date[transaction_date >= (latest_date - last_6_months)]) == 0)
                                                                      {as.numeric(NA)}
                                                                  else
                                                                      {as.numeric(median(diff(sort(transaction_date[transaction_date >= (latest_date - last_6_months)]),1), na.rm=TRUE))},    
                                
                                                                
                                days_between_with_variance      = CV_normalised(as.numeric(diff(sort(transaction_date),1))),
                                
                                days_since_last_with            = if(length(transaction_date[transaction_date >= (latest_date - last_12_months)]) == 0)
                                                                        {as.numeric(NA)}
                                                                    else
                                                                        {as.numeric(latest_date - max(transaction_date, na.rm=TRUE))}),
                                
                                by=key(acc_trans_withdrawals)]



#Merge all tables to create the account transaction master table
account_transaction_attributes <- merge(acc_trans_deposits_attributes,acc_trans_withdrawals_attributes, by="account_number", all=TRUE )

#remove some tables and unwanted temprary variables
rm(acc_trans_deposits, acc_trans_deposits_attributes, acc_trans_withdrawals, acc_trans_withdrawals_attributes, account_transactions)

#turn warnings back on
options(warn = 0)

# save image
save.image(paste(workingDir,"results/R_workspace/share_of_wallet_workspace.RData", sep=""))
