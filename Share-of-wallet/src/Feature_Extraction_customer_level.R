# An R script to perform feaure extraction based on the customer-level
#Also uses data from the account level and further attributes by the bet-level
# N.G 05-05-16

workingDir <- "/home/ubuntu/datascience-share-of-wallet/Development/"

# check for the R workspacce and load
if(file.exists(paste(workingDir,"results/R_workspace/share_of_wallet_workspace.RData", sep="")))
    load(paste(workingDir,"results/R_workspace/share_of_wallet_workspace.RData", sep=""))

suppressMessages(library(plyr))
suppressMessages(library(data.table))
suppressMessages(library(e1071))
#temprarily turn off warnings, since we may get infinity calculations
options(warn = -1)

#perform a merge between the customer_acount & the account_transaction_attributes table prior to aggregation
customer_account_all <- data.table(merge(customer_account,account_transaction_attributes, by="account_number", all.x=TRUE ))

#set a keys for customer_number aggregation
setkey(customer_account_all, "customer_number")

#Now aggregate the table to the customer level for further analysis
###################################################################

cat('Performing customer attributes selection ...\n\n')
#aggregation
############
customer_SoW_attributes_general <- customer_account_all[, list(
                                            
                                                    #reward points
                                                    stsp_opening_balance            = sum(stsp_opening_balance, na.rm=TRUE),
                                                    stsp_earned                     = sum(stsp_earned, na.rm=TRUE),
                                                    stsp_expired                    = sum(stsp_expired, na.rm=TRUE),
                                                    stsp_closing_balance            = sum(stsp_closing_balance, na.rm=TRUE),
                                                    rwdp_opening_balance            = sum(rwdp_opening_balance, na.rm=TRUE),
                                                    rwdp_earned                     = sum(rwdp_earned, na.rm=TRUE),
                                                    rwdp_redeemed                   = sum(rwdp_redeemed, na.rm=TRUE),
                                                    rwdp_expired                    = sum(rwdp_expired, na.rm=TRUE),
                                                    rwdp_expired_predicted          = sum(rwdp_expired_predicted, na.rm=TRUE),
                                                    rwdp_forfeited                  = sum(rwdp_forfeited, na.rm=TRUE),
                                                    rwdp_closing_balance            = sum(rwdp_closing_balance,na.rm=TRUE),
                                                    
                                                
                                                    days_since_last_bet             = as.numeric(min(latest_date - as.Date(as.character(last_bet_date), format="%Y-%m-%d"), na.rm=TRUE)), 

                                                    #account info
                                                                                            
                                                    balance_last_14_days            = sum(credit_card_balance[ as.Date(last_bet_date) >= (latest_date - last_14_days)], na.rm=TRUE) +
                                                                                      sum(cash_balance[as.Date(last_bet_date) >= (latest_date - last_14_days)], na.rm=TRUE) + 
                                                                                      sum(cheque_balance[as.Date(last_bet_date) >= (latest_date - last_14_days)], na.rm=TRUE),
                                                    
                                                    balance_last_28_days            = sum(credit_card_balance[ as.Date(last_bet_date) >= (latest_date - last_28_days)], na.rm=TRUE) +
                                                                                      sum(cash_balance[as.Date(last_bet_date) >= (latest_date - last_28_days)], na.rm=TRUE) + 
                                                                                      sum(cheque_balance[as.Date(last_bet_date) >= (latest_date - last_28_days)], na.rm=TRUE),
                                                    
                                                    balance_last_3_months           = sum(credit_card_balance[ as.Date(last_bet_date) >= (latest_date - last_3_months)], na.rm=TRUE) +
                                                                                      sum(cash_balance[as.Date(last_bet_date) >= (latest_date - last_3_months)], na.rm=TRUE) + 
                                                                                      sum(cheque_balance[as.Date(last_bet_date) >= (latest_date - last_3_months)], na.rm=TRUE),
                                                    
                                                    balance_last_6_months           = sum(credit_card_balance[ as.Date(last_bet_date) >= (latest_date - last_6_months)], na.rm=TRUE) +
                                                                                      sum(cash_balance[as.Date(last_bet_date) >= (latest_date - last_6_months)], na.rm=TRUE) + 
                                                                                      sum(cheque_balance[as.Date(last_bet_date) >= (latest_date - last_6_months)], na.rm=TRUE),
                                                    
                                                     
                                                    
                                                    balance_ratio_last_14_days_28_days = (last_28_days/last_14_days)* ((sum(credit_card_balance[ as.Date(last_bet_date) >= (latest_date - last_14_days)], na.rm=TRUE) +
                                                                                                                      sum(cash_balance[as.Date(last_bet_date) >= (latest_date - last_14_days)], na.rm=TRUE) + 
                                                                                                                      sum(cheque_balance[as.Date(last_bet_date) >= (latest_date - last_14_days)], na.rm=TRUE)) /  
                                                    
                                                                                                                      (sum(credit_card_balance[ as.Date(last_bet_date) >= (latest_date - last_28_days)], na.rm=TRUE) +
                                                                                                                      sum(cash_balance[as.Date(last_bet_date) >= (latest_date - last_28_days)], na.rm=TRUE) + 
                                                                                                                      sum(cheque_balance[as.Date(last_bet_date) >= (latest_date - last_28_days)], na.rm=TRUE))),
                                                    
                                                    
                                                    balance_ratio_last_28_days_3_months= (last_3_months/last_28_days) *((sum(credit_card_balance[ as.Date(last_bet_date) >= (latest_date - last_28_days)], na.rm=TRUE) +
                                                                                                                      sum(cash_balance[as.Date(last_bet_date) >= (latest_date - last_28_days)], na.rm=TRUE) + 
                                                                                                                      sum(cheque_balance[as.Date(last_bet_date) >= (latest_date - last_28_days)], na.rm=TRUE)) / 
                                                      
                                                                                                                      (sum(credit_card_balance[ as.Date(last_bet_date) >= (latest_date - last_3_months)], na.rm=TRUE) +
                                                                                                                      sum(cash_balance[as.Date(last_bet_date) >= (latest_date - last_3_months)], na.rm=TRUE) + 
                                                                                                                      sum(cheque_balance[as.Date(last_bet_date) >= (latest_date - last_3_months)], na.rm=TRUE))),
                                                    
                                                    
                                                    balance_ratio_last_28_days_6_months= (last_6_months/last_28_days) *((sum(credit_card_balance[ as.Date(last_bet_date) >= (latest_date - last_28_days)], na.rm=TRUE) +
                                                                                                                      sum(cash_balance[as.Date(last_bet_date) >= (latest_date - last_28_days)], na.rm=TRUE) + 
                                                                                                                      sum(cheque_balance[as.Date(last_bet_date) >= (latest_date - last_28_days)], na.rm=TRUE)) / 
                                                      
                                                                                                                      (sum(credit_card_balance[ as.Date(last_bet_date) >= (latest_date - last_6_months)], na.rm=TRUE) +
                                                                                                                      sum(cash_balance[as.Date(last_bet_date) >= (latest_date - last_6_months)], na.rm=TRUE) + 
                                                                                                                      sum(cheque_balance[as.Date(last_bet_date) >= (latest_date - last_6_months)], na.rm=TRUE))),
                                                    
                                                    
                                                    #account transactional data
                                                    deposits_last_28_days                           = sum(deposits_last_28_days, na.rm=TRUE),
                                                    deposits_last_3_months                          = sum(deposits_last_3_months, na.rm=TRUE),
                                                    deposits_last_6_months                          = sum(deposits_last_6_months, na.rm=TRUE),

                                                    
                                                   
                                                    deposits_ratio_last_28_days_3_months            = (last_3_months/last_28_days) *(as.numeric(sum(deposits_last_28_days, na.rm=TRUE)/sum(deposits_last_3_months, na.rm=TRUE))),
                                                    deposits_ratio_last_28_days_6_months            = (last_6_months/last_28_days) *(as.numeric(sum(deposits_last_28_days, na.rm=TRUE)/sum(deposits_last_6_months, na.rm=TRUE))),
                                                    
                                                    
                                                    
                                                    deposits_sum_last_28_days                       = sum(dep_sum_last_28_days, na.rm=TRUE),
                                                    deposits_sum_last_3_months                      = sum(dep_sum_last_3_months, na.rm=TRUE),
                                                    deposits_sum_last_6_months                      = sum(dep_sum_last_6_months, na.rm=TRUE),
                                                   
                                                    deposits_sum_last_28_days_3_months              = (last_3_months/last_28_days) *(as.numeric(sum(dep_sum_last_28_days, na.rm=TRUE)/sum(dep_sum_last_3_months, na.rm=TRUE))),
                                                    deposits_sum_last_28_days_6_months              = (last_6_months/last_28_days) *(as.numeric(sum(dep_sum_last_28_days, na.rm=TRUE)/sum(dep_sum_last_6_months, na.rm=TRUE))),
                                                    
                                                    
                                                    
                                                    deposit_avg_last_28_days                        = mean(dep_median_last_28_days, na.rm=TRUE),
                                                    deposit_avg_last_3_months                       = mean(dep_median_last_3_months, na.rm=TRUE),
                                                    deposit_avg_last_6_months                       = mean(dep_median_last_6_months, na.rm=TRUE),
                                                   
                                                    deposit_avg_ratio_last_28_days_3_months         = as.numeric(mean(dep_median_last_28_days, na.rm=TRUE)/mean(dep_median_last_3_months, na.rm=TRUE)),
                                                    deposit_avg_ratio_last_28_days_6_months         = as.numeric(mean(dep_median_last_28_days, na.rm=TRUE)/mean(dep_median_last_6_months, na.rm=TRUE)),
                                                    
                                                                                                        
                                                    
                                                    deposit_gradient_last_28_days                   = mean(dep_gradient_last_28_days, na.rm=TRUE),
                                                    deposit_gradient_last_3_months                  = mean(dep_gradient_last_3_months, na.rm=TRUE),
                                                    deposit_gradient_last_6_months                  = mean(dep_gradient_last_6_months, na.rm=TRUE),
                                                    
                                                    deposit_gradient_ratio_last_28_days_6_months    = as.numeric(mean(dep_gradient_last_28_days/dep_gradient_last_6_months, na.rm=TRUE)),
                                                   
                                                    
                                                    days_between_deposits_last_28_days              = as.numeric(median(days_between_dep_last_28_days, na.rm=TRUE)),
                                                    days_between_deposits_last_3_months             = as.numeric(median(days_between_dep_last_3_months, na.rm=TRUE)),
                                                    days_between_deposits_last_6_months             = as.numeric(median(days_between_dep_last_6_months, na.rm=TRUE)),
                                                    
                                                    days_between_deposits_ratio_last_28_days_3_months   = median(as.numeric(days_between_dep_last_28_days)/as.numeric(days_between_dep_last_3_months, na.rm=TRUE)),
                                                    days_between_deposits_ratio_last_28_days_6_months   = median(as.numeric(days_between_dep_last_28_days)/as.numeric(days_between_dep_last_6_months, na.rm=TRUE)),
                                                    
                                                    
                                                    days_between_deposits_var                       = mean(days_between_dep_variance, na.rm=TRUE),
                                                    days_since_last_deposit                         = as.numeric(median(days_since_last_dep, na.rm=TRUE)),
                                                    

                                                    
                                                    withdrawals_last_28_days                        = sum(withdrawals_last_28_days, na.rm=TRUE),
                                                    withdrawals_last_3_months                       = sum(withdrawals_last_3_months, na.rm=TRUE),
                                                    withdrawals_last_6_months                       = sum(withdrawals_last_6_months, na.rm=TRUE),
                                                    
                                                    
                                                    withdrawals_ratio_last_28_days_3_months         = (last_3_months/last_28_days) *(as.numeric(sum(withdrawals_last_28_days, na.rm=TRUE)/sum(withdrawals_last_3_months, na.rm=TRUE))),
                                                    withdrawals_ratio_last_28_days_6_months         = (last_6_months/last_28_days) *(as.numeric(sum(withdrawals_last_28_days, na.rm=TRUE)/sum(withdrawals_last_6_months, na.rm=TRUE))),
 
                                                    withdrawals_sum_last_28_days                    = sum(with_sum_last_28_days, na.rm=TRUE),
                                                    withdrawals_sum_last_3_months                   = sum(with_sum_last_3_months, na.rm=TRUE),
                                                    withdrawals_sum_last_6_months                   = sum(with_sum_last_6_months, na.rm=TRUE),
                                                    
                                                    withdrawals_sum_last_28_days_3_months           = (last_3_months/last_28_days) *(as.numeric(sum(with_sum_last_28_days, na.rm=TRUE)/sum(with_sum_last_3_months, na.rm=TRUE))),
                                                    withdrawals_sum_last_28_days_6_months           = (last_6_months/last_28_days) *(as.numeric(sum(with_sum_last_28_days, na.rm=TRUE)/sum(with_sum_last_6_months, na.rm=TRUE))),
                                                                                                       
                                                    withdrawal_avg_last_28_days                     = mean(with_median_last_28_days, na.rm=TRUE),
                                                    withdrawal_avg_last_3_months                    = mean(with_median_last_3_months, na.rm=TRUE),
                                                    withdrawal_avg_last_6_months                    = mean(with_median_last_6_months, na.rm=TRUE),
                                                    
                                                    withdrawal_avg_ratio_last_28_days_3_months      = as.numeric(mean(with_median_last_28_days, na.rm=TRUE)/mean(with_median_last_3_months, na.rm=TRUE)),
                                                    withdrawal_avg_ratio_last_28_days_6_months      = as.numeric(mean(with_median_last_28_days, na.rm=TRUE)/mean(with_median_last_6_months, na.rm=TRUE)),
                                                    
                                                   
                                                    
                                                    withdrawal_gradient_last_28_days                = mean(with_gradient_last_28_days, na.rm=TRUE),
                                                    withdrawal_gradient_last_3_months               = mean(with_gradient_last_3_months, na.rm=TRUE),
                                                    withdrawal_gradient_last_6_months               = mean(with_gradient_last_6_months, na.rm=TRUE),
                                                   
                                                    withdrawal_gradient_ratio_last_28_days_6_months = as.numeric(mean(with_gradient_last_28_days/with_gradient_last_6_months, na.rm=TRUE)),
                                                    
                                                    
                                                    days_between_withdrawals_last_28_days           = as.numeric(median(days_between_with_last_28_days, na.rm=TRUE)),
                                                    days_between_withdrawals_last_3_months          = as.numeric(median(days_between_with_last_3_months, na.rm=TRUE)),
                                                    days_between_withdrawals_last_6_months          = as.numeric(median(days_between_with_last_6_months, na.rm=TRUE)),
                                                    
                                                    
                                                    
                                                    days_between_withdrawals_var                    = mean(days_between_with_variance, na.rm=TRUE),
                                                    days_since_last_withdrawal                      = as.numeric(median(days_since_last_with, na.rm=TRUE)),
                                                    
                                                    
                                                    #additional attributes
                                                    withdrawal_deposit_ratio_last_28_days           = sum(withdrawals_last_28_days, na.rm=TRUE) / sum(deposits_last_28_days, na.rm=TRUE), 
                                                    withdrawal_deposit_ratio_last_3_months          = sum(withdrawals_last_3_months, na.rm=TRUE) / sum(deposits_last_3_months, na.rm=TRUE), 
                                                    withdrawal_deposit_ratio_last_6_months          = sum(withdrawals_last_6_months, na.rm=TRUE) / sum(deposits_last_6_months, na.rm=TRUE)), 
                                                        
                                                    
                                                    by=key(customer_account_all)]



# cleaning up data to remove infinities for glm (versioned up)
customer_SoW_attributes_general[mapply(is.na, customer_SoW_attributes_general)]         <- NA
customer_SoW_attributes_general[mapply(is.infinite, customer_SoW_attributes_general)]   <- NA

#merge with other attribute tables as section1, section2, section3, linear regression and monthly calcs
# Do the Monthly calc with linear regression first
#######################################################################################################
customer_SoW_attributes <- merge(customer_attributes_monthly_calcs, customer_attributes_linear_reg, by="customer_number", all.x=TRUE )
customer_SoW_attributes <- merge(customer_SoW_attributes,customer_SoW_attributes_general, by="customer_number", all.x=TRUE )
customer_SoW_attributes <- merge(customer_SoW_attributes,customer_attributes_section1, by="customer_number", all.x=TRUE )
customer_SoW_attributes <- merge(customer_SoW_attributes,customer_attributes_section2, by="customer_number", all.x=TRUE )
customer_SoW_attributes <- merge(customer_SoW_attributes,customer_attributes_section3, by="customer_number", all.x=TRUE )

#further cleansing
customer_SoW_attributes[mapply(is.na, customer_SoW_attributes)]         <- NA
customer_SoW_attributes[mapply(is.infinite, customer_SoW_attributes)]   <- NA


#after these mergers change the target variables into factors
customer_SoW_attributes$churn_1_days_between_bets     <- as.factor(customer_SoW_attributes$churn_1_days_between_bets)
customer_SoW_attributes$churn_2_days_since_last_bet   <- as.factor(customer_SoW_attributes$churn_2_days_since_last_bet)
customer_SoW_attributes$churn_3_days_between_bets     <- as.factor(customer_SoW_attributes$churn_3_days_between_bets)



#remove some tables and unwanted temprary variables
rm(account_transaction_attributes, customer_account, customer_account_all, customer_attributes_section1, customer_attributes_section2,customer_attributes_section3, customer_attributes_linear_reg, customer_attributes_monthly_calcs, customer_SoW_attributes_general)

#turn warnings back on
options(warn = 0)

# save image
save.image(paste(workingDir,"results/R_workspace/share_of_wallet_workspace.RData", sep=""))
