# An R script to perform feaure extraction based on the customer-level
#Also uses data from the account level and further attributes by the bet-level
# v1.0 15/11/15
workingDir <- "/home/ubuntu/churn_model/Production/"
load(paste(workingDir,"results/R_workspace/churn_model_workspace.RData", sep=""))

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
customer_attributes_all <- customer_account_all[, list(
                                                    #demographical
                                                    state                           = as.factor(MaxOccurance(state)),
                                                    #postcode                        = as.factor(MaxOccurance(postcode)),
                                                    gender                          = as.factor(MaxOccurance(gender)),
                                                    dob                             = as.factor(MaxOccurance(dob)),
                                                    
                                                    #materials received
                                                    no_3rd_party_mail               = as.factor(any(no_3rd_party_mail)),
                                                    no_tab_mail                     = as.factor(any(no_tab_mail)),
                                                    document_provided               = as.factor(any(document_provided)),
                                                    tab_member_scheme_opted_out     = as.factor(any(tab_member_scheme_opted_out)),
                                                    market_research_opted_out       = as.factor(any(market_research_opted_out)),
                                                    online_id_verified              = as.factor(any(online_id_verified)),
                                                    
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
                                                    acc_frozen_reason_code          = as.factor(unique(sapply((acc_frozen_reason_code),function(x) {paste0(acc_frozen_reason_code, collapse='.')}))),
                                                    acc_status_code                 = as.factor(unique(sapply((acc_status_code),function(x) {paste0(acc_status_code, collapse='.')}))),
                                                    acc_wdl_block_reason_code       = as.factor(unique(sapply((acc_wdl_block_reason_code),function(x) {paste0(acc_wdl_block_reason_code, collapse='.')}))),
                                                    frozen                          = as.factor(any(frozen)),
                                                    internet_betting_activated      = as.factor(any(internet_betting_activated)),
                                                
                                                    days_since_last_bet             = as.numeric(min(latest_date - as.Date(as.character(last_bet_date), format="%Y-%m-%d"), na.rm=TRUE)), ##

                                                    #account info
                                                    closure_reason_code             = as.factor(unique(sapply((closure_reason_code),function(x) {paste0(closure_reason_code, collapse='.')}))),
                                                    security_blocked                = as.factor(any(security_blocked)),
                                                    withdrawal_blocked              = as.factor(any(withdrawal_blocked)),
                                                    acc_closed_ratio                = length(acc_closed[acc_closed == TRUE]) / length(acc_closed),
                                                    last_closed_account             = as.numeric(latest_date - max(as.Date(as.character(acc_closed_date[acc_closed == TRUE]), format="%Y-%m-%d"), na.rm=TRUE)),
                                                    credit_card_balance             = sum(credit_card_balance,na.rm=TRUE),
                                                    cash_balance                    = sum(cash_balance, na.rm=TRUE),
                                                    cheque_balance                  = sum(cheque_balance, na.rm=TRUE),
                                                    pending_eft_balance             = sum(pending_eft_balance, na.rm=TRUE),
                                                    promotion_balance               = sum(promotion_balance, na.rm=TRUE),
                                                    
                                                    #account transactional data
                                                    deposits                        = sum(deposits, na.rm=TRUE),
                                                    deposits_last_7_days            = sum(deposits_last_7_days, na.rm=TRUE),
                                                    deposits_last_14_days           = sum(deposits_last_14_days, na.rm=TRUE),
                                                    deposits_last_28_days           = sum(deposits_last_28_days, na.rm=TRUE),
                                                    deposits_last_3_months          = sum(deposits_last_3_months, na.rm=TRUE),
                                                    deposits_last_6_months          = sum(deposits_last_6_months, na.rm=TRUE),
                                                    deposits_last_12_months         = sum(deposits_last_12_months, na.rm=TRUE),
                                                    deposits_ratio_last_7_days_28_days  = (last_28_days/last_7_days) * (as.numeric(sum(deposits_last_7_days, na.rm=TRUE)/sum(deposits_last_28_days, na.rm=TRUE))),
                                                    deposits_ratio_last_7_days_3_months = (last_3_months/last_7_days)* (as.numeric(sum(deposits_last_7_days, na.rm=TRUE)/sum(deposits_last_3_months, na.rm=TRUE))),
                                                    deposits_ratio_last_14_days_28_days = (last_28_days/last_14_days)* (as.numeric(sum(deposits_last_14_days, na.rm=TRUE)/sum(deposits_last_28_days, na.rm=TRUE))),
                                                    deposits_ratio_last_28_days_3_months= (last_3_months/last_28_days) *(as.numeric(sum(deposits_last_28_days, na.rm=TRUE)/sum(deposits_last_3_months, na.rm=TRUE))),
                                                    deposits_ratio_last_28_days_6_months= (last_6_months/last_28_days) *(as.numeric(sum(deposits_last_28_days, na.rm=TRUE)/sum(deposits_last_6_months, na.rm=TRUE))),
                                                    
                                                    deposits_sum                        = sum(dep_sum, na.rm=TRUE),
                                                    deposits_sum_last_7_days            = sum(dep_sum_last_7_days, na.rm=TRUE),
                                                    deposits_sum_last_14_days           = sum(dep_sum_last_14_days, na.rm=TRUE),
                                                    deposits_sum_last_28_days           = sum(dep_sum_last_28_days, na.rm=TRUE),
                                                    deposits_sum_last_3_months          = sum(dep_sum_last_3_months, na.rm=TRUE),
                                                    deposits_sum_last_6_months          = sum(dep_sum_last_6_months, na.rm=TRUE),
                                                    deposits_sum_last_12_months         = sum(dep_sum_last_12_months, na.rm=TRUE),
                                                    deposits_sum_last_7_days_28_days    = (last_28_days/last_7_days) * (as.numeric(sum(dep_sum_last_7_days, na.rm=TRUE)/sum(dep_sum_last_28_days, na.rm=TRUE))),
                                                    deposits_sum_last_7_days_3_months   = (last_3_months/last_7_days)* (as.numeric(sum(dep_sum_last_7_days, na.rm=TRUE)/sum(dep_sum_last_3_months, na.rm=TRUE))),
                                                    deposits_sum_last_14_days_28_days   = (last_28_days/last_14_days)* (as.numeric(sum(dep_sum_last_14_days, na.rm=TRUE)/sum(dep_sum_last_28_days, na.rm=TRUE))),
                                                    deposits_sum_last_28_days_3_months  = (last_3_months/last_28_days) *(as.numeric(sum(dep_sum_last_28_days, na.rm=TRUE)/sum(dep_sum_last_3_months, na.rm=TRUE))),
                                                    deposits_sum_last_28_days_6_months  = (last_6_months/last_28_days) *(as.numeric(sum(dep_sum_last_28_days, na.rm=TRUE)/sum(dep_sum_last_6_months, na.rm=TRUE))),
                                                    
                                                    
                                                    deposit_avg                     = mean(dep_median, na.rm=TRUE),
                                                    deposit_avg_last_7_days         = mean(dep_median_last_7_days, na.rm=TRUE),
                                                    deposit_avg_last_14_days        = mean(dep_median_last_14_days, na.rm=TRUE),
                                                    deposit_avg_last_28_days        = mean(dep_median_last_28_days, na.rm=TRUE),
                                                    deposit_avg_last_3_months       = mean(dep_median_last_3_months, na.rm=TRUE),
                                                    deposit_avg_last_6_months       = mean(dep_median_last_6_months, na.rm=TRUE),
                                                    deposit_avg_last_12_months      = mean(dep_median_last_12_months, na.rm=TRUE),
                                                    deposit_avg_ratio_last_7_days_28_days   = as.numeric(mean(dep_median_last_7_days, na.rm=TRUE)/mean(dep_median_last_28_days, na.rm=TRUE)),
                                                    deposit_avg_ratio_last_7_days_3_months  = as.numeric(mean(dep_median_last_7_days, na.rm=TRUE)/mean(dep_median_last_3_months, na.rm=TRUE)),
                                                    deposit_avg_ratio_last_14_days_28_days  = as.numeric(mean(dep_median_last_14_days, na.rm=TRUE)/mean(dep_median_last_28_days, na.rm=TRUE)),
                                                    deposit_avg_ratio_last_28_days_3_months = as.numeric(mean(dep_median_last_28_days, na.rm=TRUE)/mean(dep_median_last_3_months, na.rm=TRUE)),
                                                    deposit_avg_ratio_last_28_days_6_months = as.numeric(mean(dep_median_last_28_days, na.rm=TRUE)/mean(dep_median_last_6_months, na.rm=TRUE)),
                                                    
                                                    deposit_var                     = mean(dep_variance, na.rm=TRUE),
                                                    deposit_var_last_7_days         = mean(dep_variance_last_7_days, na.rm=TRUE),
                                                    deposit_var_last_14_days        = mean(dep_variance_last_14_days, na.rm=TRUE),
                                                    deposit_var_last_28_days        = mean(dep_variance_last_28_days, na.rm=TRUE),
                                                    deposit_var_last_3_months       = mean(dep_variance_last_3_months, na.rm=TRUE),
                                                    deposit_var_last_6_months       = mean(dep_variance_last_6_months, na.rm=TRUE),
                                                    deposit_var_last_12_months      = mean(dep_variance_last_12_months, na.rm=TRUE),
                                                    
                                                
                                                    deposit_var_ratio_last_7_days_month     = as.numeric(mean(dep_variance_last_7_days, na.rm=TRUE)/mean(dep_variance_last_28_days, na.rm=TRUE)),
                                                    deposit_var_ratio_last_7_days_3_months  = as.numeric(mean(dep_variance_last_7_days, na.rm=TRUE)/mean(dep_variance_last_3_months, na.rm=TRUE)),
                                                    deposit_var_ratio_last_14_days_month    = as.numeric(mean(dep_variance_last_14_days, na.rm=TRUE)/mean(dep_variance_last_28_days, na.rm=TRUE)),
                                                    deposit_var_ratio_last_28_days_3_months = as.numeric(mean(dep_variance_last_28_days, na.rm=TRUE)/mean(dep_variance_last_3_months, na.rm=TRUE)),
                                                    deposit_var_ratio_last_28_days_6_months = as.numeric(mean(dep_variance_last_28_days, na.rm=TRUE)/mean(dep_variance_last_6_months, na.rm=TRUE)),
                                                    
                                                    deposit_gradient_all            = mean(dep_gradient_all, na.rm=TRUE),
                                                    deposit_gradient_last_14_days   = mean(dep_gradient_last_14_days, na.rm=TRUE),
                                                    deposit_gradient_last_28_days   = mean(dep_gradient_last_28_days, na.rm=TRUE),
                                                    deposit_gradient_last_3_months  = mean(dep_gradient_last_3_months, na.rm=TRUE),
                                                    deposit_gradient_last_6_months  = mean(dep_gradient_last_6_months, na.rm=TRUE),
                                                    deposit_gradient_last_12_months = mean(dep_gradient_last_12_months, na.rm=TRUE),
                                                    deposit_gradient_ratio_last_14_days_28_days     = as.numeric(mean(dep_gradient_last_14_days/dep_gradient_last_28_days, na.rm=TRUE)),
                                                    deposit_gradient_ratio_last_14_days_3_months    = as.numeric(mean(dep_gradient_last_14_days/dep_gradient_last_3_months, na.rm=TRUE)),
                                                    deposit_gradient_ratio_last_28_days_6_months    = as.numeric(mean(dep_gradient_last_28_days/dep_gradient_last_6_months, na.rm=TRUE)),
                                                    deposit_gradient_ratio_last_28_days_12_months   = as.numeric(mean(dep_gradient_last_28_days/dep_gradient_last_12_months, na.rm=TRUE)),
                                                    
                                                    days_between_deposits               = as.numeric(median(days_between_dep, na.rm=TRUE)),
                                                    days_between_deposits_last_7_days   = as.numeric(median(days_between_dep_last_7_days, na.rm=TRUE)),
                                                    days_between_deposits_last_14_days  = as.numeric(median(days_between_dep_last_14_days, na.rm=TRUE)),
                                                    days_between_deposits_last_28_days  = as.numeric(median(days_between_dep_last_28_days, na.rm=TRUE)),
                                                    days_between_deposits_last_3_months = as.numeric(median(days_between_dep_last_3_months, na.rm=TRUE)),
                                                    days_between_deposits_last_6_months = as.numeric(median(days_between_dep_last_6_months, na.rm=TRUE)),
                                                    days_between_deposits_last_12_months= as.numeric(median(days_between_dep_last_12_months, na.rm=TRUE)),
                                                    days_between_deposits_ratio_last_7_days_28_days     = median(as.numeric(days_between_dep_last_7_days)/as.numeric(days_between_dep_last_28_days, na.rm=TRUE)),
                                                    days_between_deposits_ratio_last_7_days_3_months    = median(as.numeric(days_between_dep_last_7_days)/as.numeric(days_between_dep_last_3_months, na.rm=TRUE)),
                                                    days_between_deposits_ratio_last_14_days_28_days    = median(as.numeric(days_between_dep_last_14_days)/as.numeric(days_between_dep_last_28_days, na.rm=TRUE)),
                                                    days_between_deposits_ratio_last_28_days_3_months   = median(as.numeric(days_between_dep_last_28_days)/as.numeric(days_between_dep_last_3_months, na.rm=TRUE)),
                                                    days_between_deposits_ratio_last_28_days_6_months   = median(as.numeric(days_between_dep_last_28_days)/as.numeric(days_between_dep_last_6_months, na.rm=TRUE)),
                                                    
                                                    max_days_between_deposits               = as.numeric(max(max_days_between_dep, na.rm=TRUE)),
                                                    max_days_between_deposits_last_7_days   = as.numeric(max(max_days_between_dep_last_7_days, na.rm=TRUE)),
                                                    max_days_between_deposits_last_14_days  = as.numeric(max(max_days_between_dep_last_14_days, na.rm=TRUE)),
                                                    max_days_between_deposits_last_28_days  = as.numeric(max(max_days_between_dep_last_28_days, na.rm=TRUE)),
                                                    max_days_between_deposits_last_3_months = as.numeric(max(max_days_between_dep_last_3_months, na.rm=TRUE)),
                                                    max_days_between_deposits_last_6_months = as.numeric(max(max_days_between_dep_last_6_months, na.rm=TRUE)),
                                                    max_days_between_deposits_last_12_months= as.numeric(max(max_days_between_dep, na.rm=TRUE)),
                                                    
                                                    days_between_deposits_var       = mean(days_between_dep_variance, na.rm=TRUE),
                                                    days_since_last_deposit         = as.numeric(median(days_since_last_dep, na.rm=TRUE)),
                                                    

                                                    withdrawals                             = sum(withdrawals, na.rm=TRUE),
                                                    withdrawals_last_7_days                 = sum(withdrawals_last_7_days, na.rm=TRUE),
                                                    withdrawals_last_14_days                = sum(withdrawals_last_14_days, na.rm=TRUE),
                                                    withdrawals_last_28_days                = sum(withdrawals_last_28_days, na.rm=TRUE),
                                                    withdrawals_last_3_months               = sum(withdrawals_last_3_months, na.rm=TRUE),
                                                    withdrawals_last_6_months               = sum(withdrawals_last_6_months, na.rm=TRUE),
                                                    withdrawals_last_12_months              = sum(withdrawals_last_12_months, na.rm=TRUE),
                                                    withdrawals_ratio_last_7_days_28_days   = (last_28_days/last_7_days) * (as.numeric(sum(withdrawals_last_7_days, na.rm=TRUE)/sum(withdrawals_last_28_days, na.rm=TRUE))),
                                                    withdrawals_ratio_last_7_days_3_months  = (last_3_months/last_7_days)* (as.numeric(sum(withdrawals_last_7_days, na.rm=TRUE)/sum(withdrawals_last_3_months, na.rm=TRUE))),
                                                    withdrawals_ratio_last_14_days_month    = (last_28_days/last_14_days)* (as.numeric(sum(withdrawals_last_14_days, na.rm=TRUE)/sum(withdrawals_last_28_days, na.rm=TRUE))),
                                                    withdrawals_ratio_last_28_days_3_months = (last_3_months/last_28_days) *(as.numeric(sum(withdrawals_last_28_days, na.rm=TRUE)/sum(withdrawals_last_3_months, na.rm=TRUE))),
                                                    withdrawals_ratio_last_28_days_6_months = (last_6_months/last_28_days) *(as.numeric(sum(withdrawals_last_28_days, na.rm=TRUE)/sum(withdrawals_last_6_months, na.rm=TRUE))),
 
                                                    withdrawals_sum                         = sum(with_sum, na.rm=TRUE),
                                                    withdrawals_sum_last_7_days             = sum(with_sum_last_7_days, na.rm=TRUE),
                                                    withdrawals_sum_last_14_days            = sum(with_sum_last_14_days, na.rm=TRUE),
                                                    withdrawals_sum_last_28_days            = sum(with_sum_last_28_days, na.rm=TRUE),
                                                    withdrawals_sum_last_3_months           = sum(with_sum_last_3_months, na.rm=TRUE),
                                                    withdrawals_sum_last_6_months           = sum(with_sum_last_6_months, na.rm=TRUE),
                                                    withdrawals_sum_last_12_months          = sum(with_sum_last_12_months, na.rm=TRUE),
                                                    withdrawals_sum_last_7_days_28_days     = (last_28_days/last_7_days) * (as.numeric(sum(with_sum_last_7_days, na.rm=TRUE)/sum(with_sum_last_28_days, na.rm=TRUE))),
                                                    withdrawals_sum_last_7_days_3_months    = (last_3_months/last_7_days)* (as.numeric(sum(with_sum_last_7_days, na.rm=TRUE)/sum(with_sum_last_3_months, na.rm=TRUE))),
                                                    withdrawals_sum_last_14_days_month      = (last_28_days/last_14_days)* (as.numeric(sum(with_sum_last_14_days, na.rm=TRUE)/sum(with_sum_last_28_days, na.rm=TRUE))),
                                                    withdrawals_sum_last_28_days_3_months   = (last_3_months/last_28_days) *(as.numeric(sum(with_sum_last_28_days, na.rm=TRUE)/sum(with_sum_last_3_months, na.rm=TRUE))),
                                                    withdrawals_sum_last_28_days_6_months   = (last_6_months/last_28_days) *(as.numeric(sum(with_sum_last_28_days, na.rm=TRUE)/sum(with_sum_last_6_months, na.rm=TRUE))),
                                                                                                       
                                                    withdrawal_avg                  = mean(with_median, na.rm=TRUE),
                                                    withdrawal_avg_last_7_days      = mean(with_median_last_7_days, na.rm=TRUE),
                                                    withdrawal_avg_last_14_days     = mean(with_median_last_14_days, na.rm=TRUE),
                                                    withdrawal_avg_last_28_days     = mean(with_median_last_28_days, na.rm=TRUE),
                                                    withdrawal_avg_last_3_months    = mean(with_median_last_3_months, na.rm=TRUE),
                                                    withdrawal_avg_last_6_months    = mean(with_median_last_6_months, na.rm=TRUE),
                                                    withdrawal_avg_last_12_months   = mean(with_median_last_12_months, na.rm=TRUE),
                                                    withdrawal_avg_ratio_last_7_days_28_days    = as.numeric(mean(with_median_last_7_days, na.rm=TRUE)/mean(with_median_last_28_days, na.rm=TRUE)),
                                                    withdrawal_avg_ratio_last_7_days_3_months   = as.numeric(mean(with_median_last_7_days, na.rm=TRUE)/mean(with_median_last_3_months, na.rm=TRUE)),
                                                    withdrawal_avg_ratio_last_14_days_28_days   = as.numeric(mean(with_median_last_14_days, na.rm=TRUE)/mean(with_median_last_28_days, na.rm=TRUE)),
                                                    withdrawal_avg_ratio_last_28_days_3_months  = as.numeric(mean(with_median_last_28_days, na.rm=TRUE)/mean(with_median_last_3_months, na.rm=TRUE)),
                                                    withdrawal_avg_ratio_last_28_days_6_months  = as.numeric(mean(with_median_last_28_days, na.rm=TRUE)/mean(with_median_last_6_months, na.rm=TRUE)),
                                                    
                                                    withdrawal_var                  = mean(with_variance, na.rm=TRUE),
                                                    withdrawal_var_last_7_days      = mean(with_variance_last_7_days, na.rm=TRUE),
                                                    withdrawal_var_last_14_days     = mean(with_variance_last_14_days, na.rm=TRUE),
                                                    withdrawal_var_last_28_days     = mean(with_variance_last_28_days, na.rm=TRUE),
                                                    withdrawal_var_last_3_months    = mean(with_variance_last_3_months, na.rm=TRUE),
                                                    withdrawal_var_last_6_months    = mean(with_variance_last_6_months, na.rm=TRUE),
                                                    withdrawal_var_last_12_months   = mean(with_variance_last_12_months, na.rm=TRUE),
                                                    withdrawal_var_ratio_last_7_days_28_days    = as.numeric(mean(with_variance_last_7_days, na.rm=TRUE)/mean(with_variance_last_28_days, na.rm=TRUE)),
                                                    withdrawal_var_ratio_last_7_days_3_months   = as.numeric(mean(with_variance_last_7_days, na.rm=TRUE)/mean(with_variance_last_3_months, na.rm=TRUE)),
                                                    withdrawal_var_ratio_last_14_days_28_days   = as.numeric(mean(with_variance_last_14_days, na.rm=TRUE)/mean(with_variance_last_28_days, na.rm=TRUE)),
                                                    withdrawal_var_ratio_last_28_days_3_months  = as.numeric(mean(with_variance_last_28_days, na.rm=TRUE)/mean(with_variance_last_3_months, na.rm=TRUE)),
                                                    withdrawal_var_ratio_last_28_days_6_months  = as.numeric(mean(with_variance_last_28_days, na.rm=TRUE)/mean(with_variance_last_6_months, na.rm=TRUE)),
                                                    
                                                    withdrawal_gradient_all             = mean(with_gradient_all, na.rm=TRUE),
                                                    withdrawal_gradient_14_days         = mean(with_gradient_last_14_days, na.rm=TRUE),
                                                    withdrawal_gradient_last_28_days    = mean(with_gradient_last_28_days, na.rm=TRUE),
                                                    withdrawal_gradient_last_3_months   = mean(with_gradient_last_3_months, na.rm=TRUE),
                                                    withdrawal_gradient_last_6_months   = mean(with_gradient_last_6_months, na.rm=TRUE),
                                                    withdrawal_gradient_last_12_months  = mean(with_gradient_last_12_months, na.rm=TRUE),
                                                    withdrawal_gradient_ratio_last_14_days_28_days  = as.numeric(mean(with_gradient_last_14_days/with_gradient_last_28_days, na.rm=TRUE)),
                                                    withdrawal_gradient_ratio_last_14_days_3_months = as.numeric(mean(with_gradient_last_14_days/with_gradient_last_3_months, na.rm=TRUE)),
                                                    withdrawal_gradient_ratio_last_28_days_6_months = as.numeric(mean(with_gradient_last_28_days/with_gradient_last_6_months, na.rm=TRUE)),
                                                    withdrawal_gradient_ratio_last_28_days_12_months= as.numeric(mean(with_gradient_last_28_days/with_gradient_last_12_months, na.rm=TRUE)),
                                                    
                                                    days_between_withdrawals                = as.numeric(median(days_between_with, na.rm=TRUE)),
                                                    days_between_withdrawals_last_7_days    = as.numeric(median(days_between_with_last_7_days, na.rm=TRUE)),
                                                    days_between_withdrawals_last_14_days   = as.numeric(median(days_between_with_last_14_days, na.rm=TRUE)),
                                                    days_between_withdrawals_last_28_days   = as.numeric(median(days_between_with_last_28_days, na.rm=TRUE)),
                                                    days_between_withdrawals_last_3_months  = as.numeric(median(days_between_with_last_3_months, na.rm=TRUE)),
                                                    days_between_withdrawals_last_6_months  = as.numeric(median(days_between_with_last_6_months, na.rm=TRUE)),
                                                    days_between_withdrawals_last_12_months = as.numeric(median(days_between_with_last_12_months, na.rm=TRUE)),
                                                    days_between_withdrawals_ratio_last_7_days_28_days      = median(as.numeric(days_between_with_last_7_days)/as.numeric(days_between_with_last_28_days, na.rm=TRUE)),
                                                    days_between_withdrawals_ratio_last_7_days_3_months     = median(as.numeric(days_between_with_last_7_days)/as.numeric(days_between_with_last_3_months, na.rm=TRUE)),
                                                    days_between_withdrawals_ratio_last_14_days_28_days     = median(as.numeric(days_between_with_last_14_days)/as.numeric(days_between_with_last_28_days, na.rm=TRUE)),
                                                    days_between_withdrawals_ratio_last_28_days_3_months    = median(as.numeric(days_between_with_last_28_days)/as.numeric(days_between_with_last_3_months, na.rm=TRUE)),
                                                    days_between_withdrawals_ratio_last_28_days_6_months    = median(as.numeric(days_between_with_last_28_days)/as.numeric(days_between_with_last_6_months, na.rm=TRUE)),
                                                    
                                                    max_days_between_withdrawals                = as.numeric(max(max_days_between_with, na.rm=TRUE)),
                                                    max_days_between_withdrawals_last_7_days    = as.numeric(max(max_days_between_with_last_7_days, na.rm=TRUE)),
                                                    max_days_between_withdrawals_last_14_days   = as.numeric(max(max_days_between_with_last_14_days, na.rm=TRUE)),
                                                    max_days_between_withdrawals_last_28_days   = as.numeric(max(max_days_between_with_last_28_days, na.rm=TRUE)),
                                                    max_days_between_withdrawals_last_3_months  = as.numeric(max(max_days_between_with_last_3_months, na.rm=TRUE)),
                                                    max_days_between_withdrawals_last_6_months  = as.numeric(max(max_days_between_with_last_6_months, na.rm=TRUE)),
                                                    max_days_between_withdrawals_last_12_months = as.numeric(max(max_days_between_with, na.rm=TRUE)),
                                                    
                                                    days_between_withdrawals_var    = mean(with_between_dep_variance, na.rm=TRUE),
                                                    days_since_last_withdrawal      = as.numeric(median(days_since_last_with, na.rm=TRUE)),
                                                    
                                                    
                                                    #additional attributes
                                                    withdrawal_deposit_ratio                        = sum(withdrawals, na.rm=TRUE)/sum(deposits, na.rm=TRUE),
                                                    withdrawal_deposit_ratio_last_7_days            = sum(withdrawals_last_7_days, na.rm=TRUE) / sum(deposits_last_7_days, na.rm=TRUE),
                                                    withdrawal_deposit_ratio_last_14_days           = sum(withdrawals_last_14_days, na.rm=TRUE) / sum(deposits_last_14_days, na.rm=TRUE),
                                                    withdrawal_deposit_ratio_last_28_days           = sum(withdrawals_last_28_days, na.rm=TRUE) / sum(deposits_last_28_days, na.rm=TRUE), 
                                                    withdrawal_deposit_ratio_last_3_months          = sum(withdrawals_last_3_months, na.rm=TRUE) / sum(deposits_last_3_months, na.rm=TRUE), 
                                                    withdrawal_deposit_ratio_last_6_months          = sum(withdrawals_last_6_months, na.rm=TRUE) / sum(deposits_last_6_months, na.rm=TRUE), 
                                                    withdrawal_deposit_ratio_last_7_days_28_days    = as.numeric((sum(withdrawals_last_7_days, na.rm=TRUE) / sum(deposits_last_7_days, na.rm=TRUE))/(sum(withdrawals_last_28_days, na.rm=TRUE) / sum(deposits_last_28_days, na.rm=TRUE))),
                                                    withdrawal_deposit_ratio_last_7_days_3_months   = as.numeric((sum(withdrawals_last_7_days, na.rm=TRUE) / sum(deposits_last_7_days, na.rm=TRUE))/(sum(withdrawals_last_3_months, na.rm=TRUE) / sum(deposits_last_3_months, na.rm=TRUE))),
                                                    withdrawal_deposit_ratio_last_28_days_3_months  = as.numeric((sum(withdrawals_last_28_days, na.rm=TRUE) / sum(deposits_last_28_days, na.rm=TRUE))/(sum(withdrawals_last_3_months, na.rm=TRUE) / sum(deposits_last_3_months, na.rm=TRUE))),
                                                    withdrawal_deposit_ratio_last_28_days_6_months  = as.numeric((sum(withdrawals_last_28_days, na.rm=TRUE) / sum(deposits_last_28_days, na.rm=TRUE))/(sum(withdrawals_last_6_months, na.rm=TRUE) / sum(deposits_last_6_months, na.rm=TRUE))),
                                                        
                                                    acc_adjustments                     = sum(acc_adjustments, na.rm=TRUE),
                                                    acc_adjustments_last_7_days         = sum(acc_adjustments_7_days, na.rm=TRUE),
                                                    acc_adjustments_last_14_days        = sum(acc_adjustments_last_14_days, na.rm=TRUE),
                                                    acc_adjustments_last_28_days        = sum(acc_adjustments_last_28_days, na.rm=TRUE),
                                                    acc_adjustments_last_3_months       = sum(acc_adjustments_last_3_months, na.rm=TRUE),
                                                    acc_adjustments_last_6_months       = sum(acc_adjustments_last_6_months, na.rm=TRUE),
                                                    acc_adjustments_last_12_months      = sum(acc_adjustments_last_12_months, na.rm=TRUE),

                                                    cancel_deposits                     = sum(cancel_deposits, na.rm=TRUE),
                                                    cancel_deposits_last_7_days         = sum(cancel_deposits_7_days, na.rm=TRUE),
                                                    cancel_deposits_last_14_days        = sum(cancel_deposits_last_14_days, na.rm=TRUE),
                                                    cancel_deposits_last_28_days        = sum(cancel_deposits_last_28_days, na.rm=TRUE),
                                                    cancel_deposits_last_3_months       = sum(cancel_deposits_last_3_months, na.rm=TRUE),
                                                    cancel_deposits_last_6_months       = sum(cancel_deposits_last_6_months, na.rm=TRUE),
                                                    cancel_deposits_last_12_months      = sum(cancel_deposits_last_12_months, na.rm=TRUE),
                                                    
                                                    cancel_withdrawals                  = sum(cancel_withdrawals, na.rm=TRUE),
                                                    cancel_withdrawals_last_7_days      = sum(cancel_withdrawals_7_days, na.rm=TRUE),
                                                    cancel_withdrawals_last_14_days     = sum(cancel_withdrawals_last_14_days, na.rm=TRUE),
                                                    cancel_withdrawals_last_28_days     = sum(cancel_withdrawals_last_28_days, na.rm=TRUE),
                                                    cancel_withdrawals_last_3_months    = sum(cancel_withdrawals_last_3_months, na.rm=TRUE),
                                                    cancel_withdrawals_last_6_months    = sum(cancel_withdrawals_last_6_months, na.rm=TRUE),
                                                    cancel_withdrawals_last_12_months   = sum(cancel_withdrawals_last_12_months, na.rm=TRUE)),
                                                    
                                                    by=key(customer_account_all)]


#perform a table reduction - activity from 2014-01-01
# must do this first to avoid memory issues
#####################################################
cat('Reducing the customer attributes table ...\n\n')
customer_churn_attributes <- customer_attributes_all[customer_attributes_all$days_since_last_bet <= as.numeric(latest_date - begin_date), ]

#merge with other attribute tables as section1 & section2
#########################################################
customer_churn_attributes <- merge(customer_churn_attributes,customer_attributes_section1, by="customer_number", all.x=TRUE )
customer_churn_attributes <- merge(customer_churn_attributes,customer_attributes_section2, by="customer_number", all.x=TRUE )

#after these mergers change the target variables into factors
customer_churn_attributes$churn_1_days_between_bets     <- as.factor(customer_churn_attributes$churn_1_days_between_bets)
customer_churn_attributes$churn_2_days_since_last_bet   <- as.factor(customer_churn_attributes$churn_2_days_since_last_bet)
customer_churn_attributes$churn_3_days_between_bets     <- as.factor(customer_churn_attributes$churn_3_days_between_bets)

#Also must perform thi pad the "online_id_verified" column - to be fixed later
customer_churn_attributes$online_id_verified <- "NA"

#convert the NANs and Infs to NA - however due to memory issues doesnt work
#customer_churn_attributes[mapply(is.infinite, customer_churn_attributes)] <- NA
#customer_churn_attributes[mapply(is.na, customer_churn_attributes)] <- NA

#cleaning up data to remove infinities for glm (versioned up)
#if memory is low must break into columns then recombine using cbind e.g.
x1 <- subset(customer_churn_attributes, select=c(1:50))
x1[mapply(is.infinite , x1)] <- NA
x1[mapply(is.na , x1)] <- NA

x2 <- subset(customer_churn_attributes, select=c(51:100))
x2[mapply(is.infinite , x2)] <- NA
x2[mapply(is.na , x2)] <- NA

x3 <- subset(customer_churn_attributes, select=c(101:150))
x3[mapply(is.infinite , x3)] <- NA
x3[mapply(is.na , x3)] <- NA

x4 <- subset(customer_churn_attributes, select=c(151:200))
x4[mapply(is.infinite , x4)] <- NA
x4[mapply(is.na , x4)] <- NA

x5 <- subset(customer_churn_attributes, select=c(201:250))
x5[mapply(is.infinite , x5)] <- NA
x5[mapply(is.na , x5)] <- NA

x6 <- subset(customer_churn_attributes, select=c(251:300))
x6[mapply(is.infinite , x6)] <- NA
x6[mapply(is.na , x6)] <- NA

x7 <- subset(customer_churn_attributes, select=c(301:350))
x7[mapply(is.infinite , x7)] <- NA
x7[mapply(is.na , x7)] <- NA

x8 <- subset(customer_churn_attributes, select=c(351:400))
x8[mapply(is.infinite , x8)] <- NA
x8[mapply(is.na , x8)] <- NA

x9 <- subset(customer_churn_attributes, select=c(401:425))
x9[mapply(is.infinite , x9)] <- NA
x9[mapply(is.na , x9)] <- NA

#bind all temporary data frames
customer_churn_attributes <- cbind(x1,x2,x3,x4,x5,x6,x7,x8,x9)

#remove some tables and unwanted temprary variables
rm(account_transaction_attributes, customer_account, customer_account_all, customer_attributes_all, customer_attributes_section1, customer_attributes_section2,x1,x2,x3,x4,x5,x6,x7,x8,x9)

#turn warnings back on
options(warn = 0)

# save image
save.image(paste(workingDir,"results/R_workspace/churn_model_workspace.RData", sep=""))
