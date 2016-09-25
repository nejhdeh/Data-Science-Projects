# R script to reduce the feature and perform a feature extraction exercise

workingDir <- "/home/ubuntu/datascience-share-of-wallet/Development/"

# check for the R workspacce and load
if(file.exists(paste(workingDir,"results/R_workspace/share_of_wallet_workspace.RData", sep="")))
    load(paste(workingDir,"results/R_workspace/share_of_wallet_workspace.RData", sep=""))


#further cleansing
customer_SoW_attributes_reduced[mapply(is.na, customer_SoW_attributes_reduced)]         <- NA
customer_SoW_attributes_reduced[mapply(is.infinite, customer_SoW_attributes_reduced)]   <- NA


# create for classification & further reduction for now - LAST ACTIVE DAY <= 30   
customer_SoW_attributes_classified <- customer_SoW_attributes_reduced[customer_SoW_attributes_reduced$DaysSinceLastActiveDay <= 28,]

# for proper casting
customer_SoW_attributes_classified <- customer_SoW_attributes_classified[!is.na(customer_SoW_attributes_classified$customer_number),]


attach(customer_SoW_attributes_classified)

# Key attributes
#'turnover_last_month_weight_self',
#'turnover_last_month_weight_deseasonalized',
#'turnover_gradient_last_28_days',
#'turnover_gradient_last_3_months',
#'turnover_gradient_last_6_months',
#'TYTurnoverRatio28Days90Days',
#'TYTurnoverRatio28Days180Days'

# weights
self_ref_high               <- 0.9
self_ref_low                <- 0.6

gradient_28_days_high       <- -0.2
gradient_28_days_low        <- -0.5

gradient_3_months_high_1    <- -0.2
gradient_3_months_low_1     <- -0.2
gradient_3_months_high_2    <- -0.1
gradient_3_months_low_2     <- -0.4
gradient_3_months_high_3    <- 0.0
gradient_3_months_low_3     <- -0.3

gradient_6_months_high_1    <- -0.1
gradient_6_months_low_1     <- -0.1
gradient_6_months_high_2    <- 0.0
gradient_6_months_low_2     <- 0.0




#Hierarchial Rule base for the following classifications
########################################################
No_Action   <- as.character('No_Action')
Monitor     <- as.character('Monitor')
Action      <- as.character('Action')


for (i in 1 : length(customer_number))
{
    #First check to see not an NA - if so then label as "Montitor'
    if(is.na(turnover_last_month_weight_self[i]))
        customer_SoW_attributes_classified$SoW_class[i] <- Monitor
    
    else
    {
        # top of tree - branch 1
        if(turnover_last_month_weight_self[i] >= self_ref_high) 
        {
            if ((is.na(turnover_gradient_last_3_months[i])) | (turnover_gradient_last_3_months[i] >= gradient_3_months_high_1)) 
                customer_SoW_attributes_classified$SoW_class[i] <- No_Action 
            else
                customer_SoW_attributes_classified$SoW_class[i] <- Monitor   
        }
        
        # top of tree - branch 2
        else if ((turnover_last_month_weight_self[i] < self_ref_high) & (turnover_last_month_weight_self[i] >= self_ref_low)) 
        {
            #middle brach firast
            if( (is.na(turnover_gradient_last_3_months[i])) | ((turnover_gradient_last_3_months[i] < gradient_3_months_high_2) & (turnover_gradient_last_3_months[i] >= gradient_3_months_low_2)))
            {
                if(  (is.na(turnover_gradient_last_6_months[i])) | (turnover_gradient_last_6_months[i] >= gradient_6_months_high_1))
                    customer_SoW_attributes_classified$SoW_class[i] <- Monitor
                
                else if (turnover_gradient_last_6_months[i] < gradient_6_months_high_1)
                    customer_SoW_attributes_classified$SoW_class[i] <- Action  
                
                else # catch all
                    customer_SoW_attributes_classified$SoW_class[i] <- Monitor     
            }
            
            else if (turnover_gradient_last_3_months[i] >= gradient_3_months_high_2)
                customer_SoW_attributes_classified$SoW_class[i] <- No_Action
            

            else
                customer_SoW_attributes_classified$SoW_class[i] <- Action
        }
        
        # top of tree - branch 3
        else
        {
            
            #middle branch first
            if( (is.na(turnover_gradient_last_3_months[i])) | ((turnover_gradient_last_3_months[i] < gradient_3_months_high_3) & (turnover_gradient_last_3_months[i] >= gradient_3_months_low_3)))
            {
                if(  (is.na(turnover_gradient_last_6_months[i])) | (turnover_gradient_last_6_months[i] >= gradient_6_months_high_2))
                    customer_SoW_attributes_classified$SoW_class[i] <- Monitor
                
                else if (turnover_gradient_last_6_months[i] < gradient_6_months_high_2)
                    customer_SoW_attributes_classified$SoW_class[i] <- Action  
                
                else # catch all
                    customer_SoW_attributes_classified$SoW_class[i] <- Monitor     
            }
            
            else if (turnover_gradient_last_3_months[i] >= gradient_3_months_high_3)
                customer_SoW_attributes_classified$SoW_class[i] <- Monitor
            
            
            else
                customer_SoW_attributes_classified$SoW_class[i] <- Action
            
        }
    
    }

}


# save image
save.image(paste(workingDir,"results/R_workspace/share_of_wallet_workspace.RData", sep=""))     



