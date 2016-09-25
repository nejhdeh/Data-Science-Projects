# R script to perform the C5.0 algorithm
workingDir <- "/home/ubuntu/churn_model/Production/"
load(paste(workingDir,"results/R_workspace/churn_model_workspace.RData", sep=""))

suppressMessages(library(plyr))
suppressMessages(library(C50))
suppressMessages(library(caret))
suppressMessages(library(e1071))

############################################################################################
# DATA PREPARATION
############################################################################################
#use the data to perform some data normalisation
training_set_C5.0  <- training_set
test_set_C5.0      <- test_set 
thresh  <- 0.5

############################################################################################
# C5.0 MODELLING
############################################################################################
#setup the C5.0 formulas
c5.0_formula3 <- churn_1_days_between_bets ~    #accounts
                                                withdrawal_deposit_ratio_last_28_days +
                                                
                                                #bets
                                                TotalMedianDaysBetweenActiveDays + 
                                                TotalActiveDays +                                                                                                   
                                                TotalTurnoverperActiveDay +               
                                                TotalBets +                             
                                                TotalBetsperActiveDay +                 
                                                TotalWinBets +                          
                                                TotalWinBetRatio +                        
                                                TotalDividends +                          
                                                TotalDivTurnoverRatio +                   
                                                TotalMaxDaysBetweenActiveDays +                  
                                                days_since_last_bet +   
                                                #TotalMedianDaysBand  #this is a factor with " as one of the factors              
                                                ActiveinLast30Days +
                                                                                            
                                                #cyclic
                                                TYActiveDaysLast7days +
                                                TYActiveDaysLast28days +
                                                TYActiveDaysRatio7Days14Days +
                                                TYBetsRatio7Days28Days +
                                                TYTurnoverRatio7Days28Days

#run the c5.0 algorithm
c5.0_fit3 <- C5.0(c5.0_formula3, data=training_set_C5.0, na.action=na.pass, rules=TRUE)


#get the accuracy of the model - silly way since cant extract the eror rate for the summary
c5.0_fit3_train_summary <- predict.C5.0(c5.0_fit3, newdata = training_set_C5.0, type="class")
c5.0_fit3_train_summary <- confusionMatrix(c5.0_fit3_train_summary,training_set_C5.0$churn_1_days_between_bets)

###########################################################################################
# PREDICTION & VERIFICATION
###########################################################################################
#perform the prediction - using a probability output. Can also use type="class"
c5.0_fit3_predict <- predict.C5.0(c5.0_fit3, newdata = test_set_C5.0, predict.all=TRUE, type="prob")

#use the cut function to convert into class type just for performance figures
#note: the predict function of C5.0 is slightly different to glm hence must take the last column
c5.0_fit3_predict_binary <- cut(c5.0_fit3_predict[,2], breaks=c(-Inf, thresh, Inf), labels=c(0,1))

#develope the truth matrix - compare the predicted values against the target sets of the test set
c5.0_fit3_predict_summary <- confusionMatrix(c5.0_fit3_predict_binary,test_set_C5.0$churn_1_days_between_bets)


#obtain accuaroverall acccuracy
c5.0_fit3_predict_summary_accuracy         <- c5.0_fit3_predict_summary$overall["Accuracy"] 
c5.0_fit3_predict_summary_accuracy_pValue  <- c5.0_fit3_predict_summary$overall["AccuracyPValue"]
 
#remove some tables and unwanted temprary variables
rm(training_set_C5.0, test_set_C5.0, c5.0_fit3_predict, c5.0_fit3_predict_binary)

# save image
save.image(paste(workingDir,"results/R_workspace/churn_model_workspace.RData", sep=""))
