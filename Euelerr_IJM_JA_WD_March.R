library(data.table)
library(ggplot2)
library(eulerr)
library(dplyr)
library(tidyr)
library(magrittr)
library(ggplot2)



# Import recipient data in form of csv. file from sandbox for IJM JA and WD send:
#=========================================================================================
# Method 1: table with unique users and interaction count 
# (or interaction indicator - i.e. 1 = has received email, 0 has not received email)

#import data from csv. file
plotdata <- fread("C:\\Users\\parastoo.ghalamchi\\OneDrive - StepStone Group\\Documents\\AlexThio\\Venn_Diagram_analysis\\IJM_JA_March.csv")
#exclude null
plotdata[is.na(plotdata)] <- 0

#test data
#======================================================================================
JA = sum(plotdata$JA_send)
#JA in MArch = 31.6 15 days
#JA in June = 31M 15 days

IJM = sum(plotdata$IJM_send)
IJM
#IJM in March = 26.8M 15 days
#IJM in June = 25.5M 15 days

WD = sum(plotdata$WeeklyDigest_send)
#1.5M 15 days

#how the venn analysis data look like?
#newdata <- mydata[ which(mydata$gender=='F'
#                         & mydata$age > 65), ]

JA_only <- plotdata[ which(plotdata$JA_send > 0
                         & plotdata$IJM_send == 0
                         & plotdata$WeeklyDigest_send == 0), ]

IJM_only <- plotdata[ which(plotdata$IJM_send > 0
                            & plotdata$JA_send == 0
                            & plotdata$WeeklyDigest_send == 0), ]

WD_only <- plotdata[ which(plotdata$WeeklyDigest_send > 0
                            & plotdata$IJM_send == 0
                            & plotdata$JA_send == 0), ]

JA_only$JA <- 1
IJM_only$IJM <- 1
WD_only$WD <- 1

WD_only = select(WD_only, user_id, WD)
JA_only = select(JA_only, user_id, JA)
IJM_only = select(IJM_only, user_id, IJM)

# overlaps:
JA_WD = merge(WD_only, JA_only, by = "user_id")
IJM_WD = merge(WD_only, IJM_only, by = "user_id")
JA_IJM = merge(IJM_only, JA_only, by = "user_id")

JA_WD_IJM = merge(JA_WD, IJM_WD, by = "user_id")
JA_WD_IJM = merge(JA_WD_IJM, IJM_JA, by = "user_id")

JA_WD = merge(WD_only, JA_only, by = "user_id")


#============================================================================
#Open questions: how to group data to based on the Euller segments?


#my attempts so far:===============================================================
#pivote plotdata:

#IJM_JA_WD_df <- read.df("IJM_JA_WD_df.csv", source="csv", header=TRUE, inferSchema=TRUE)

plotdata %>% 
   groupBy("user_id") %>% 
   pivot("campaign_type") %>% 
   agg(sum(column("user_id")))

#dcast


#colors
cols <- c("#0C2577", "#4088EE", "#7AAE1A")

#method1:==================================================
dteuler <- euler(plotdata[,
               .(JA_send = as.integer(JA_send > 0),
                 IJM_send = as.integer(IJM_send > 0))])

dteuler1 <- euler(plotdata)
dteuler1

plot(dteuler, quantities = T, legend=F, fills = cols, alpha=.5)

# Method 2: aggregate data==================================
#we know the numbers before the analysis
#WD = 40k
#JA = 530k
#IJM = 173k
#WD&JA = 340k + 470k
#WD&IJM = 18.5k + 470k
#JA&IJM = 170k + 470k
#WD&IJM&JA = 470k
#WD_IJM_JA_total = 40+530+173+470 = 1213

combo_IJM_JA_WD <- c(WD = 40, JA = 530, IJM = 173, "WD&JA" = 810, "WD&IJM" = 488, "JA&IJM" = 640, "WD&JA&IJM" = 470)
fit_IJM_JA_WD <- euler(combo_IJM_JA_WD)
plot(fit_IJM_JA_WD, quantities = T, legend = F, fills = cols, alpha= .5)


combo_IJM_JA_WD_recipient_ratio <- c(WD = .14, JA = .5, IJM = .14, "WD&JA&IJM" = .4)
fit_IJM_JA_WD <- euler(combo_IJM_JA_WD_recipient_ratio)
plot(fit_IJM_JA_WD, quantities = T, legend = F, fills = cols, alpha= .5)


#unsubscribe only for the overlapp of three campaigns
  #send/canddate
  #click_unique/candidate
  #open_unique/candidate
  #unsubscribe through each overlap 

#table with cohorts of overlaps as collumns and KPI metrics as rows

combo_IJM_JA_WD_unsub_ratio <- c(WD = .14, JA = .5, IJM = .14, "WD&JA&IJM" = .4)
fit_IJM_JA_WD <- euler(combo_IJM_JA_WD_unsub_ratio)
plot(fit_IJM_JA_WD, quantities = T, legend = F, fills = cols, alpha= .5)


combo <- c(A = 2, B = 2, C1 = 2, "A&B" = 1, "A&C1" = 1, "B&C1" = 1)
fit1 <- euler(combo)
plot(fit1, quantities = T, legend = F, fills = cols, alpha= .5)
