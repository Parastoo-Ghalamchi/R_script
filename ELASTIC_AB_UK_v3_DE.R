#22/06/2020

# click and open: the underlying DB table is called report_ja in schema stepcom_datamart_presentation on the Data Wizard's redshift
# The test will run from June 15th to June 30th.


#DE_SlasticSearchUpgrade_ABtest
#===================================================================================================================
library(RPostgres)
library(DBI)
library(data.table)
library(lubridate)
library(RSiteCatalyst)
#Connect to servers
#=========================================================


# UK Redshift 
UK_RS_conn <- 
  dbConnect(RPostgres::Postgres(),  
            host = "tjgsqlrs.cuiplefksiss.eu-west-1.redshift.amazonaws.com", 
            port = 5439, 
            user = "parastooghalamchi", 
            password = "7cj57VqMtt9", 
            dbname = "tjgrsdb",
            sslmode="require")

# CE country DB
CE_CD1_conn <- 
  DBI::dbConnect(odbc::odbc(), Driver = "SQL Server",
                 Server = "REP-mssql-cit.stepstone.tools",
                 port = 1433)

CE_CD2_conn <- 
  DBI::dbConnect(odbc::odbc(), Driver = "SQL Server",
                 Server = "REP1-mssql-cit.stepstone.tools",
                 port = 1433)


# CE DW Redshift
CE_RS_conn <-
  dbConnect(RPostgres::Postgres(),  
            host = "redshift-dw-live.c4twbv2guw8k.eu-central-1.redshift.amazonaws.com", 
            port = 5439, 
            user = "ghalap01", 
            password = "4mpYPyM5xib8UJxc", 
            dbname = "live",
            sslmode="require")
#==========================================================================
# This is how you connect to Adobe Analytics using RSiteCatalyst
key <- "Parastoo.Ghalamchi@stepstone.com:StepStone" # you need to replace this with the username in the email
secret <- "5792a815a90be19c79bebc3c6f38c6c3" # replace this with your shared secret in the email
SCAuth(key, secret)
rm(key, secret)

# With the following methods from RSiteCatalyst you can find out IDs of report suites, segments, evars, props and events
# e.g. segments you build in Adobe Analytics will be listed in these tables. 

#DE:
reportsuites_DE <- GetReportSuites()
segments_DE <- setDT(GetSegments("stepstone-de-core-v5"))
metrics_DE <- setDT(GetMetrics("stepstone-de-core-v5"))
elements_DE <- setDT(GetElements("stepstone-de-core-v5"))

#create a connection to continental data base
# CE country DB
CE_CD1_conn <- 
  DBI::dbConnect(odbc::odbc(), Driver = "SQL Server",
                 Server = "REP-mssql-cit.stepstone.tools",
                 port = 1433)  

# Create a connection object to StepStone UK's redshift instance
pconn_r <- 
  dbConnect(RPostgres::Postgres(),  
            host = "tjgsqlrs.cuiplefksiss.eu-west-1.redshift.amazonaws.com", # change server if needed
            port = 5439, 
            user = "parastooghalamchi", # replace with your username
            password = "7cj57VqMtt9", # replace with your password
            dbname = "tjgrsdb", 
            sslmode="require")


#r_devstepstonede <- 
#  dbGetQuery(
#    CE_CD1_conn,
#    paste0(
#      "SELECT  top 10 *
#    FROM r_devstepstonede.dbo.JAVerifiedStats
#    where TemplateName like 'ELASTIC%' and [Date] >= '2020-05-16' and Channel = 'stepstone'"))

#DE_ElasticSearch_ABtest===================================================================================================
#DE:
# Set storage location and retrieve the existing data if available
#Question: Does all of the ELASTIC search upgrade exist in "ELASTIC_UK/"? What should it change to for DE?
# Why else condition is "2020-06-11"? 
# script runs on one day
# run the script every day to  see why data is imported like this?

#build a script like this for unsubscribe.


data_loc <- "./"

if(file.exists(paste0(data_loc, "Elastic_DE_Excluding_outliers.csv"))) {
  output <- fread(paste0(data_loc, "Elastic_DE_Excluding_outliers.csv"))
  # If file exists, remove today's and yesterday's data so it can be overwritten in case you update the data multiple times on a given day
  output <- output[date < Sys.Date() - 1, ]
  fwrite(output, paste0(data_loc, "Elastic_DE_Excluding_outliers.csv"), row.names = FALSE, append = FALSE)
  date_list <- seq.Date(max(as.Date(output$date) + 1), Sys.Date(), "day")
}else {
  date_list <- seq.Date(as.Date("2020-06-11"), Sys.Date(), "day")
}

#View(data_loc)
#its empty!

#==================================================================================================


#DE: testid as TemplateName like 'Elastic%' ElasticsSearch_A_DE and ElasicSearch_B_DE  
  # There are various types of reports you can query using RSiteCatalyst. Have a look at the documentation.
  # Question: where is RSiteCatalyst documentation?
  #=======================================================================================
#Metrics are changef for DE into:
  #using parameter dictionary (checked with Ammar)
  #Job Details View(e14), 
    #UK: e14
    #DE: e14
  #Application:
    #UK: (B2C JA Edited (e43)), 
    # DE: AIT(e82) and  event75(B2C ApplyNow Button Click) 
    # in PowerBi: event75 in table
  #Email unsubscribe: 
    #UK: B2C JA Unsubscribed(e27),
    #DE: B2C JA Unsubscribed(e27)
  #Unique Visitors_TD 
    #UK: Visits_TD
    #DE: Visits_TD
  ##############so metrics are passed to metrics as: metrics = c("visits", "event14", "event82", "event27")
  
#segments are changed for DE into:
#GroupA:
  #ElasticsSearch_A_DE: 5eea0b39d398fd07109ddc3d
  #StSt-JobAgent-Hits: 54d22fe2e4b03d75f51cbe61
  
#GroupB:  
  #ElasicSearch_B_DE: 5eea0b39d398fd07109ddc3d
  #StSt-JobAgent-Hits: 54d22fe2e4b03d75f51cbe61

#query needs to change and use date from date


  

for(idate in 1:length(date_list)){  
  AA_GroupA_DE <-
    QueueOvertime(
      reportsuite.id = "stepstone-de-core-v5", 
      date.from = date_list[idate], date.to = date_list[idate], 
      metrics = c("visits", "event14", "event82", "event75", "event27"),
      segment.id = c("54d22fe2e4b03d75f51cbe61", "s1102_5eeb3ea48d55e526e5e9fb8d"),
      enqueueOnly = TRUE)
  
  AA_GroupB_DE <-
    QueueOvertime(
      reportsuite.id = "stepstone-de-core-v5", 
      date.from = date_list[idate], date.to = date_list[idate], 
      metrics = c("visits", "event14", "event82", "event75","event27"),
      segment.id = c("54d22fe2e4b03d75f51cbe61", "s1102_5eeb3ee290f9042ce947ce96"),
      enqueueOnly = TRUE)

  
  
#Note: UK and DE difference in considered:
      #UK: send + not_send = considered
      #DE: considered and send
#partitionyear = ",year(date_list[idate])
  
nSend_nConsidered_date <- 
    dbGetQuery(
      CE_CD1_conn,
      paste0(
        "SELECT nVerified as considered_DE, nSent as send_DE, TemplateName, [Date]
         FROM r_devstepstonede.dbo.JAVerifiedStats
         where [Date] = '",as.character(date_list[idate]), "' and TemplateName like 'ELASTIC%'"))

# Download the Adobe data. 
# Important: GetReport will ask Adobe if the report is ready in constant intervals (here every 30 seconds,
# and max 10 tries). These requests count against the company quota, i.e. if the interavl is too short,
# and there are too many retries, no-one can work again. So, be mindful, of setting these parameters!
AA_GroupA_DE <- setDT(GetReport(AA_GroupA_DE, interval.seconds = 30, max.attempts = 10))
AA_GroupB_DE <- setDT(GetReport(AA_GroupB_DE, interval.seconds = 30, max.attempts = 10))  
  
  
#get the oupute csv file to see what is the format to prevent changing powerbi later
# Add Adobe data to the output table

#combine AA_GroupA_DE and AA_GroupB_DE
GroupA_GroupB <- rbindlist(list(AA_GroupA_DE[, TemplateName := "ELASTICSEARCH_A"], 
                                AA_GroupB_DE[, TemplateName := "ELASTICSEARCH_B"]), fill = TRUE)

#rename headers to equivalent segments in Adobe
colnames(nSend_nConsidered_date)[3]  <- "testid"
colnames(GroupA_GroupB)[1]  <- "Date"
colnames(GroupA_GroupB)[9]  <- "listing_view"
colnames(GroupA_GroupB)[10]  <- "AIT"
colnames(GroupA_GroupB)[11]  <- "application"
colnames(GroupA_GroupB)[12]  <- "unsubscribe"
colnames(GroupA_GroupB)[13]  <- "testid"


#rbind nSend_nConsidered_date and GroupA_GroupB on date

#change the format of Date collumn to combine the tables
GroupA_GroupB$Date <- as.character(GroupA_GroupB$Date)
#as.Date()
nSend_nConsidered_date$Date <- as.character(nSend_nConsidered_date$Date)

#GroupA_GroupB <- within(GroupA_GroupB, event.date.time <- as.POSIXct(as.character(event.date.time), format = "%Y-%m-%d"))

output <- 
  rbindlist(
    list(nSend_nConsidered_date,
         rbindlist(list(GroupA_GroupB), 
                   use.names = TRUE)), use.names = TRUE, fill = TRUE)


  
#only for send, considered, visits, listing_view, application, AIT, unsubscribe  
  output <-
    output[, 
           .(send = sum(send_DE, na.rm = TRUE),
             open_t = 0,
             open_u = 0,
             click_t = 0,
             click_u = 0,
             visits = sum(visits, na.rm = TRUE),
             listing_view = sum(listing_view, na.rm = TRUE),
             application = sum(application, na.rm = TRUE),
             unsubscribe = sum(unsubscribe, na.rm = TRUE),
             considered = sum(considered_DE, na.rm = TRUE),
             AIT = sum(AIT, na.rm = TRUE)), 
           .(date = as.Date(as.character(Date)), testid)]
  output[, update_time := Sys.time()]
  
  
  # Write queried day's data to csv file

#fwrite(output, file = "./Elastic_DE.csv", sep = ",", row.names = FALSE, append = TRUE)
fwrite(output, paste0(data_loc, "Elastic_DE_Excluding_outliers.csv"), row.names = FALSE, append = TRUE)
}

# Question: 
  #Should I use data_loc? 
  # using header=TRUE does not work! fwrite(output, file = "./Elastic_DE_22_06_2020.csv", sep = ",", header=TRUE, row.names = FALSE, append = TRUE)}

#fwrite(output, paste0(data_loc, "Elastic_DE.csv"), row.names = FALSE, append = TRUE)



