# Project: UX_hamonization_Rnd1_DE

# .	The test is scheduled to start on Monday, 17 August and run for 2 weeks, ending 31 August. 
# .	The setup will take place probably around 10:00 or 12:00, so the first IJM sending in test will be at 11:00 or 13:00
# .	The TESTNAME will be TEMPLATE1 and TESTVARIANTS will include A, B and C
#===============================================================

#1. downoad libraries
#==========================================================
library(RPostgres)
library(DBI)
library(data.table)
library(lubridate)
library(RSiteCatalyst)
library(dplyr)
library(stringr)
library(jsonlite)

#Connect to servers
#=========================================================
#2. connect to servers 

# Download send / open / click data from DW redshift #####
CE_RS_conn <-
  dbConnect(RPostgres::Postgres(),  
            host = "redshift-dw-live.c4twbv2guw8k.eu-central-1.redshift.amazonaws.com", 
            port = 5439, 
            user = "ghalap01", 
            password = "4mpYPyM5xib8UJxc", 
            dbname = "live",
            sslmode="require")

#=================================================================
# 3. connect to Adobe Analytics using RSiteCatalyst

key <- "Parastoo.Ghalamchi@stepstone.com:StepStone" # you need to replace this with the username in the email
secret <- "5792a815a90be19c79bebc3c6f38c6c3" # replace this with your shared secret in the email
SCAuth(key, secret)
rm(key, secret)

#Download list of segments, metrics and elements from Adove:
# reportsuites_DE <- GetReportSuites()
# segments_DE <- setDT(GetSegments("stepstone-de-core-v5"))
# metrics_DE <- setDT(GetMetrics("stepstone-de-core-v5"))
# elements_DE <- setDT(GetElements("stepstone-de-core-v5"))

#=================================================================
#4. Set storage location and retrieve the existing data if available
# start date = 17th 
# the code is tested for UX harmonization ABC test on IJM and IJM+: between 17th to 31st of August



data_loc <- "E:/OneDrive - thioal01/OneDrive - StepStone Group/broadcast dashboards/IJM_Harmonisation_DE_Round1/"

if(file.exists(paste0(data_loc, "IJM_Harmonisation_DE_Round1.csv"))) {
  output <- fread(paste0(data_loc, "IJM_Harmonisation_DE_Round1.csv"))
  output <- output[date < Sys.Date() - 2, ]
  fwrite(output, paste0(data_loc, "IJM_Harmonisation_DE_Round1.csv"), row.names = FALSE, append = FALSE)
  if(nrow(output) == 0) {
    date_list <- seq.Date(as.Date("2020-08-17"), Sys.Date(), "day")
  } else {
    date_list <- seq.Date(max(as.Date(output$date) + 1), Sys.Date(), "day")
  }
  print("file exists")
}else {
  print("no file")
  date_list <- seq.Date(as.Date("2020-08-17"), Sys.Date(), "day")
}


#======================================================================
#5. query data
#Note: segments should change for UX_harmonization test. This is to test the code on IJM+ for Halo AB test

#GroupA, GroupB, GroupC: evar111: TEMPLATE_A, TEMPLATE_B, TEMPLATE_C
#DE - IJM/IJMPlus (hits)	: s1102_5dd3c2e78d55e533b65fd3ae


#define InlineSegment to search evar111 for "TESTNAME will be TEMPLATE1 and TESTVARIANTS will include A, B and C"
#InLineSegment is created to identify the groups


InlineSegment <-
  list(
    container=list(
      type=unbox("hits"),
      rules=data.frame(
        name=c(""),
        element=c("evar111"),
        operator=c("contains"),
        value=c(as.character("TEMPLATE1_"))
      )
    )
  )
for(idate in 1:length(date_list)){ 
 
#import Adobe data:===================================================
  Adobe_IJM_DE <-
    QueueRanked(
      reportsuite.id = "stepstone-de-core-v5", 
      date.from = date_list[idate], 
      date.to = date_list[idate], 
      metrics = c("visits", "uniquevisitors","event14", "event82", "event75"),
      elements = c("evar111"),
      segment.inline = InlineSegment,
      segment.id = c("s1102_5dd3c2e78d55e533b65fd3ae"),
      enqueueOnly = TRUE)
  
  # import Adobe data and add TemplateName based on the test_id and date
  Adobe_IJM_DE <- setDT(GetReport(Adobe_IJM_DE, interval.seconds = 30, max.attempts = 10))
  
  Adobe_IJM_DE2 <-
    Adobe_IJM_DE %>%
    mutate(
      templatename = case_when(
        name %like% "TEMPLATE1_A" ~ "TEMPLATE1_A",
        name %like% "TEMPLATE1_B" ~ "TEMPLATE1_B",
        name %like% "TEMPLATE1_C" ~ "TEMPLATE1_C",
        TRUE ~ "other"),
      date = as.Date(as.character(date_list[idate]),
      )
    )


#import send, open, click from DB ================================================================================
#to test the query for one day: idate = 1
#filter on country == "DE" and ac_actioncode == 'IJM%'  
  
  
 Send <-
   setDT(dbGetQuery(
     CE_RS_conn,
     paste0(
       "SELECT sentdate as date,
               sum(sent) as sent,
               sum(opensunique) as openunique,
               sum(opens) as open_t,
               sum(clickunique) as clickunique,
               sum(clicks) as click_t,
               sum(clicks_apply_total) as click_apply,
               sum(clicks_inside_content_total) as click_inside_content_t,
               sum(delivered) as delivered,
               sum(unsubscribes) as unsubscribes_t,
               CONCAT(CONCAT(ab_testname, '_'), ab_testvariant) as  templatename
       FROM stepcom_datamart_presentation.report_ijm
       where sentdate  > '", date_list[idate] -1 , "' and sentdate < '", date_list[idate] +1, "' and country = 'DE' and ac_actioncode like 'IJM%'
       group by sentdate, templatename")))
 Send[, sent := as.integer(sent)]
 Send[, openunique := as.integer(openunique)]
 Send[, open_t := as.integer(open_t)]
 Send[, click_t := as.integer(click_t)]
 Send[, unsubscribes_t := as.integer(unsubscribes_t)]
 Send[, delivered := as.integer(delivered)]
 Send[, click_apply := as.integer(click_apply)]
 Send[, click_inside_content_t := as.integer(click_inside_content_t)]

#change format to be able to bind DB ad Adobe data sets
 Adobe_IJM_DE2$date <- as.character(Adobe_IJM_DE2$date)
 Send$date <- as.character(Send$date)


#bind Adobe and DB data sets:  
  output <- 
    rbindlist(
      list(Send,
           rbindlist(list(Adobe_IJM_DE2), 
                     use.names = TRUE)), use.names = TRUE, fill = TRUE)
  
#===========================================================================================  
#6. oupute data into csv. file to be exported to PBi  
  
  output <-
    output[!is.na(templatename) & templatename != "other" & templatename != "_", 
           .(send = sum(sent, na.rm = TRUE),
             open_t = sum(open_t, na.rm = TRUE),
             open_u = sum(openunique, na.rm = TRUE),
             click_t = sum(click_t, na.rm = TRUE),
             click_u = sum(clickunique, na.rm = TRUE),
             click_apply = sum(click_apply, na.rm = TRUE),
             click_inside_content_t = sum(click_inside_content_t, na.rm = TRUE),
             visits = sum(visits, na.rm = TRUE),
             unique_visitor = sum(uniquevisitors, na.rm = TRUE),
             listing_view = sum(event14, na.rm = TRUE),
             applynow_AA = sum(event75, na.rm = TRUE),
             AIT = sum(event82, na.rm = TRUE),
             unsubscribe = sum(unsubscribes_t, na.rm = TRUE)),
           .(date = as.Date(as.character(date)), templatename)]
  output[, update_time := Sys.time()]

  
  setDT(output, keep.rownames=TRUE, key=NULL, check.names=FALSE)  
  
# Write queried day's data to csv file
  fwrite(output, paste0(data_loc, "IJM_Harmonisation_DE_Round1.csv"), row.names = FALSE, append = TRUE)}
