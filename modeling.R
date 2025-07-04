library(tidyverse)

source("../CommonFunctions/getSQL.r")


con <- DBI::dbConnect(odbc::odbc(),    
                      Driver = "ODBC Driver 18 for SQL Server", 
                      Server = "abcrepldb.database.windows.net",  
                      Database = "ABCPackerRepl",   
                      UID = "abcadmin",   
                      PWD = "Trauts2018!",
                      Port = 1433
)

GBD <- DBI::dbGetQuery(con, getSQL("SQLfiles/GBDModel.sql"))

WeeklyBins <- DBI::dbGetQuery(con, getSQL("../SQLQueryRepo/SQLFiles2024/WeeklyBins.sql"))

BinDelivery <- DBI::dbGetQuery(con, getSQL("../SQLQueryRepo/SQLFiles2024/BinDelivery.sql"))

DBI::dbDisconnect(con)
#
# Need to get SF harvestdates from SF data
#
SFBins <- read_csv("data/Unpacked_20250625083843.csv",show_col_types = F) |>
  mutate(across(.cols = c(HarvestDate), ~as.Date(.,"%d/%m/%Y")))

SFHarvestDates <- SFBins |>
  filter(str_detect(SourceRun, "^P\\d{4}$")) |>
  group_by(SourceRun) |>
  summarise(HarvestDate = weighted.mean(HarvestDate, Bins, na.rm=T)) |>
  rename(SFHarvestDate = HarvestDate)
#
# Integrate the Harvest dates into the GB data frame
#
GBDSF <- GBD |>
  left_join(SFHarvestDates, by = c("ExternalRun" = "SourceRun")) |>
  mutate(HarvestDate = coalesce(gbHarvestDate, SFHarvestDate)) |>
  select(-c(gbHarvestDate,SFHarvestDate,ExternalRun)) |>
  filter(`Packing site` != "Sunfruit Limited",
         !is.na(InputKgs)) 

POAnalysis <- GBDSF |>
  filter(!is.na(`Storage type`)) |>
  mutate(PackOut = 1-RejectKgs/InputKgs,
         StorageDays = as.integer(PackDate - HarvestDate)) 
#
# 2024 RA Analysis
#

POAnalysis2024RA <- POAnalysis |>
  filter(`Storage type` == "RA",
         Season == 2024,
         !is.na(StorageDays),
         !is.na(PackOut))

NLmodel2024 <- nls(PackOut ~ alpha *exp(beta * StorageDays) + theta, 
                   data=POAnalysis2024RA, 
                   start=list(alpha=0.2, beta=-.05, theta=0.70))

summary(NLmodel2024)

POAnalysis2024CA <- POAnalysis |>
  filter(`Storage type` == "CA",
         Season == 2024,
         StorageDays > 30,
         !is.na(StorageDays),
         !is.na(PackOut))

CAmodel2024 <- lm(PackOut ~ StorageDays, 
                  data=POAnalysis2024CA)

summary(CAmodel2024)

CAModeled2024 <- tibble(StorageDays = POAnalysis2024CA$StorageDays) |>
  bind_cols(predict(CAmodel2024, interval = "confidence")) |>
  mutate(Season = 2024,
         `Storage type` = "CA") |>
  rename(estimate = fit)
#
# Calculate the uplift from 2024
#
meanCAPO <- CAModeled2024 |>
  summarise(meanModeledPO = mean(estimate, na.rm=T)) |>
  mutate(CAUplift = meanModeledPO-summary(NLmodel2024)$coef[[3,1]])
#
# Calculate the confidence intervals
#

yield.bootRPIN <- nlstools::nlsBoot(NLmodel2024, niter = 999)
meanPO2024 <- data.frame(estimate = predict(NLmodel2024,
                                            newdata = POAnalysis2024RA))
CIPO2024 <- data.frame(nlstools::nlsBootPredict(yield.bootRPIN, 
                                                newdata = POAnalysis2024RA, 
                                                interval = "confidence"))

mean_curvePO2024RA <- POAnalysis2024RA |>
  bind_cols(meanPO2024) |>
  bind_cols(CIPO2024) |>
  rename(low = X2.5.,
         high = X97.5.) |>
  mutate(Season = 2024)

#
# 2025 Data - This needs the season packout information
#

POAnalysis2025RA <- POAnalysis |>
  filter(`Storage type` == "RA",
         Season == 2025)

StorageDaysRASc1 <- tibble(StorageDays = seq(0,166,1))

NLmodel2025RA <- nls(PackOut ~ alpha *exp(beta * StorageDays) + theta, 
                     data=POAnalysis2025RA, 
                     start=list(alpha=0.247, beta=-.012, theta=0.7))

summary(NLmodel2025RA)

yield.boot2025 <- nlstools::nlsBoot(NLmodel2025RA, niter = 999)
meanPO2025 <- data.frame(estimate = predict(NLmodel2025RA,
                                            newdata = StorageDaysRASc1))
CIPO2025 <- data.frame(nlstools::nlsBootPredict(yield.boot2025, 
                                                newdata = StorageDaysRASc1, 
                                                interval = "confidence"))

mean_curvePO2025RA <- StorageDaysRASc1 |> 
  bind_cols(meanPO2025) |>
  bind_cols(CIPO2025) |>
  rename(low = X2.5.,
         high = X97.5.) |>
  mutate(Season = 2025)

mean_curvePORA <- mean_curvePO2024RA |>
  select(c(StorageDays,estimate,Median,low,high,Season)) |>
  bind_rows(mean_curvePO2025RA) |>
  mutate(`Storage type` = "RA")

# Generate synthetic data for 2025

newdata <- StorageDaysRASc1

meanPO2025syn <- data.frame(estimate = predict(NLmodel2025RA,
                                               newdata = newdata)) |>
  bind_cols(newdata) |>
  mutate(Season = 2025,
         `Storage type` = "RA")

PackoutEndRA <- predict(NLmodel2025RA,newdata = tibble(StorageDays = 166))

modelCAPO2025 <- tibble(StorageDays = seq(166,264,1)) |>
  mutate(Packout = PackoutEndRA+meanCAPO$CAUplift[[1]],
         Season = 2025,
         `Storage type` = "CA")

########################################################################################
#         Incorporates data for the seasonal packout                                   #
########################################################################################








