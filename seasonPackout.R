con <- DBI::dbConnect(odbc::odbc(),    
                      Driver = "ODBC Driver 18 for SQL Server", 
                      Server = "abcrepldb.database.windows.net",  
                      Database = "ABCPackerRepl",   
                      UID = "abcadmin",   
                      PWD = "Trauts2018!",
                      Port = 1433
)

WeeklyBins <- DBI::dbGetQuery(con, getSQL("../SQLQueryRepo/SQLFiles2024/WeeklyBins.sql"))

BinDelivery <- DBI::dbGetQuery(con, getSQL("../SQLQueryRepo/SQLFiles2024/BinDelivery.sql"))

BinsToGo <- DBI::dbGetQuery(con, getSQL("../PackoutPrediction2025/SQLfiles/BinsToGo.sql"))

AlreadyPacked <- DBI::dbGetQuery(con, getSQL("../PackoutPrediction2025/SQLfiles/AlreadyPacked.sql"))

DBI::dbDisconnect(con)

#PackingSummary <- WeeklyBins |>
#  mutate(PackWeek = isoweek(PackDate),
#         Storage = case_when(`Storage type` == "CA Smartfresh" ~ "CA",
#                             `Storage type` == "Normal Air" ~ "RA",
#                             `Storage type` == "Smartfreshed" ~ "RA",
#                             TRUE ~ "RA")) |>
#  group_by(Season, `Packing site`, `Storage`, PackWeek) |>
#  summarise(Batches = n(),
#            BinQty = sum(BinQty, na.rm=T),
#            .groups = "drop") 

#WeeklyPackingTotalsByPackSite <- PackingSummary |>
#  group_by(Season, `Packing site`) |>
#  summarise(BinsPerWeek = mean(BinQty),
#            BatchesPerWeek = mean(Batches, na.rm=T),
#            .groups = "drop")

#WeeklyPackingTotalsBySeason <- PackingSummary |>
#  group_by(Season) |>
#  summarise(BinsPerWeek = mean(BinQty),
#            BatchesPerWeek = mean(Batches, na.rm=T)) |>
#  mutate(BinsPerBatch = BinsPerWeek/BatchesPerWeek)

TotalBinsRA <- WeeklyBins |>
  mutate(Storage = case_when(`Storage type` == "CA Smartfresh" ~ "CA",
                             `Storage type` == "Normal Air" ~ "RA",
                             `Storage type` == "Smartfreshed" ~ "RA",
                             TRUE ~ "RA")) |>
  filter(Storage == "RA") |>
  group_by(Season) |>
  summarise(BinQty = sum(BinQty, na.rm=T))

TotalBinsCA <- WeeklyBins |>
  mutate(Storage = case_when(`Storage type` == "CA Smartfresh" ~ "CA",
                             `Storage type` == "Normal Air" ~ "RA",
                             `Storage type` == "Smartfreshed" ~ "RA",
                             TRUE ~ "RA")) |>
  filter(Storage == "CA") |>
  group_by(Season) |>
  summarise(BinQty = sum(BinQty, na.rm=T))

if(nrow(TotalBinsCA) < 2) {
  temp <- tibble(Season = as.character(2025),
                 BinQty = 0)

  TotalBinsCA <- TotalBinsCA |>
    bind_rows(temp)
}

TotalSeasonalBinsRA <- BinDelivery |>
  mutate(Storage = case_when(`Storage type` == "CA Smartfresh" ~ "CA",
                             `Storage type` == "Normal Air" ~ "RA",
                             `Storage type` == "Smartfreshed" ~ "RA",
                             TRUE ~ "RA")) |>
  filter(Storage == "RA") |>
  group_by(Season) |>
  summarise(NoOfBins = sum(NoOfBins))

TotalSeasonalBinsCA <- BinDelivery |>
  mutate(Storage = if_else(`Storage type` == "CA Smartfresh",
                           "CA","RA")) |>
  filter(Storage == "CA") |>
  group_by(Season) |>
  summarise(NoOfBins = sum(NoOfBins))

WAHarvestDate <- BinsToGo |>
  mutate(`Storage type` = if_else(StorageTypeID == 4, "CA", "RA")) |>
  select(-StorageTypeID) |>
  group_by(`Storage type`) |>
  summarise(HarvestDate = weighted.mean(HarvestDate, BinsToGo),
            BinsToGo = sum(BinsToGo))

WAMassPerBin <- WeeklyBins |>
  filter(!is.na(InputKgs),
         !is.na(BinQty)) |>
  group_by(Season) |>
  summarise(BinQty = sum(BinQty),
            InputKgs = sum(InputKgs)) |>
  mutate(MassPerBin = InputKgs/BinQty)

RemainingBinsRA <- (TotalSeasonalBinsRA$NoOfBins[[2]]-TotalBinsRA$BinQty[[2]])

RemainingBinsRASc1 <- RemainingBinsRA-8000
RemainingBinsRAsc2 <- RemainingBinsRA-10000

RemainingBinsCA <- (TotalSeasonalBinsCA$NoOfBins[[2]]-TotalBinsCA$BinQty[[2]])
#
# Specifiy the finish date and calculate the number fo bins per week
#
# Generate tibble for the packing programme as given by Josh Buick
#

SiteTipProgramme <- tibble(isoweek = c(seq(from = 27, to = 50)),
                        BinsTeIpu = c(rep(575,9),1410,1175,rep(1410,5),940,940,rep(1410,6)),
                        BinsSunfruit = c(rep(0,7),1250,0,0,900,0,900,0,900,0,975,0,0,rep(1250,5)),
                        BinsKiwiCrunch = c(700,0,0,585,0,0,585,0,585,rep(0,15))) |>
  rowwise() |>
  mutate(TotalBins = sum(c_across(BinsTeIpu:BinsKiwiCrunch)),
         RABinsTipped = 0,
         RemainingRABins = 0,
         CABinsTipped = 0,
         RemainingCABins = 0)

#
# Create function with logic to assign a sequential RA/CA packing plan
#

PackProgramme <- function(SiteTipProgramme, RemainingRA, RemainingCA) {

  for (i in 1:nrow(SiteTipProgramme)) {
    if (i == 1) {
      SiteTipProgramme$RemainingRABins[i] <-  {{RemainingRA}}-SiteTipProgramme$TotalBins[i]
      SiteTipProgramme$RABinsTipped[i] <- SiteTipProgramme$TotalBins[i]
    } else {
      if (SiteTipProgramme$RemainingRABins[i-1]-SiteTipProgramme$TotalBins[i] >= 0) {
        SiteTipProgramme$RemainingRABins[i] <-  SiteTipProgramme$RemainingRABins[i-1]-SiteTipProgramme$TotalBins[i]
        SiteTipProgramme$RABinsTipped[i] <- SiteTipProgramme$TotalBins[i]
      } else {
        SiteTipProgramme$RemainingRABins[i-1] 
        SiteTipProgramme$RABinsTipped[i] <-  SiteTipProgramme$RemainingRABins[i-1] 
        break
      }
    }
  }

  for (j in 1:nrow(SiteTipProgramme)) {
    if(SiteTipProgramme$RemainingRABins[j]>0 & SiteTipProgramme$RABinsTipped[j] == SiteTipProgramme$TotalBins[j]) {
      SiteTipProgramme$RemainingCABins[j] <- {{RemainingCA}}
      SiteTipProgramme$CABinsTipped[j] <- 0
    } else if (SiteTipProgramme$RABinsTipped[j]>0 & SiteTipProgramme$RemainingRABins[j] == 0) {
      SiteTipProgramme$RemainingCABins[j] <- RemainingBinsCA-(SiteTipProgramme$TotalBins[j]-SiteTipProgramme$RABinsTipped[j])
      SiteTipProgramme$CABinsTipped[j] <- SiteTipProgramme$TotalBins[j]-SiteTipProgramme$RABinsTipped[j]
    } else if (SiteTipProgramme$RABinsTipped[j] == 0 & SiteTipProgramme$RemainingRABins[j] == 0) {
      if (SiteTipProgramme$RemainingCABins[j-1] >= SiteTipProgramme$TotalBins[j]) {
        SiteTipProgramme$RemainingCABins[j] <- SiteTipProgramme$RemainingCABins[j-1]-SiteTipProgramme$TotalBins[j]
        SiteTipProgramme$CABinsTipped[j] <- SiteTipProgramme$TotalBins[j]
      } else {
        SiteTipProgramme$RemainingCABins[j] <- 0
        SiteTipProgramme$CABinsTipped[j] <- SiteTipProgramme$RemainingCABins[j-1]
      }
    } 
  }
  
  return(SiteTipProgramme)
  
}

#
# Run Functions for the two scenarios (i.e. -5,000 and -10,000 bins)
#
PackProgrammeToGoBase <- PackProgramme(SiteTipProgramme, RemainingBinsRA, RemainingBinsCA)  
PackProgrammeToGoSc1 <- PackProgramme(SiteTipProgramme, RemainingBinsRASc1, RemainingBinsCA)  
PackProgrammeToGoSc2 <- PackProgramme(SiteTipProgramme, RemainingBinsRAsc2, RemainingBinsCA) 

write_csv(PackProgrammeToGoBase, "PPTGBase.csv")
write_csv(PackProgrammeToGoSc1, "PPTGSc1.csv")
write_csv(PackProgrammeToGoSc2, "PPTGSc2.csv")

#
# Genrate the RA packout for each scenario
#

FuturePackSummary <- function(PackProgrammeToGo,WAHarvestDate,WAMassPerBin,NLmodel2025RA) {
  
  FuturePackDataRA <- PackProgrammeToGo |>
    filter(RABinsTipped > 0) |>
    mutate(weekdate = str_c("2025-W",isoweek,"-3"),
           PackDate = ISOweek::ISOweek2date(weekdate),
           StorageDays = as.integer(PackDate-WAHarvestDate[[2,2]]),
           CumBinsTipped = cumsum(RABinsTipped),
           InputKgs = RABinsTipped*WAMassPerBin[[1,1]])
  
  Packout <- tibble(Packout = predict(NLmodel2025RA, newdata = tibble(StorageDays = FuturePackDataRA$StorageDays))) 
  
  FinalFuturePackDatesRA <- FuturePackDataRA |>
    bind_cols(Packout) |>
    mutate(ExportKgs = Packout*InputKgs) |>
    select(c(PackDate,StorageDays,RABinsTipped,InputKgs,ExportKgs)) 
  
  return(FinalFuturePackDatesRA)
  
}

FPSRABase <- FuturePackSummary(PackProgrammeToGoBase,WAHarvestDate,WAMassPerBin,NLmodel2025RA)
FPSRASc1 <- FuturePackSummary(PackProgrammeToGoSc1,WAHarvestDate,WAMassPerBin,NLmodel2025RA)
FPSRASc2 <- FuturePackSummary(PackProgrammeToGoSc2,WAHarvestDate,WAMassPerBin,NLmodel2025RA)

FPSRABase |>
  ungroup() |>
  summarise(InputKgs = sum(InputKgs),
            ExportKgs = sum(ExportKgs)) |>
  mutate(Packout = ExportKgs/InputKgs)

FPSRASc1 |>
  ungroup() |>
  summarise(InputKgs = sum(InputKgs),
            ExportKgs = sum(ExportKgs)) |>
  mutate(Packout = ExportKgs/InputKgs)

FPSRASc2 |>
  ungroup() |>
  summarise(InputKgs = sum(InputKgs),
            ExportKgs = sum(ExportKgs)) |>
  mutate(Packout = ExportKgs/InputKgs)

StorageDayEstimates <- function(PackProgrammeToGo, WAHarvestDate, Scenario) {

  FuturePackDataRA <- PackProgrammeToGo |>
    filter(RABinsTipped > 0) |>
    mutate(weekdate = str_c("2025-W",isoweek,"-3"),
           PackDate = ISOweek::ISOweek2date(weekdate),
           StorageDays = as.integer(PackDate-WAHarvestDate[[2,2]]))

  FuturePackDataCA <- PackProgrammeToGo |>
    filter(CABinsTipped > 0) |>
    mutate(weekdate = str_c("2025-W",isoweek,"-3"),
           PackDate = ISOweek::ISOweek2date(weekdate),
           StorageDays = as.integer(PackDate-WAHarvestDate[[1,2]]))

  StorageDayCuts <- tibble(`Storage type` = c("RA","CA"),
                           StorageDays_Start = c(FuturePackDataRA$StorageDays[[1]],
                                                FuturePackDataCA$StorageDays[[1]]),
                           StorageDays_End = c(FuturePackDataRA$StorageDays[[nrow(FuturePackDataRA)]],
                                           FuturePackDataCA$StorageDays[[nrow(FuturePackDataCA)]]),
                           Scenario = {{Scenario}}) |>
    ungroup()
  
  return(StorageDayCuts)

}


SDEBase <- StorageDayEstimates(PackProgrammeToGoBase,WAHarvestDate,"Base")
SDESc1 <- StorageDayEstimates(PackProgrammeToGoSc1,WAHarvestDate,"Base-8,000")
SDESc2 <- StorageDayEstimates(PackProgrammeToGoSc2,WAHarvestDate,"Base-10,000")

WeekCABeginsEnds <- function(PackProgrammeToGo, Scenario) {
  
  FuturePackDataCA <- PackProgrammeToGo |>
    filter(CABinsTipped > 0) 

  WeekCABegins <- tibble(`CA start week` = FuturePackDataCA$isoweek[[1]],
                         `CA finish week` = FuturePackDataCA$isoweek[[nrow(FuturePackDataCA)]],
                           Scenario = {{Scenario}})
  
  return(WeekCABegins)
  
}

CBBase <- WeekCABeginsEnds(PackProgrammeToGoBase,"Base")
CBSc1 <- WeekCABeginsEnds(PackProgrammeToGoSc1,"Base-8,000")
CBSc2 <- WeekCABeginsEnds(PackProgrammeToGoSc2,"Base-10,000")

CB <- CBBase |>
  bind_rows(CBSc1) |>
  bind_rows(CBSc2) 


SDEBase |>
  bind_rows(SDESc1) |>
  bind_rows(SDESc2) |>
  pivot_wider(id_cols = Scenario,
              names_from = `Storage type`,
              values_from = c(StorageDays_Start,StorageDays_End)) |>
  rename(StorageDays_RA_Start = StorageDays_Start_RA,
         StorageDays_RA_End = StorageDays_End_RA,
         StorageDays_CA_Start = StorageDays_Start_CA,
         StorageDays_CA_End = StorageDays_End_CA) |>
  relocate(StorageDays_RA_End, .after = StorageDays_RA_Start) |>
  left_join(CB, by = "Scenario") |>
  relocate(`CA start week`, .after = Scenario) |>
  relocate(`CA finish week`, .after = `CA start week`) |>
  rename(CA_Week_Start = `CA start week`,
         CA_Week_Finish = `CA finish week`) |>
  flextable::flextable() |>
  flextable::separate_header() |>
  flextable::bold(bold=T, part="header") |>
  flextable::autofit() 


POPlotRA <- function(NLmodel2025RA, yield.boot2025, SDE, Scenario) {
  
  StorageDaysRA <- tibble(StorageDays = seq(0,SDE[[1,3]],1))

  meanPO2025 <- data.frame(estimate = predict(NLmodel2025RA,
                                            newdata = StorageDaysRA))
  CIPO2025 <- data.frame(nlstools::nlsBootPredict(yield.boot2025, 
                                                newdata = StorageDaysRA, 
                                                interval = "confidence"))

  mean_curvePO2025RA <- StorageDaysRA |> 
    bind_cols(meanPO2025) |>
    bind_cols(CIPO2025) |>
    rename(low = X2.5.,
           high = X97.5.) |>
    mutate(Season = 2025,
           `Storage type` = "RA",
           Scenario = {{Scenario}})
  
  return(mean_curvePO2025RA)

}
  
POPlotRABase <- POPlotRA(NLmodel2025RA, yield.boot2025, SDEBase, "Base")
POPlotRASc1 <- POPlotRA(NLmodel2025RA, yield.boot2025, SDESc1, "Base-8,000")
POPlotRASc2 <- POPlotRA(NLmodel2025RA, yield.boot2025, SDESc2, "Base-10,000")

POPlotRA <- POPlotRABase |>
  bind_rows(POPlotRASc1) |>
  bind_rows(POPlotRASc2) |>
  mutate(Scenario = factor(Scenario, levels = c("Base","Base-8,000","Base-10,000")))

#
# make plot for the scattergraph
#
POAnalysisRASc1 <- POAnalysis2025RA |>
  mutate(Scenario = "Base-8,000") 

POAnalysisRASc2 <- POAnalysis2025RA |>
  mutate(Scenario = "Base-10,000") 

POAnalysisRA <- POAnalysis2025RA |>
  mutate(Scenario = "Base") |>
  bind_rows(POAnalysisRASc1) |>
  bind_rows(POAnalysisRASc2) |>
  mutate(Scenario = factor(Scenario, levels = c("Base","Base-8,000","Base-10,000")))

#
# determine CA start values
#

EndRABase <- tibble(StorageDays = predict(NLmodel2025RA, newdata = SDEBase[1,3] |> rename(StorageDays = StorageDays_End)))
EndRASc1 <- tibble(StorageDays = predict(NLmodel2025RA, newdata = SDESc1[1,3] |> rename(StorageDays = StorageDays_End)))
EndRASc2 <- tibble(StorageDays = predict(NLmodel2025RA, newdata = SDESc2[1,3] |> rename(StorageDays = StorageDays_End)))

CA2025Base = EndRABase + meanCAPO$CAUplift[[1]]
CA2025Sc1 = EndRASc1 + meanCAPO$CAUplift[[1]]
CA2025Sc2 = EndRASc2 + meanCAPO$CAUplift[[1]]

CA2025PO <- CA2025Base |>
  bind_rows(CA2025Sc1) |>
  bind_rows(CA2025Sc2) |>
  mutate(Scenario = c("Base", "Base-8,000", "Base-10,000"))

#
# Generate the CA Data frames
#

CAPO2025Sc1 <- tibble(StorageDays = seq(SDESc1[[2,2]],SDESc1[[2,3]],1),
                    Packout = CA2025Sc1[[1]],
                    Scenario = "Base-8,000")

CAPO2025Sc2 <- tibble(StorageDays = seq(SDESc2[[2,2]],SDESc2[[2,3]],1),
                    Packout = CA2025Sc2[[1]],
                    Scenario = "Base-10,000")


CAPO2025 <- tibble(StorageDays = seq(SDEBase[[2,2]],SDEBase[[2,3]],1),
                 Packout = CA2025Base[[1]],
                 Scenario = "Base") |>
  bind_rows(CAPO2025Sc1) |>
  bind_rows(CAPO2025Sc2) |>
  mutate(Scenario = factor(Scenario, levels = c("Base","Base-8,000","Base-10,000")))
  



POPlotRA |>
  ggplot() +
  geom_line(aes(StorageDays, estimate),colour = "#48762e", linewidth=1) +
  facet_wrap(~Scenario, ncol = 1) +
  geom_ribbon(aes(x=StorageDays, ymin = low, ymax = high, group = factor(Season)), fill="grey50", alpha=0.5) +
  geom_point(data = POAnalysisRA, aes(x=StorageDays, y=PackOut), colour = "#48762e", alpha=0.5) +
  geom_line(data = CAPO2025, aes(StorageDays, Packout), colour = "#a9342c", linewidth = 1) +
  scale_y_continuous("Packout / %", labels = scales::label_percent(1.0)) +
  labs(x = "Storage days") +
  scale_fill_manual(values = c("#a9342c","#48762e","#526280","#f6c15f")) +
  scale_colour_manual(values = c("#a9342c","#48762e","#526280","#f6c15f")) +
  ggthemes::theme_economist() + 
  theme(legend.position = "top",
        axis.title.x = element_text(margin = margin(t = 10), size = 10),
        axis.title.y = element_text(margin = margin(r = 10), size = 10),
        axis.text.y = element_text(size = 10, hjust=1),
        axis.text.x = element_text(size = 10),
        plot.background = element_rect(fill = "#F7F1DF", colour = "#F7F1DF"),
        legend.text = element_text(size = 10),
        legend.title = element_text(size = 10),
        strip.text = element_text(margin = margin(b=10), size = 10))


POAnalysis |>
  filter(Season == "2025",
         !is.na(StorageDays),
         !is.na(PackOut)) |>
  ggplot() +
  geom_point(aes(x=StorageDays,y=PackOut,colour=`Storage type`), alpha = 0.3) +
  facet_wrap(~Season, ncol = 1) +
  geom_line(data = mean_curvePORA, aes(x=StorageDays, y=estimate, group = factor(Season), colour = `Storage type`), linewidth=1) +
  geom_line(data = meanPO2025syn, aes(x=StorageDays, y=estimate, group = factor(Season), colour = `Storage type`),linewidth = 1) +
  geom_line(data = CAModeled2024, aes(x=StorageDays, y=estimate, group=factor(Season), colour = `Storage type`), linewidth = 1) +
  geom_line(data = modelCAPO2025, aes(x=StorageDays, y=Packout, group=factor(Season), colour = `Storage type`), linewidth = 1) +
  geom_ribbon(data = mean_curvePORA, 
              aes(x=StorageDays, ymin = low, ymax = high, group = factor(Season)), fill="grey50", alpha=0.5) +
  geom_ribbon(data = CAModeled2024,
              aes(x = StorageDays, ymin = lwr, ymax = upr, group = factor(Season)), fill="grey50", alpha = 0.5) +
  labs(x = "Storage days",
       y = "Packout / %") +
  scale_y_continuous(labels = scales::label_percent(1.0)) +
  scale_fill_manual(values = c("#a9342c","#48762e","#526280","#f6c15f")) +
  scale_colour_manual(values = c("#a9342c","#48762e","#526280","#f6c15f")) +
  ggthemes::theme_economist() + 
  theme(legend.position = "top",
        axis.title.x = element_text(margin = margin(t = 10), size = 10),
        axis.title.y = element_text(margin = margin(r = 10), size = 10),
        axis.text.y = element_text(size = 10, hjust=1),
        axis.text.x = element_text(size = 10),
        plot.background = element_rect(fill = "#F7F1DF", colour = "#F7F1DF"),
        legend.text = element_text(size = 10),
        legend.title = element_text(size = 10),
        strip.text = element_text(margin = margin(b=10), size = 10))


