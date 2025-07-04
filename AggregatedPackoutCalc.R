con <- DBI::dbConnect(odbc::odbc(),    
                      Driver = "ODBC Driver 18 for SQL Server", 
                      Server = "abcrepldb.database.windows.net",  
                      Database = "ABCPackerRepl",   
                      UID = "abcadmin",   
                      PWD = "Trauts2018!",
                      Port = 1433
)

BinsToGo <- DBI::dbGetQuery(con, getSQL("../PackoutPrediction2025/SQLfiles/BinsToGo.sql"))

AlreadyPacked <- DBI::dbGetQuery(con, getSQL("../PackoutPrediction2025/SQLfiles/AlreadyPacked.sql"))

DBI::dbDisconnect(con)

################################################################################
#      Look at actual storage day distribution for remaining RA fruit          #
################################################################################

WAHarvestDate <- BinsToGo |>
  mutate(`Storage type` = if_else(StorageTypeID == 4, "CA", "RA")) |>
  select(-StorageTypeID) |>
  group_by(`Storage type`) |>
  summarise(HarvestDate = weighted.mean(HarvestDate, BinsToGo),
            BinsToGo = sum(BinsToGo))


FuturePackSummary <- function(PackProgrammeToGo,WAHarvestDate,WAMassPerBin,NLmodel2025RA) {

  FuturePackDataRA <- PackProgrammeToGo |>
    filter(RABinsTipped > 0) |>
    mutate(weekdate = str_c("2025-W",isoweek,"-3"),
           PackDate = ISOweek::ISOweek2date(weekdate),
           StorageDays = as.integer(PackDate-WAHarvestDate[[2,2]]),
           CumBinsTipped = cumsum(RABinsTipped),
           InputKgs = RABinsTipped*WAMassPerBin$MassPerBin[[2]])

  Packout <- tibble(Packout = predict(NLmodel2025RA, newdata = tibble(StorageDays = FuturePackDataRA$StorageDays))) 

  FinalFuturePackDatesRA <- FuturePackDataRA |>
    bind_cols(Packout) |>
    mutate(ExportKgs = Packout*InputKgs) |>
    select(c(PackDate,StorageDays,RABinsTipped,InputKgs,ExportKgs)) 

  return(FinalFuturePackDatesRA)
  
}

FPSRABase <- FuturePackSummary(PackProgrammeToGoOrig,WAHarvestDate,WAMassPerBin,NLmodel2025RA)
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

################################################################################
#                           CA Bin calculation                                 #
################################################################################
#
# First calculate the end of RA packing for each scenario
#

WeekCABegins <- function(PackProgrammeToGo, Scenario) {
  
  FuturePackDataCA <- PackProgrammeToGo |>
    filter(CABinsTipped > 0) 
  
  WeekCABegins <- tibble(`CA start week` = FuturePackDataCA$isoweek[[1]],
                         Scenario = {{Scenario}})
  
  return(WeekCABegins)
  
}

CBOrig <- WeekCABegins(PackProgrammeToGoOrig,"Base")
CBSc1 <- WeekCABegins(PackProgrammeToGoSc1,"Base-8,000")
CBSc2 <- WeekCABegins(PackProgrammeToGoSc2,"Base-10,000")

CB <- CBOrig |>
  bind_rows(CBSc1) |>
  bind_rows(CBSc2) 


################################################################################
#                          RA Bins already Packed                              #
################################################################################

AlreadyPacked2025RA <- AlreadyPacked |>
  mutate(ExportKgs = InputKgs-RejectKgs) |>
  select(c(InputKgs,ExportKgs)) |>
  summarise(InputKgs = sum(InputKgs),
            ExportKgs = sum(ExportKgs)) |>
  mutate(Packout = ExportKgs/InputKgs)

################################################################################
#              Calculate the CA numbers                                        #
################################################################################

FuturePackCASummary <- function(PackProgrammeToGo,WAHarvestDate,WAMassPerBin,CAUplift) {
  
  FuturePackDataCA <- PackProgrammeToGo |>
    filter(CABinsTipped > 0) |>
    mutate(weekdate = str_c("2025-W",isoweek,"-3"),
           PackDate = ISOweek::ISOweek2date(weekdate),
           StorageDays = as.integer(PackDate-WAHarvestDate[[1,2]]),
           CumBinsTipped = cumsum(CABinsTipped),
           InputKgs = CABinsTipped*WAMassPerBin$MassPerBin[[2]],
           Packout = {{CAUplift}}[[1]],
           ExportKgs = Packout*InputKgs) |>
    select(c(PackDate,StorageDays,CABinsTipped,InputKgs,ExportKgs)) 

  return(FuturePackDataCA)
  
}

FPSCABase <- FuturePackCASummary(PackProgrammeToGoBase,WAHarvestDate,WAMassPerBin,CA2025Base)
FPSCASc1 <- FuturePackCASummary(PackProgrammeToGoSc1,WAHarvestDate,WAMassPerBin,CA2025Sc1)
FPSCASc2 <- FuturePackCASummary(PackProgrammeToGoSc2,WAHarvestDate,WAMassPerBin,CA2025Sc2)

#################################################################################
#                     Assemble all PO components together                       #
#################################################################################

SeasonalPackouts <- function(FPSRA,FPSCA,AlreadyPacked) {
  
  WeeklyList <- FPSRA |>
    bind_rows(FPSCA) |>
    mutate(BinsTipped = coalesce(RABinsTipped, CABinsTipped)) |>
    select(-c(RABinsTipped, CABinsTipped)) 
  
  ToBePacked <- WeeklyList |>
    ungroup() |>
    summarise(InputKgs = sum(InputKgs),
              ExportKgs = sum(ExportKgs)) |>
    mutate(Packout = ExportKgs/InputKgs)
  
  SeasonPackout <- ToBePacked |>
    bind_rows(AlreadyPacked2025RA) |>
    ungroup() |>
    summarise(InputKgs = sum(InputKgs),
              ExportKgs = sum(ExportKgs)) |>
    mutate(Packout = ExportKgs/InputKgs)
  
  return(SeasonPackout)

}

SPBase <- SeasonalPackouts(FPSRABase,FPSCABase,AlreadyPacked2025RA)
SPSc1 <- SeasonalPackouts(FPSRASc1,FPSCASc1,AlreadyPacked2025RA)
SPSc2 <- SeasonalPackouts(FPSRASc2,FPSCASc2,AlreadyPacked2025RA)

SPBase |>
  bind_rows(SPSc1) |>
  bind_rows(SPSc2) |>
  mutate(Scenario = c("Base", "Base-8,000", "Base-10,000"),
         across(.cols = c(InputKgs, ExportKgs),~scales::comma(.,1.0)),
         Packout = scales::percent(Packout, 0.01)) |>
  relocate(Scenario, .before = InputKgs) |>
  rename(`Input kgs` = InputKgs,
         `Export kgs` = ExportKgs) |>
  flextable::flextable() |>
  flextable::autofit()


