library(stringr)
library(DBI)
library(RMySQL)
library(datapuppy)
library(zoo)
library(lubridate)


# An edit to see if it shows up in version control.

MySQL(max.con = 100, fetch.default.rec = 1000)

###levellogger data
csvFilesRawDataCampaigns = solinstDataCampaignExtract("/Users/aquageoecollc/Documents/R/meacham_creek/meachamDatabase/data/levelloggers/campaign013", "data/levelloggers/campaign013")
baroCsvFilesRawDataCampaigns = solinstDataCampaignExtract("/Users/aquageoecollc/Documents/R/meacham_creek/meachamDatabase/baroData/levellogger/campaign013", "baroData/levellogger/campaign013")

csvFiles = csvFilesRawDataCampaigns$csvFiles
rawData = csvFilesRawDataCampaigns$rawData
loggerSerialNumbers = csvFilesRawDataCampaigns$loggerSerialNumber
campaign = csvFilesRawDataCampaigns$campaigns

SolinstDataFrames <- SolinstDataFramesList(csvFiles, suffix = "data/levelloggers/campaign013", columnRange = 2:3, dayOrHour = "hour", headerIndex = 14)

baroCsvFiles = baroCsvFilesRawDataCampaigns$csvFiles
baroRawData = baroCsvFilesRawDataCampaigns$rawData
baroLoggerSerialNumbers = baroCsvFilesRawDataCampaigns$loggerSerialNumber
baroCampaign = baroCsvFilesRawDataCampaigns$campaigns

SolinstBaroDataFrames <- SolinstAPDataFramesList(baroCsvFiles, suffix = "baroData/levellogger/campaign013", columnRange = 2, dayOrHour = "hour", headerIndex = 13)

####### Baro Set stuff
myConnArgs = dpConnectionArgs(drv = MySQL(), dbname = "meachamcreek11012016", user = "aquageoecollc", password = "h0ly,sh1t", host = "localhost", port = 3306)
myAPConn = dpConnect(myConnArgs)
myAPSet = dpSet("/Users/aquageoecollc/Documents/R/meacham_creek/meachamDatabase/dpAPSet",
              myConnArgs,
              
              batchesTableName = "deployment",
              batchNameColumnName = "deployment_name",
              
              dataTableName = "APobservation",
              batchRowColumnName = "batch_row_id",
              datumValueColumnName = "data_value",
              
              typesTableName = "APmetrics",
              datumTypeColumnName = "metric_name"
)

####### Set stuff
myConnArgs = dpConnectionArgs(drv = MySQL(), dbname = "meachamcreek11012016", user = "aquageoecollc", password = "h0ly,sh1t", host = "localhost", port = 3306)
myConn = dpConnect(myConnArgs)
mySet = dpSet("/Users/aquageoecollc/Documents/R/meacham_creek/meachamDatabase/dpSet",
              myConnArgs,
              
              batchesTableName = "deployment",
              batchNameColumnName = "deployment_name",
              
              dataTableName = "observation",
              batchRowColumnName = "batch_row_id",
              datumValueColumnName = "data_value",
              
              typesTableName = "metrics",
              datumTypeColumnName = "metric_name"
)

mySet = dpLoadSet("/Users/aquageoecollc/Documents/R/meacham_creek/meachamDatabase/dpSet")

###### level logger stuff

dbDisconnect(con)
drv <- dbDriver("MySQL")
con <- dbConnect(drv, port = 3306, host = "localhost", user="aquageoecollc", password="h0ly,sh1t", dbname="meachamcreek11012016")

#wellsCampaignsInstallations = dbGetQuery(con, "SELECT * FROM wells_campaigns_installations")
wellsCampaignsInstallationsInstruments <- dbGetQuery(con,
"SELECT
wells_campaigns_installations.site_name,
wells_campaigns_installations.serial_number,
wells_campaigns_installations.campaign,
wells_campaigns_installations.installation_id,
instrument.id AS 'instrument_id',
instrument.builtin_offset,
installation.instrument_depth,
installation.z_coord,
installation.z_offset
FROM
wells_campaigns_installations,
instrument,
installation
WHERE
wells_campaigns_installations.serial_number = instrument.serial_number AND
wells_campaigns_installations.installation_id = installation.installation_id;")

dbDisconnect(con)

#### level logger meta data
campaignSubset = subset(wellsCampaignsInstallationsInstruments, wellsCampaignsInstallationsInstruments$campaign %in% "campaign013")
campaignSubset <- campaignSubset[order(campaignSubset[,2]),]
installationIDX = subset(campaignSubset$installation_id,  campaignSubset$serial_number %in% loggerSerialNumbers)
instrumentIDX <- subset(campaignSubset$instrument_id,  campaignSubset$serial_number %in% loggerSerialNumbers)
deploymentName = paste0(campaign, "_", campaignSubset$serial_number[campaignSubset$serial_number %in% loggerSerialNumbers])
instrument_depth = subset(campaignSubset$instrument_depth, campaignSubset$serial_number %in% loggerSerialNumbers) / 39.3701
builtin_offset = subset(campaignSubset$builtin_offset, campaignSubset$serial_number %in% loggerSerialNumbers) 
z_offset = subset(campaignSubset$z_offset, campaignSubset$serial_number %in% loggerSerialNumbers) / 3.28084
z_coord = subset(campaignSubset$z_coord, campaignSubset$serial_number %in% loggerSerialNumbers) / 3.28084

#### baro logger meta data
baroInstallationIDX = subset(campaignSubset$installation_id,  campaignSubset$serial_number %in% baroLoggerSerialNumbers)
baroInstrumentIdx = subset(campaignSubset$instrument_id, campaignSubset$serial_number %in% baroLoggerSerialNumbers)
baroDeploymentName = paste0(baroCampaign, "_", baroLoggerSerialNumbers)

#### baro Logger batch
baroMediumID = list(1)
baroBatchRec = Map(
  function(dn, instrID, installID, mediumID) 
    list(
      deployment_name = dn, 
      instrument_id = instrID, 
      installation_id = installID,
      medium_id = mediumID), 
  baroDeploymentName, 
  baroInstrumentIdx, 
  baroInstallationIDX,
  baroMediumID
)

baroBatch = Map(
  function(br, ms, d, rd) 
    dpBatch(br, ms, d, rd),
  baroBatchRec,
  list(myAPSet),
  SolinstBaroDataFrames,
  baroRawData
)

#### Tweak this, bitch.

myTweakedAPBatch = dpTweak(baroBatch[[1]], meters2kPa, args = list(columnName = "pressure"), "convert meters of air pressure into kPa using Solinst correction factor of 0.102")

#commit level logger batches to the database
lapply(baroBatch, dpCommitBatchData)

#to reload batches
baroBatch = lapply(baroDeploymentName, dpLoadBatch, set = myAPSet)

#### level logger
mediumID = list(1)
LevelBatchRec = Map(
  function(dn, instrID, installID, mediumID) 
    list(
      deployment_name = dn, 
      instrument_id = instrID, 
      installation_id = installID,
      medium_id = mediumID), 
  deploymentName, 
  instrumentIDX, 
  installationIDX,
  mediumID
)

levelBatch = Map(
  function(br, ms, d, rd) 
    dpBatch(br, ms, d, rd),
  LevelBatchRec,
  list(mySet),
  SolinstDataFrames,
  rawData
)

#### tweak batches

dbDisconnect(con)
drv <- dbDriver("MySQL")
con <- dbConnect(drv, port = 3306, host = "localhost", user="aquageoecollc", password="h0ly,sh1t", dbname="meachamcreek11012016")

pressure = dbGetQuery(
  con, 
  "SELECT
  APobservation.date_time,
  APobservation.data_value
  FROM meachamcreek11012016.APobservation;")

dbDisconnect(con)


rawLevelsZoo = testThis(SolinstDataFrames, 2)
#pressureZoo = testThis(SolinstBaroDataFrames, 2)
pressureZoo = testThis(list(pressure), 2)
rawTemperatureZoo = testThis(SolinstDataFrames, 3)

rawLevelsWindow <- lapply(rawLevelsZoo, function(x, s, e) window(x, start = ymd_hms("2016-8-18 09:00:00"), end = ymd_hms("2016-8-18 09:00:00")))
rawPressureWindow <- lapply(pressureZoo, function(x, s, e) window(x, start = ymd_hms("2016-8-18 09:00:00"), end = ymd_hms("2016-8-18 09:00:00")))
temperatureWindow <- lapply(rawTemperatureZoo, function(x, s, e) window(x, start = ymd_hms("2016-8-18 09:00:00"), end = ymd_hms("2016-8-18 09:00:00")))

offsetCorrectedRawLevels <- Map(function(x, y) x - y, rawLevelsWindow, builtin_offset)
pressureCorrectedRawLevels = Map(function(x, y) x - (y * .102), offsetCorrectedRawLevels, rawPressureWindow )

#### QA/QC first
depthToH2O <- Map(function(x,y) (x - y) * 3.28084, instrument_depth, pressureCorrectedRawLevels)
wellNames <- subset(campaignSubset$site_name,  campaignSubset$serial_number %in% loggerSerialNumbers)
names(depthToH2O) <- wellNames
fieldDepsthToH2O <- c(6.85, 5.27, 7.02, 5.24, 7.0, 5.52, 8.65, 7, 7.4, 7.37, 7, 3.83, 5.79, 6.22, 6.71, 4.7, 7, 6.03, 3.8) 
diffsFeet = Map(function(x, y) (x - y) , depthToH2O, fieldDepsthToH2O)
diffsMeters = Map(function(x, y) (x - y) / 3.28084 , depthToH2O, fieldDepsthToH2O)

#SOLINST change in density with temperature 
## p1 = p0 / (1 + b * (t1 - t0)) #http://www.engineeringtoolbox.com/fluid-density-temperature-pressure-d_309.html
waterDensity = lapply(rawTemperatureZoo, function(x) 1000 / (1 + 0.0002 * (x - 4)))

rawLevelsKpa = lapply(offsetCorrectedRawLevels, function(x) x /.102)


waterLevel = Map(function(x, y, z) (x * 1000 / (y * 9.807)) + z, rawLevelsKpa, waterDensity, diffsMeters)

#zeeOffsetDepth = Map(function(x, y, z) list(x[,2] / 3.28084, y[,3] / 3.28084, (z[,4] / 12) / 3.28), rawLevelsZoo, rawLevelsZoo, rawLevelsZoo)

solinstInstrumentElevation <- Map(function(x,y,z) z - (x-y), instrument_depth, z_offset, z_coord)

#<- lapply(rawLevelsWindow, function(x) (x[,2]  / 3.28084) + (x[,3] / 3.28084) - ((x[,4] / 12) / 3.28084))

solinstGwElevation = Map(function(x, y) x + y, solinstInstrumentElevation, waterLevel)
names(solinstGwElevation) <- wellNames

#commit level logger batches to the database
lapply(levelBatch, dpCommitBatchData)

#to reload batches
levelBatch = lapply(deploymentName, dpLoadBatch, set = mySet)


######### make pressure corrections

dbDisconnect(con)
drv <- dbDriver("MySQL")
con <- dbConnect(drv, port = 3306, host = "localhost", user="aquageoecollc", password="h0ly,sh1t", dbname="meachamcreek11012016")

rawLevels = dbGetQuery(
con, 
"SELECT DISTINCT
observation.date_time, 
observation.data_value,
installation.z_coord,
installation.z_offset,
installation.instrument_depth,
instrument.builtin_offset,
site.site_name
FROM
observation,
deployment,
installation,
instrument,
site,
medium,
metrics
WHERE
observation.deployment_id = deployment.deployment_id AND
deployment.installation_id = installation.installation_id AND
deployment.instrument_id = instrument.id AND
installation.site_id = site.id AND
deployment.medium_id = medium.medium_id AND
deployment.medium_id = 1 AND
observation.metric_id = 3;")

rawPressure = dbGetQuery(
  con, 
  "SELECT DISTINCT
observation.date_time, 
  observation.data_value,
  installation.z_coord,
  installation.z_offset,
  installation.instrument_depth,
  instrument.builtin_offset,
  site.site_name
  FROM
  observation,
  deployment,
  installation,
  instrument,
  site,
  medium,
  metrics
  WHERE
  observation.deployment_id = deployment.deployment_id AND
  deployment.installation_id = installation.installation_id AND
  deployment.instrument_id = instrument.id AND
  installation.site_id = site.id AND
  deployment.medium_id = medium.medium_id AND
  deployment.medium_id = 2 AND
  observation.metric_id = 3;")

temperature = dbGetQuery(
  con, 
"SELECT DISTINCT
observation.date_time, 
observation.data_value,
site.site_name
FROM
observation,
deployment,
installation,
instrument,
site,
medium,
metrics
WHERE
observation.deployment_id = deployment.deployment_id AND
deployment.installation_id = installation.installation_id AND
deployment.instrument_id = instrument.id AND
installation.site_id = site.id AND
deployment.medium_id = medium.medium_id AND
deployment.medium_id = 1 AND
observation.metric_id = 1;"
)

dbDisconnect(con)

rawLevelsSplit = split(rawLevels, as.factor(rawLevels$site_name))
rawLevelsZoo = testThis(rawLevelsSplit, 2:6)
#rawLevelsZooMerge = lapply(rawLevelsZoo, function(x) merge(x[,1]))

rawPressureSplit = split(rawPressure, as.factor(rawPressure$site_name))
rawPressureZoo = testThis(rawPressureSplit, 2:6)

temperatureSplit = split(temperature, as.factor(temperature$site_name))
temperatureZoo = testThis(temperatureSplit, 2)

rawLevelsWindow <- lapply(rawLevelsZoo, function(x, s, e) window(x, start = ymd_hms("2013-6-12 00:00:00"), end = ymd_hms("2014-1-12 23:00:00")))
rawPressureWindow <- lapply(rawPressureZoo, function(x, s, e) window(x, start = ymd_hms("2013-6-12 00:00:00"), end = ymd_hms("2014-1-12 23:00:00")))
temperatureWindow <- lapply(temperatureZoo, function(x, s, e) window(x, start = ymd_hms("2013-6-12 00:00:00"), end = ymd_hms("2014-1-12 23:00:00")))

offsetCorrectedRawLevels = lapply(rawLevelsWindow, function(x) x[,1] - x[,5])
pressureCorrectedRawLevels = Map(function(x, y) x - y[,1], offsetCorrectedRawLevels, rawPressureWindow)
pressureCorrectedRawLevelsZoo = lapply(pressureCorrectedRawLevels, function(x) zoo(x, order.by = index(rawLevelsZoo[[1]])))

#SOLINST change in density with temperature 
## p1 = p0 / (1 + b * (t1 - t0)) #http://www.engineeringtoolbox.com/fluid-density-temperature-pressure-d_309.html
waterDensity = lapply(temperatureZoo, function(x) 1000 / (1 + 0.0002 * (x - 4)))
rawLevelsKpa = lapply(pressureCorrectedRawLevelsZoo, function(x) x /.102)
waterLevel = Map(function(x,y) x * 1000 / (y * 9.807), rawLevelsKpa, waterDensity)

zeeOffsetDepth = Map(function(x, y, z) list(x[,2] / 3.28084, y[,3] / 3.28084, (z[,4] / 12) / 3.28), rawLevelsZoo, rawLevelsZoo, rawLevelsZoo)
solinstInstrumentElevation <- lapply(rawLevelsWindow, function(x) (x[,2]  / 3.28084) + (x[,3] / 3.28084) - ((x[,4] / 12) / 3.28084))
solinstGwElevation = Map(function(x, y) x + y, solinstInstrumentElevation, waterLevel)

window(solinstGwElevation$`well 6`, start = ymd_hms("2013-08-25 23:00:00 UTC"), end = ymd_hms("2013-08-26 23:00:00 UTC"))

constant = sample(1:1000, 1000)
constant

myTweakedBatch = dpTweak(myBatches[[1]], dpTweakAddConstant, args = list(columnName = "water_elevation", constant = constant), "Cable connecting logger to top of well was mismeasured by 0.5m")
dpTweak()
head(myTweakedBatch$batchData)
head(myBatches[[1]]$batchData)

myTweakedBatch$tweaks