
Sys.setenv(TZ='UTC')
library("lubridate")
library("xts")
library("zoo")

### Take data read in from .csv from a Solinst level logger and conert it to a
### data frame with new column names.  New Column names should be named to match
### the metrics table in the database.  This function crunches the data through
### a time series routine to get the date-time to generate hourly data.

SolinstDataFramesList = function(csvFiles, suffix, dayOrHour, columnRange, headerIndex){
  dataFramesList = lapply(paste0(suffix, "/", csvFiles), read.csv, header = F, stringsAsFactors = F, skip = headerIndex)
  # mn = metricName
  dataFramesList = lapply(
    dataFramesList, 
    function(df, mn) 
      data.frame(
        date_time = paste(df[[1]], df[[2]]), 
        water_depth = df[[4]], 
        temperature = df[[5]],
        stringsAsFactors = F
      )
  )
  
  dataFramesTsList = tsBuilder(dataFramesList, dayOrHour, columnRange)
  
  dataFramesList = lapply(dataFramesTsList,
                          function(ts) {
                            theData = coredata(ts)
                            data.frame(date_time = index(ts),
                                       water_elevation = theData[,1],
                                       temperature = theData[,2],
                                       stringsAsFactors = F)
                          }
  )
  return(dataFramesList)
}

SolinstAPDataFramesList = function(csvFiles, suffix, dayOrHour, columnRange, headerIndex){
  dataFramesList = lapply(paste0(suffix, "/", csvFiles), read.csv, header = F, stringsAsFactors = F, skip = headerIndex)
  # mn = metricName
  dataFramesList = lapply(
    dataFramesList, 
    function(df, mn) 
      data.frame(
        date_time = paste(df[[1]], df[[2]]), 
        pressure = df[[4]],
        stringsAsFactors = F
      )
  )
  
  dataFramesTsList = tsBuilder(dataFramesList, dayOrHour, columnRange)
  
  dataFramesList = lapply(dataFramesTsList,
                          function(ts) {
                            theData = coredata(ts)
                            data.frame(date_time = index(ts),
                                       pressure = theData[,1],
                                       stringsAsFactors = F)
                          }
  )
  return(dataFramesList)
}
#########
#
#
#
#########
# "/Users/aquageoecollc/Documents/R/meacham_creek/meachamDatabase/data"
# e.g. suffix = "data"
solinstDataCampaignExtract = function(dataDir, suffix){
csvFiles = list.files(dataDir, recursive = T, pattern = ".csv")
rawData = lapply(paste0(suffix, "/", csvFiles), read.csv, header = F, stringsAsFactors = F)
loggerSerialNumbers = sapply(rawData, '[', 2,1)
dirsNstuff = list.dirs(suffix)
dirSplit = sapply(dirsNstuff, str_split, pattern = "/")
#names(dirSplit) = c("one","two","three","four")
#names(dirSplit) <- letters[1:length(dirSplit[[1]])]
campaigns = dirSplit[[1]][length(dirSplit[[1]])]
return(c(csvFiles = list(csvFiles), rawData = list(rawData), loggerSerialNumber = list(loggerSerialNumbers),  campaigns = list(campaigns)))
}


##################################################################
# this function takes a data frame
# and transforms the data and the index into a zoo time series
# rawTimeSeries is the data frame
# dayOrHour = "day" or "hour"
#
#################################################################

tsBuilder = function (rawTimeSeries, dayOrHour, columnRange) {
  #rawTimeSeries = split(rawTimeSeries, rawTimeSeries$site_name)
  tsList =
    lapply(
      rawTimeSeries,
      function(rawTimeSeries){
        
        ## data values in the time series
        dataValue = rawTimeSeries[,columnRange] #rawTimeSeries[,column]#
        
        ## time index in the time series
        ## note that this is class text from SQL
        timeIndex = ymd_hms(rawTimeSeries$date_time)
        
        ## create a zoo time series
        ts.zoo = zoo(dataValue, order.by = timeIndex)
        
        ## convert the text index to a POSIX index
        #foo <- ymd_hms(index(ts.zoo), tz="UTC")[1:(length(index(ts.zoo)))]
        
        ## replace old text index with new POSIX index
        #index(ts.zoo) <- foo
        
        ## conver to xts ts so that we can use period.apply
        ts.xts <- as.xts(ts.zoo)
        ts.xts.period <- period.apply(ts.xts,
                                      INDEX = endpoints(index(ts.xts), paste(dayOrHour, "s", sep = "")),
                                      FUN = mean)
        
        ## convert back to zoo ts so that we can modulo to whole time (e.g. top o' the day or hour)
        ts.zoo.period <- as.zoo(ts.xts.period)
        
        ## modulo the index
        foo.bar <- floor_date(index(ts.zoo.period), dayOrHour)
        
        ## update the index
        index(ts.zoo.period) <- foo.bar
        
        ## name the columns with site names
        #colnames(ts.zoo.period) = rawTimeSeries$site_name[1]
        
        return(ts.zoo.period)
      }
    )
  
  #timeSeries = Reduce(merge, tsList)
  
  #return(timeSeries)
}

meters2kPa <- function(x, columnName) {
  x$batchData[,columnName] =  x$batchData[,columnName] / 0.102
  return(x)
}
