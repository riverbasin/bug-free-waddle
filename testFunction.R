
  testThis = function(x, cols){
    dateTime <- lapply(x, function(x) ymd_hms(x$date_time))
    obs <- lapply(x, function(x) as.matrix(x[cols]))
    rawLevelsZoo <- Map(function(x, y) zoo(x, order.by = y), obs, dateTime)
      #lapply(x, function(x) zoo(obs, order.by = dateTime))
    return(rawLevelsZoo)
  }