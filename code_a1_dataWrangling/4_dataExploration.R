############## Packages needed ####################

library(tidyverse)
library(Gmisc)
library(fasttime)
library(doParallel)
library(foreach)
library(lubridate)
library(corrplot)
library(ggcorrplot)
library(pracma)
library(ggpubr)

# ---

############# Import of Data ##############

transactions_clean <- readRDS("transactions_clean.rds")
transactions_unclean <- readRDS("transactions_unclean.rds")

dailyDemandTs <- getTsDemand(transactions_clean,
                                    granularity = "day",
                                    topPercentSkus)

saveRDS(dailyDemandTs_top10, "dailyDemandTs_top10")

#---

############### Timeseries of Demand##################

# Input:
# 1) transactions: The functions 'dataTransformation_main' have to have been applied on the transactions already.
# It is possible to use uncleaned data (meaning not having used 'dataCleasning_main) as input for this function.
# 2) sku_code_prefix: All sku_codes_prefixes values for which the demand time series gets calculated. By default all
# sku_code_prefixes, that are part of the provided transactions_table
# 3) granularity: Aggregation of the demand time series
# 4) topPercentSkus: All sku_codes_prefixes get ordered by how often they appear in the data. 'topPercentSkus' specifies 
# the quantile of all sku_codes_prefixes for which the demand time series gets calculated. 'topPercent = 0.6' for example
# means that we only calculate demand series for the top 40% of the most common sku_codes_prefixes. 
# IMPORTANT NOTE: 'topPercentSkus' overwrites the argument 'sku_codes_prefix'.

getTsDemand <- function(transactions,
                        sku_codes_prefix = unique(transactions$sku_code_prefix), 
                        granularity = c("day",
                                        "week"),
                        topPercentSkus = 0,
                        dateStart = NULL,
                        dateEnd = NULL) {
  
  granularity <- match.arg(granularity)
  
  sku_codes <- getTopSkus(transactions, 
                          rankingByPrefix = TRUE, 
                          topPercentSkus = topPercentSkus)
  
  # Just needed if the transactions haven't been cleaned already
  sku_codes <- setdiff(sku_codes, NA)
  
  transactions <- transactions %>%
    filter(sku_code %in% sku_codes) %>%
    filter(Ordered_date_date >= dateStart) %>%
    filter(Ordered_date_date <= dateEnd)
  
  transactions <- setorderv(as.data.table(transactions), cols = "Ordered_date")
  
  timeFeature <- switch(granularity, 
                        "day" = "Ordered_date_date",
                        "week" = "Ordered_date_yearweek")
  
    dateFirst <- transactions[[timeFeature]][1]
    dateLast <- transactions[[timeFeature]][nrow(transactions)]
    
  timeUnits <- seq(dateFirst, dateLast, 1)
  
  transactions_agg <- transactions %>%
    group_by_at(vars(sku_code_prefix, timeFeature)) %>%
    summarise(demand = sum(quantity * quantity_perPackage)) %>%
    as.data.frame()
  
  transactions_groups <- transactions_agg %>%
    group_by(sku_code_prefix) %>%
    group_split()
  
  transactions_keys <- transactions_agg %>%
    group_by(sku_code_prefix) %>%
    group_keys()
  
  if(granularity == "day") {
    # We create some additional time features that we add later on
    Ordered_date_year <- year(timeUnits)
    
    Ordered_date_yearweek <- (Ordered_date_year == 2018)*0 + 
      (Ordered_date_year == 2019)*52 + (Ordered_date_year == 2020)*104 + isoweek(timeUnits)
    
    # The date '2019-12-30' for example has the isoweek 1 and would therefor receive the Ordered_date_yearweek 
    # value of 53 despite the fact that '2019-12-30' actually belongs to week 105.
    # The following 2 lines of code keep track of that.
    endOfYearCorrection_index <- month(timeUnits) == 12 & isoweek(timeUnits) == 1
    Ordered_date_yearweek[endOfYearCorrection_index] <- Ordered_date_yearweek[endOfYearCorrection_index] + 52
    
    timeUnitsSinceStart_toAdd <- data.frame(timeUnits, 
                                            "timeUnitsSinceStart" = seq_along(timeUnits),
                                            "Ordered_date_yearweek" = Ordered_date_yearweek)
    colnames(timeUnitsSinceStart_toAdd)[1] <- timeFeature
    
  } else {
    timeUnitsSinceStart_toAdd <- data.frame(timeUnits, 
                                            "timeUnitsSinceStart" = seq_along(timeUnits))
    colnames(timeUnitsSinceStart_toAdd)[1] <- timeFeature
  }
  
  timeseriesPerSku <- lapply(transactions_groups, function(group) {

    demandData <- group[, c(timeFeature, "demand")]
    
    if(!all(timeUnits %in% demandData[[timeFeature]])) {
      
      timeUnitsAvailable <- group[[timeFeature]]
      timeUnitsMissing <- timeUnits[!timeUnits %in% timeUnitsAvailable]
      
      demandMissing <- data.frame(timeUnitsMissing, 0)
      colnames(demandMissing) <- c(timeFeature, "demand")
      
      demandData <- rbind(demandData, demandMissing)
      demandData <- setorderv(demandData, cols = timeFeature)
    }
    demandData <- as.data.frame(left_join(demandData, timeUnitsSinceStart_toAdd, by = timeFeature))
    
    # We return a list that contains each sku-demand time series. In order to be able to use rbind
    # on that list easily, we also add a column tracking the name of the sku.
    sku_code_prefix_toAdd <- data.frame("sku_code_prefix" = unique(group$sku_code_prefix))
    demandData <- cbind(demandData, sku_code_prefix_toAdd)
  })
  names(timeseriesPerSku) <- transactions_keys$sku_code_prefix
  
  return(timeseriesPerSku)
}

transactions_clean <- readRDS("transactions_clean.rds")

weeklyDemandTs <- getTsDemand(transactions_clean,
                              granularity = "week",
                              topPercentSkus = 1,
                              dateStart = "2019-03-11",
                              dateEnd = "2020-05-03")

saveRDS(weeklyDemandTs, "weeklyDemandTs.rds")

#---

################### Ts of Demand per Wholesaler ###############

getTsDemandPerWholesaler <- function(transactions,
                                     sku_codes_prefix = unique(transactions$sku_code_prefix), 
                                     granularity = c("day",
                                                     "week",
                                                     "month"),
                                     topPercentSkus = 1) {
  
  granularity <- match.arg(granularity)
  
  # 'topPercentSkus == 1' implies that we simply use all sku_codes_prefix
  if(topPercentSkus != 1) {
    
    countPerSku <- transactions %>%
      group_by(sku_code_prefix) %>%
      summarise(count = n())
    
    sku_codes_prefix <- countPerSku %>%
      filter(count >= quantile(countPerSku$count, 1 - topPercentSkus)) %>%
      select(sku_code_prefix) %>%
      fastDoCall("c", .)
  }
  
  # Just needed if the transactions haven't been cleaned already
  sku_codes_prefix <- setdiff(sku_codes_prefix, NA)
  
  transactions_sku <- transactions %>%
    filter(sku_code_prefix %in% as.character(sku_codes_prefix))
  
  timeFeature <- switch(granularity, 
                        "day" = "Ordered_date_date",
                        "week" = "Ordered_date_week",
                        "month" = "Ordered_date_month")
  transactionsOrdered <- transactions[order(transactions$Ordered_date), ]
  timeUnits <- transactionsOrdered[[timeFeature]][1]
  
  for(i in 2:nrow(transactions)) {
    if(transactionsOrdered[[timeFeature]][i] != timeUnits[length(timeUnits)]) {
      timeUnits[length(timeUnits) + 1] <- transactionsOrdered[[timeFeature]][i]
    }
  }
  
  distributor_ids <- sort(unique(transactions$distributor_id))
  
  timeDistributorCombs_list <- lapply(1:length(timeUnits), function(i) {
    data.frame(rep.int(timeUnits[i], times = length(distributor_ids)), i, distributor_ids)
  })
  
  timeDistributorCombs <- fastDoCall("rbind", timeDistributorCombs_list)
  colnames(timeDistributorCombs) <- c(timeFeature, "timeSinceBeginning", "distributor_id")
  
  
  aggDemand <- transactions_sku %>%
    group_by_at(vars(sku_code_prefix, timeFeature, distributor_id)) %>%
    summarise(sum(quantity * quantity_perPackage)) %>%
    as.data.frame()
  
  aggDemand <- add_column(aggDemand, 0, .after = 2)
  colnames(aggDemand)[3] <- "timeSinceBeginning"

  for(i in 1:length(timeUnits)) {
    indexToAdd <- aggDemand[[timeFeature]] == timeUnits[i]
    aggDemand[indexToAdd, "timeSinceBeginning"] <- i
  }
  
  colnames(aggDemand)[2] <- paste(timeFeature)
  colnames(aggDemand)[5] <- switch(granularity, 
                                   "day" = "daily_demand",
                                   "week" = "weekly_demand",
                                   "month" = "monthly_demand")
  registerDoParallel()
  
  timeseriesPerSku <- lapply(sku_codes_prefix, function(sku_code_prefix) {
  # timeseriesPerSku <- foreach(sku_code_prefix = unique(sku_codes_prefix), .packages = "Gmisc") %dopar% {
    aggDemandSku <- aggDemand[aggDemand$sku_code_prefix == sku_code_prefix, ]

    browser()
    aggDemand_combs <- aggDemandSku[, c(2, 4)]

    availableCombs <- tail(x = duplicated(rbind(aggDemand_combs, timeDistributorCombs[, c(1, 3)])), 
                           n = nrow(timeDistributorCombs))
    
    missingCombs <- timeDistributorCombs[!availableCombs, ]
    
    rowsToAdd <- data.frame(sku_code_prefix, missingCombs, 0)
    colnames(rowsToAdd)[5] <- colnames(aggDemand)[5]
    
    aggDemandSku <- rbind(aggDemandSku, rowsToAdd)
    
    aggDemandSku <- aggDemandSku[order(aggDemandSku[["timeSinceBeginning"]], aggDemandSku[["distributor_id"]]), ]
    
    rownames(aggDemandSku) <- 1:nrow(aggDemandSku)
    browser()
    return(aggDemandSku)
  })
  names(timeseriesPerSku) <- unique(sku_codes_prefix)
  browser()
  return(timeseriesPerSku)
}

daylyDemandTs <- getTsDemandPerWholesaler(transactions = transactions,
                                           granularity = "day",
                                           topPercentSkus = 0.15)

saveRDS(daylyDemandTs, "daylyDemandTs.rds")

dailyDemandTs <- getTsDemandPerWholesaler(transactions,
                                          granularity = "day",
                                          topPercentSkus = 0.15)

#---

################### Corr of Demand ###################

getCorrelationsDemand <- function(transactions_ts,
                                  sku_codes_prefix = names(transactions_ts)) {
  
  transactions_ts <- transactions_ts[sku_codes_prefix]
  
  transactions_ts_noTrend <- lapply(transactions_ts, function(ts) {
    ts_detrended <- detrend(ts)
    return(ts_detrended)
    
    # ts_decomposed <- decompose(ts(ts, frequency = 7), type = "multiplicative")
    # ts_random <- ts_decomposed$random[4:(length(ts) - 3)]
    return(ts_random)
  })
  
  demandMatrix <- fastDoCall("cbind", transactions_ts_noTrend)
  colnames(demandMatrix) <- names(transactions_ts)
  corrMatrix <- cor(demandMatrix)
  
  n <- nrow(corrMatrix)
  colnames <- colnames(corrMatrix)
  rownames <- rownames(corrMatrix)
  
  registerDoParallel()
  corrList <- foreach(i = 1:(nrow(corrMatrix)-1), .packages = "Gmisc") %dopar% {
    corrList <- lapply((i+1):n, function(j) {
      
      corrData <- data.frame("sku_code1" = colnames[i], "sku_code2" = rownames[j], "corr" = corrMatrix[i, j])
      return(corrData)
    })
    corrTable <- fastDoCall("rbind", corrList)
    return(corrTable)
  }
  corrTable <- fastDoCall("rbind", corrList)
  
  corrTable <- corrTable[order(corrTable$corr, decreasing = TRUE), ]
  return(corrTable)
}


# ---

################# Corr and Ts #################

# All arguments have exactly the same purpose and work the same way the did for the function 
# 'getTsDemand'. 
# This function calculates both the aggregated demand time series and the resulting correlations
# of the time series. 

getCorrelationsAndTimeseries <- function(transactions,
                                         sku_codes_prefix = unique(transactions$sku_code_prefix),
                                         granularity = c("day",
                                                         "week",
                                                         "month"),
                                         topPercentSkus = 0) {
  
  if(topPercentSkus != 0) {
    
    countPerSku <- transactions %>%
      group_by(sku_code_prefix) %>%
      summarise(count = n())
    
    sku_codes_prefix <- countPerSku %>%
      filter(count >= quantile(countPerSku$count, topPercentSkus)) %>%
      select(sku_code_prefix) %>%
      fastDoCall("c", .)
  }
  
  granularity <- match.arg(granularity)
  
  transactions_ts <- getTsDemand(transactions = transactions,
                                 sku_codes_prefix = sku_codes_prefix,
                                 granularity = granularity,
                                 topPercentSkus = topPercentSkus)
  
  corrTable <- getCorrelationsDemand(transactions_ts = transactions_ts,
                                     sku_codes_prefix = sku_codes_prefix)
  
  return(list("correlations" = corrTable,
              "transactions_ts" = transactions_ts))
}

#---

############### Plot Ts and Corr ####################

# This function plots the timeseries of the aggregated demand for the given sku_code_prefixes using the
# provided granularity. 
# It is possible to provide only a single sku_code_prefix. If 2 are provided the pearson correlation for the
# 2 time series is also calculated and shown in the subtitle of the plot.
plotTimeseriesDemand <- function(transactions,
                                 sku_code_prefix1,
                                 sku_code_prefix2 = NULL,
                                 granularity = c("day",
                                                 "week",
                                                 "month")) {
  
  # Calculate demand time series and correlations
  if(!is.null(sku_code_prefix2)) {
    corrResults <- getCorrelationsAndTimeseries(transactions = transactions,
                                                sku_codes_prefix = c(sku_code_prefix1, sku_code_prefix2),
                                                granularity = granularity)
    
    transactions_ts <- corrResults$transactions_ts
    correlations <- corrResults$correlations
    
  } else {
    transactions_ts <- getTsDemand(transactions = transactions,
                                   sku_codes_prefix = c(sku_code_prefix1, sku_code_prefix2),
                                   granularity = granularity)
  }
  
  
  # Data of sku_code_prefix1
  ts_1 <- transactions_ts[[as.character(sku_code_prefix1)]]
  
  plotData_1 <- data.frame("time" = seq_along(ts_1), 
                           "demand" = ts_1, 
                           label = as.character(sku_code_prefix1))
  
  name_1 <- transactions[transactions$sku_code_prefix_prefix == sku_code_prefix1, ]$name
  
  # Data of sku_code_prefix2 (if provided)
  # If a second sku_code_prefix is provided we have to create a different plot that also contains the 
  # correlation results and different labels.
  if(!is.null(sku_code_prefix2)) {
    ts_2 <- corrResults$transactions_ts[[as.character(sku_code_prefix2)]]
    
    plotData_2 <- data.frame("time" = seq_along(ts_2), 
                             "demand" = ts_2, 
                             label = as.character(sku_code_prefix2))
    
    plotData <- rbind(plotData_1, plotData_2)
    
    name_2 <- transactions[transactions$sku_code_prefix_prefix == sku_code_prefix2, ]$name
    title <- paste(paste0(name_1), "vs.", paste0(name_2))
    
    correlations <- corrResults[["correlations"]]
    
    corr <- correlations[correlations$sku_code_prefix1 == sku_code_prefix1 & 
                           correlations$sku_code_prefix2 == sku_code_prefix2, ]$corr
    corr <- round(corr, 4)
    subtitle <- paste("Correlation:", corr)
    
  } else {
    plotData <- plotData_1
    name_1 <- transactions[transactions$sku_code_prefix == sku_code_prefix1, ]$name
    title <- paste0(name_1)
    subtitle <- ""
  }
  
  g <- ggplot(data = plotData, aes(x = time, y = demand, col = label)) +
    geom_point() + 
    geom_line() + 
    labs(x = paste0(granularity, "s since beginning of the data"), 
         y = "Demand", 
         title = title,
         subtitle = subtitle) +
    theme(axis.text.x=element_text(angle=0), axis.text.y = element_text(hjust = 1.2)) +
    theme_bw()
  g
}

############ Comparison clean vs. unclean data #################

# This function compares the demand time series of the clean and unclean data for one single sku_code.

plotCompareDeletedSkus <- function(transactions_clean,
                                   transactions_unclean,
                                   sku_code_prefix,
                                   granularity = c("day",
                                                   "week",
                                                   "month"),
                                   onePlot = TRUE) {
  
  ts_clean <- getTsDemand(transactions_clean,
                          sku_codes_prefix = sku_code_prefix,
                          granularity = granularity)[[1]]
  
  plotData_1 <- data.frame("time" = seq_along(ts_clean), 
                           "demand" = ts_clean, 
                           label = "clean")
  
  ts_unclean <- getTsDemand(transactions_unclean,
                            sku_codes_prefix = sku_code_prefix,
                            granularity = granularity)[[1]]
  
  plotData_2 <- data.frame("time" = seq_along(ts_unclean), 
                           "demand" = ts_unclean, 
                           label = "unclean")
  
  plotData <- rbind(plotData_1, plotData_2)
  
  # Necessary arguments for plotting
  x_axis <- paste0(granularity, "s since beginning of data")
  y_axis <- "Demand in millilitres / grams / tablets"
  maxDemand <- max(c(ts_clean, ts_unclean))
  
  transactions_sku <- transactions_clean[transactions_clean$sku_code_prefix == sku_code_prefix, ]
  
  name <- transactions_sku$name[1]
  title <- paste0("Demand of ", paste0(name), " (", paste(sku_code_prefix), ")")
  
  # Point of maximal difference
  pointMax <- which.max(ts_unclean - ts_clean)
  
  
  if(onePlot) {
    p <- ggplot(plotData, aes(x = time, y = demand, col = label), colour = "Set1") +
      geom_line() +
      geom_point(data = plotData, aes(x = pointMax, y = ts_clean[pointMax]), colour = "red", size = 10, shape = 1) +
      geom_point(data = plotData, aes(x = pointMax, y = ts_unclean[pointMax]), colour = "red", size = 10, shape = 1) +
      labs(x = x_axis,
           y = y_axis,
           title = title) +
      theme_bw() +
      theme(axis.text.x=element_text(angle=0), axis.text.y = element_text(hjust = 1.2)) +
      ylim(0, maxDemand)
    
    if(granularity != "day") {
      p <- p + geom_point()
    }
    p
    
  } else {
    
    p1 <- ggplot(plotData_1, aes(x = time, y = demand)) + 
      geom_line(color = "steelblue") +
      geom_point(data = plotData, aes(x = pointMax, y = ts_clean[pointMax]), 
                 colour = "red", size = 8, shape = 1) +
      labs(x = x_axis, 
           y = y_axis, 
           title = title) + 
      theme_bw() +
      theme(axis.text.x=element_text(angle=0), axis.text.y = element_text(hjust = 1.2)) + 
      ylim(0, maxDemand)
    
    p2 <- ggplot(plotData_2, aes(x = time, y = demand)) + 
      geom_line(color = "steelblue") +
      geom_point(data = plotData, aes(x = pointMax, y = ts_unclean[pointMax]), 
                 colour = "red", size = 8, shape = 1) +
      labs(x = x_axis, 
           y = y_axis, 
           title = title) + 
      #theme_bw(axis.title.y=element_blank()) +
      theme_bw() +
      theme(axis.text.x=element_text(angle=0), axis.text.y = element_text(hjust = 1.2)) + 
      ylim(0, maxDemand)
    
    if(granularity != "day") {
      p1 <- p1 + geom_point()
      p2 <- p2 + geom_point()
    }
    
    ggarrange(p1, p2 + rremove("y.title"),
              ncol = 2, nrow = 1)
  }
}



