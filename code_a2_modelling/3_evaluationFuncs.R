

######## Over/Under Counts ########

# The argument 'errors' can either be a vector or a matrix. If a matrix is provided the errors of each model
# have to be 
getOverUnderPercent <- function(errors) {
  
  if(is.null(nrow(errors))) {
    errors <- errors[!is.na(errors)]
    overPercent <- sum(errors < - .Machine$double.eps) / length(errors)
    perfectPercent <- sum(errors >= - .Machine$double.eps & errors <= .Machine$double.eps) / length(errors)
    underPercent <- sum(errors > .Machine$double.eps) / length(errors)
    
    return(data.frame(overPercent, perfectPercent, underPercent))
    
  } else {
    
    overUnderPercent <- apply(errors, MARGIN = 2, function(errors_col) {
      errors_col <- errors_col[!is.na(errors_col)]
      overPercent <- sum(errors_col < - .Machine$double.eps) / length(errors_col)
      perfectPercent <- sum(errors_col >= - .Machine$double.eps & errors_col <= .Machine$double.eps) / length(errors_col)
      underPercent <- sum(errors_col > .Machine$double.eps) / length(errors_col)
      return(c(overPercent, perfectPercent, underPercent))
    })
    rownames(overUnderPercent) <- c("overPercent", "perfectPercent", "underPercent")
    return(overUnderPercent)
  }
}

#---

######## Plot Preds ########

plotPreds_main <- function(dataAll_noAgg,
                           sku_code_prefix,
                           aggLevel = "sku_code_prefix",
                           includeTraining = TRUE,
                           orderByTime = TRUE,
                           timeFeatureForOrder = "Ordered_date_yearweek",
                           printName = FALSE) {
  
  skuCodePrefix_provided <- as.character(sku_code_prefix)
  
  plotData <- dataAll_noAgg %>%
    filter(sku_code_prefix %in% skuCodePrefix_provided) %>%
    group_by(.dots = aggLevel) %>%
    group_by(Ordered_date_yearweek, label, add = TRUE) %>%
    summarise(actuals = sum(actuals),
              preds = sum(preds),
              hasPromotion = any(hasPromotion),
              priceIndex = mean(priceRatio_median_allTime_SkuPrefix)) %>%
    group_by() %>%
    mutate(priceIndex = priceIndex * max(actuals) * 1.2) %>%
    as.data.frame()
  
  for(skuPrefix in sku_code_prefix) {
    plotDataSku <- plotData %>%
      filter(sku_code_prefix == skuPrefix)
    
    plotPreds(dataPred = plotDataSku,
              sku_code_prefix = skuPrefix,
              includeTraining = includeTraining,
              orderByTime = orderByTime,
              timeFeatureForOrder = timeFeatureForOrder,
              printName = printName,
              prices = plotDataSku$priceIndex)
  }
}


plotPreds <- function(dataPred,
                      sku_code_prefix = NULL,
                      includeTraining = TRUE,
                      orderByTime = TRUE,
                      timeFeatureForOrder = "Ordered_date_yearweek",
                      printName = FALSE,
                      prices = NULL,
                      dataPredCI = NULL) {
  
  if(!is.null(sku_code_prefix)) {
    
    if(!sku_code_prefix %in% dataPred$sku_code_prefix) {
      stop("Specified 'sku_code_prefix' is not part of 'dataPred'.")
    }
    
    sku_code_prefix_provided <- as.character(sku_code_prefix)
    dataPred <- dataPred %>%
      filter(sku_code_prefix == sku_code_prefix_provided)
    
    titleToAdd <- paste("Sku", sku_code_prefix)
  } else {
    titleToAdd <- ""
  }
  
  if(!is.null(dataPredCI)) {
    dataPredCI <- dataPredCI %>%
      filter(sku_code_prefix == sku_code_prefix_provided)
  }

  # Name Data: Remember: One product can have several different names, only the name for each sku_code is identical
  productNames <- readRDS("SavedData/productNames.rds")
  names <- productNames$name[productNames$sku_code_prefix == sku_code_prefix]
  
  name <- names[which.min(nchar(names))]
  
  titleToAdd <- paste0(titleToAdd, ", ", name)
  
  if(orderByTime) {
    if("sku_code" %in% colnames(dataPred)) {
      dataPred <- setorderv(dataPred, cols = c(timeFeatureForOrder, "sku_code"))
    } else {
      dataPred <- setorderv(dataPred, cols = c(timeFeatureForOrder, "sku_code_prefix"))
    }
  }
  
  preds <- dataPred$preds
  actuals <- dataPred$actuals
  
  range_model <- round(max(preds) - min(preds), 1)
  range_real  <- round(max(actuals) - min(actuals), 1)
  
  preds_test <- dataPred %>%
    filter(label == "test") %>%
    select(preds) %>%
    fastDoCall("c", .)
  
  actuals_test <- dataPred %>%
    filter(label == "test") %>%
    select(actuals) %>%
    fastDoCall("c", .)
  
  mae_scaled <- round(mae_scaled(actuals_test, preds_test), digits = 2)
  
  # Revenue Data
  revenueData <- readRDS("SavedData/revenueData.rds")
  revenue <- revenueData %>%
    filter(sku_code_prefix == sku_code_prefix_provided) %>%
    select(payVolumePercent) %>%
    fastDoCall("c", .) %>%
    unname(TRUE) %>%
    round(digits = 5)
  
  revenue <- revenue * 100
  
  # Stockout-Data
  stockOutData <- readRDS("SavedData/stockOutData.rds")
  stockOuts <- stockOutData %>%
    filter(sku_code_prefix == sku_code_prefix_provided) %>%
    select(stockOuts_prefix) %>%
    fastDoCall("c", .) %>%
    unname(TRUE)
  
  plotDataActuals <- data.frame("number" = seq_along(actuals), 
                                "demand" = actuals,
                                "category" = "actuals",
                                "lower" = actuals,
                                "upper" = actuals)
  
  plotDataPreds <- data.frame("number" = seq_along(preds), 
                              "demand" = preds,
                              "category" = "preds",
                              "lower" = preds,
                              "upper" = preds)
  
  if(!is.null(prices)) {
    plotDataPrices <- data.frame("number" = seq_along(prices), 
                                 "demand" = prices,
                                 "category" = "prices",
                                 "lower" = prices,
                                 "upper" = prices)
    
    plotData <- rbind(plotDataActuals, plotDataPreds, plotDataPrices)
    
  } else {
    plotData <- rbind(plotDataActuals, plotDataPreds)
  }
  
  if(includeTraining) {
    label_toAdd <- data.frame("label_train" = dataPred$label)
    plotData <- cbind(plotData, label_toAdd)
  }
  
  if("hasPromotion" %in% colnames(dataPred)) {
    promotion_toAdd <- data.frame("hasPromotion" = dataPred$hasPromotion)
    plotData <- cbind(plotData, promotion_toAdd)
  }
  
  if(includeTraining) {
    g <- ggplot(data = plotData, aes(x = number, 
                                     y = demand, 
                                     col = category, 
                                     group = number, 
                                     shape = label_train))
  } else {
    g <- ggplot(data = plotData, aes(x = number, 
                                     y = demand, 
                                     col = category, 
                                     group = number))
  }
  
  g <- g + geom_point(size = 2) + 
    geom_line(data = plotData, aes(x = number, y = demand, group = category)) + 
    labs(x = "Week", 
         y = "Demand in tablets / grams / milliliters", 
         title = paste(titleToAdd),
         subtitle = paste0("MAE-Scaled: ", mae_scaled, 
                           ",  Percentage of revenue: ", revenue, "%",
                           ", Total number of stockouts:", stockOuts)) + 
         # subtitle = paste("range actual values:", range_real, "-", "range pred values:", range_model)) + 
    theme_bw()

  if("hasPromotion" %in% colnames(dataPred)) {
    plotDataActuals_prom <- plotData %>%
      filter(category == "actuals") %>%
      filter(hasPromotion) %>%
      select(number, demand)
    
    g <- g + geom_point(data = plotDataActuals_prom, aes(x = number,
                                                  y = demand), colour = "black", size = 3, shape = 1)
    
    plotDataPreds_prom <- plotData %>%
      filter(category == "preds") %>%
      filter(hasPromotion)
      
    g <- g + geom_point(data = plotDataPreds_prom, aes(x = number,
                                                  y = demand), colour = "black", size = 3, shape = 1)
  }
  
  if(!is.null(dataPredCI)) {
    
    plotData[plotData$category == "preds" & plotData$label_train == "test", ]$lower <- dataPredCI$lower
    plotData[plotData$category == "preds" & plotData$label_train == "test", ]$upper <- dataPredCI$upper
    
    h <- ggplot(data = plotData[plotData$category == "preds", ], aes(x = number, y = demand)) + 
      geom_point(color = "steelblue", size = 0.1) + 
      geom_ribbon(aes(ymin = lower, ymax = upper), alpha = 0.2, color = "grey", width = 0.8) + 
      geom_point(data = plotData, aes(x = number, y = demand, 
                                      shape = label_train, col = category, group = number), size = 2) + 
      geom_line(data = plotData, aes(x = number, y = demand, col = category, group = category)) + 
      labs(x = "Week", 
           y = "Demand in tablets / grams / milliliters", 
           title = paste(titleToAdd),
           subtitle = paste0("MAE-Scaled: ", mae_scaled, 
                             ",  Percentage of revenue: ", revenue, "%")) + 
      theme_bw()
    
    # # Get promotions
    # plotDataActuals_prom <- plotData %>%
    #   filter(category == "actuals") %>%
    #   filter(hasPromotion) %>%
    #   select(number, demand)
    # 
    # h <- h + geom_point(data = plotDataActuals_prom, aes(x = number,
    #                                                      y = demand), colour = "black", size = 3, shape = 1)
    # 
    # plotDataPreds_prom <- plotData %>%
    #   filter(category == "preds") %>%
    #   filter(hasPromotion)
    # 
    # h <- h + geom_point(data = plotDataPreds_prom, aes(x = number,
    #                                                    y = demand), colour = "black", size = 3, shape = 1)
    
    g <- h
  }
  
  print(g)
}

#---

######## Boxplot Errors #########

boxplotErrors <- function(dataPred,
                          abs = FALSE,
                          measureToSelect = "rmse",
                          worst = TRUE) {
  
  measureToSelect <- match.arg(measureToSelect, several.ok = FALSE)
  
  accuracy <- getAccuracyPerSku(dataPred,
                                measureToSelect)
  
  accuracy_sort <- accuracy[order(accuracy[[measureToSelect]]), ]
  if(worst) {
    skus_selected <- tail(accuracy_sort, 12)$sku_code_prefix
  } else {
    skus_selected <- head(accuracy_sort, 6)$sku_code_prefix
    skus_selected <- c(skus_selected, head(accuracy_sort, 6)$sku_code_prefix)
  }
  
  dataPred_plot <- dataPred %>%
    filter(sku_code_prefix %in% skus_selected)
  
  ggplot(dataPred_plot, aes(x = sku_code_prefix, y = errors)) +
    geom_boxplot(fill = "steelblue") + 
    #coord_cartesian(ylim=c(minRange, maxRange)) +
    labs(x = "Model", 
         y = "Error", 
         title = "Distribution of errors among all fitted models") + 
    theme_bw()
  
  # groups <- dataPred %>%
  #   group_by(sku_code_prefix) %>%
  #   group_split()
  # 
  # keys <- dataPred %>%
  #   group_by(sku_code_prefix) %>%
  #   group_keys()
  # 
  # errors_list <- lapply(groups, function(group) {
  #   if(abs) abs(group$errors) else group$errors
  # })
  # names(errors_list) <- keys$sku_code_prefix
  # browser()
  # ggplot(stack(errors_list), aes(x = ind, y = values)) +
  #   geom_boxplot(fill = "steelblue") + 
  #   coord_cartesian(ylim=c(minRange, maxRange)) +
  #   labs(x = "Model", 
  #        y = "Error", 
  #        title = "Distribution of errors among all fitted models") + 
  #   theme_bw()
}

#---

##### Accuracy Measurement #####

getAccuracyPerSku <- function(dataPred,
                              errorMeasures = c("rmse",
                                                "rmse_scaled",
                                                "mae",
                                                "mae_scaled",
                                                "smape", 
                                                "mase")) {
  
  sku_codes_prefix <- sort(unique(dataPred$sku_code_prefix))
  
  groups <- dataPred %>%
    group_by(sku_code_prefix) %>%
    group_split()
  
  accuracyPerSku <- lapply(groups, function(group) {
    
    actuals <- group$actuals
    preds <- group$preds
    
    args <- list("actuals" = actuals,
                 "preds" = preds)
    
    errorMeasurement_list <- lapply(errorMeasures, function(measure) {
      do.call(what = measure, args = args, envir = parent.env(environment()))
    })
    errorMeasurement <- fastDoCall("cbind", errorMeasurement_list)
    
    return(errorMeasurement)
  })

  accuracyPerSku <- fastDoCall("rbind", accuracyPerSku)
  
  keys <- dataPred %>%
    group_by(sku_code_prefix) %>%
    group_keys()
  
  accuracyPerSku <- data.frame("sku_code_prefix" = as.character(keys$sku_code_prefix),
                               accuracyPerSku,
                               stringsAsFactors = FALSE)
  
  return(accuracyPerSku)
}

rmse <- function(actuals,
                 preds) {
  
  errors <- actuals - preds
  
  value <- sqrt(mean((errors)^2, na.rm = TRUE)) 
  
  value <- round(value, digits = 2)
  value <- data.frame("rmse" = value)
  return(value)
}

rmse_scaled <- function(actuals,
                        preds) {
  
  
  if(all(actuals == 0) && all(preds == 0)) {
    value <- 0
  } else {
    
    errors <- actuals - preds
    
    rootMeanSqError <- sqrt(mean((errors)^2, na.rm = TRUE)) 
    
    value <- rootMeanSqError / mean(actuals)
    
  }
  
  value <- round(value, digits = 2)
  value <- data.frame("rmse_scaled" = value)
  return(value)
}

mae <- function(actuals,
                preds) {
  
  errors <- actuals - preds
  
  value <- mean(abs(errors), na.rm = TRUE)
  
  value <- round(value, digits = 2)
  value <- data.frame("mae" = value)
  return(value)
}

mae_scaled <- function(actuals,
                       preds) {
  
  if(all(actuals == 0) && all(preds == 0)) {
    value <- 0
  } else {
    
    errors <- actuals - preds
    
    meanAbsError <- mean(abs(errors), na.rm = TRUE)
    
    value <- meanAbsError / mean(actuals)
  }
  
  value <- round(value, digits = 2)
  value <- data.frame("mae_scaled" = value)
  return(value)
}

smape <- function(actuals,
                   preds) {
  
  if(all(actuals == 0) && all(preds == 0)) {
    value <- 0
  } else {
    
    errors <- actuals - preds
    value <- mean(abs(errors) / (abs(actuals) + abs(preds) / 2), na.rm = TRUE)
  }
  
  value <- round(value, digits = 2)
  value <- data.frame("smape" = value)
  return(value)
}

mase <- function(actuals,
                 preds) {
  
  if(all(actuals == 0) && all(preds == 0)) {
    value <- 0
  } else {
    
    mae_naiveOneStep <- mean(abs(actuals[-1] - actuals[-length(actuals)]))
    
    meanAbsError <- mae(actuals, preds)
    
    value <- meanAbsError / mae_naiveOneStep
  }
  
  value <- round(value, digits = 2)
  value <- data.frame("mase" = value)
  return(value)
}

#---

