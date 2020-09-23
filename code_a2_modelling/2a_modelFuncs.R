###### Packages #####

library(caret)
library(smooth)
library(doParallel)
library(foreach)
library(tidyverse)
library(forecast)
library(Gmisc)
library(data.table)
library(pracma)
library(lubridate)

#---

###### Aggregate Prices ######

aggregatePrices_SkuCode <- function(dataTrain) {
  
  price_names_prefix <- c("mean", "max", "min")
  price_names_skuCode <- paste0(price_names_prefix, "Price", "SkuCode", sep = "")
  price_names_skuPrefix <- paste0(price_names_prefix, "Price", "SkuPrefix", sep = "")
  
  lagDemand_names_skuCode <- paste0("demand_lag", 1:4, "_SkuCode", sep = "")
  lagDemand_names_skuPrefix <- paste0("demand_lag", 1:4, "_SkuPrefix", sep = "")
  
  lagCumDemand_names_skuCode <- paste0("cumDemand_lag", 1:4, "_SkuCode", sep = "")
  lagCumDemand_names_skuPrefix <- paste0("cumDemand_lag", 1:4, "_SkuPrefix", sep = "")
  
  staticCols <- c("sku_code_prefix", 
                  "quantity_perPackage",
                  price_names_skuCode,
                  price_names_skuPrefix,
                  lagDemand_names_skuCode,
                  lagDemand_names_skuPrefix,
                  lagCumDemand_names_skuCode,
                  lagCumDemand_names_skuPrefix,
                  "pharmaciesActive_month", 
                  "pharmaciesActive_sinceBeginning")
  
  dataTrain_agg <- dataTrain %>%
    group_by(sku_code) %>%
    group_by(Ordered_date_yearweek, add = TRUE) %>%
    group_by(.dots = staticCols, add = TRUE) %>%
    summarise("demand" = sum(demand),
              "demandOfUnits" = sum(demandOfUnits),
              "hasPromotion" = any(promoDiscount > 0))
  
  return(as.data.frame(dataTrain_agg))
}

#---

####### Training Skus seperately #############

mClinicaML_split <- function(data = data,
                             model,
                             formula,
                             n.ahead = 4,
                             transformation = as_mapper(sqrt),
                             backtransformation = as_mapper(~ .x^2),
                             errorMeasures = c("rmse", 
                                               "mae",
                                               "smape",
                                               "mase")) {
  
  if("molecule" %in% colnames(data)) {
    data <- data %>%
      select(-molecule)
  }
  
  maxWeek <- max(data$Ordered_date_yearweek)
  
  dataGroups <- data %>%
    group_by(sku_code_prefix) %>%
    group_split()
  
  registerDoParallel()
  
  resultsPerSku <- foreach(dataGroup = dataGroups, .packages = c("dplyr", "caret")) %dopar% {
    
    dataTrain <- dataGroup %>%
      filter(Ordered_date_yearweek <= maxWeek - n.ahead)
    
    dataTest <- dataGroup %>%
      filter(Ordered_date_yearweek > maxWeek - n.ahead)
    
    dataTrain_mod <- dataTrain %>%
      select(-c(sku_code, sku_code_prefix)) %>%
      mutate_at(.vars = vars("demand"), .funs = transformation)
    
    dataTest_mod <- dataTest %>%
      select(-c(sku_code, sku_code_prefix)) %>%
      mutate_at(.vars = vars("demand"), .funs = transformation)
    
    fitControl <- trainControl(method = "cv",
                               number = 5)
    
    fit <- train(formula, 
                 data = dataTrain_mod, 
                 method = model, 
                 trControl = fitControl)
    
    preds_train <- backtransformation(predict(fit, newdata = dataTrain_mod))
    preds_test <- backtransformation(predict(fit, newdata = dataTest_mod))
    
    errors_train <- dataTrain_mod$demand - preds_train
    errors_test <- dataTest_mod$demand - preds_test
    
    # NOTE: 'actuals' is simply identical to demand. We add this column just for compatibility with other function
    # for evaluating the forecast results.
    dataAll_train <- data.frame(dataTrain, 
                                "actuals" = dataTrain$demand, 
                                "preds" = preds_train, 
                                "errors" = errors_train, 
                                "label" = "train")
    
    dataAll_test <- data.frame(dataTest, 
                               "actuals" = dataTest$demand,
                               "preds" = preds_test, 
                               "errors" = errors_test,
                               "label" = "test")
    
    dataAll <- rbind(dataAll_train, dataAll_test)
    dataAll <- dataAll[order(dataAll$sku_code, dataAll$Ordered_date_yearweek), ]
    
    return(list("dataAll" = dataAll,
                "fit" = fit))
  }
  
  keys <- data %>%
    group_by(sku_code_prefix) %>%
    group_keys()
  sku_codes_prefix <- keys$sku_code_prefix
  
  fit_list <- lapply(resultsPerSku, function(result) {
    result$fit
  })
  names(fit_list) <- sku_codes_prefix
  
  dataAll_list <- lapply(resultsPerSku, function(result) {
    result$dataAll
  })
  dataAll <- rbindlist(dataAll_list)
  
  dataPred_noAgg <- dataAll %>%
    filter(label == "test")
  
  # In order to be able to evaluate how well our model predicts the overall demand of every sku_code per week
  # we aggregate all our predictions on a weekly sku-code level.
  dataAll_aggSkuCode <- dataAll %>%
    group_by(sku_code) %>%
    group_by(Ordered_date_yearweek, sku_code_prefix, label, add = TRUE) %>%
    summarise("actuals" = sum(actuals), 
              "preds" = sum(preds),
              "hasPromotion" = any(promoDiscount > 0)) %>%
    mutate(errors = actuals - preds)
  
  dataPred_aggSkuCode <- dataAll_aggSkuCode %>%
    filter(label == "test")
  
  # In order to be able to evaluate how well our models predict the overall demand of every sku_code_prefix per week
  # we aggregate all our predictions on a weekly sku-code_prefix level.
  dataAll_aggSkuPrefix <- dataAll %>%
    group_by(Ordered_date_yearweek, sku_code_prefix, label, add = TRUE) %>%
    summarise("actuals" = sum(actuals), 
              "preds" = sum(preds),
              "hasPromotion" = any(promoDiscount > 0)) %>%
    mutate(errors = actuals - preds)
  
  dataPred_aggSkuPrefix <- dataAll_aggSkuPrefix %>%
    filter(label == "test")
  
  return(list("dataPred_noAgg" = dataPred_noAgg,
              "dataPred_aggSkuCode" = dataPred_aggSkuCode,
              "dataPred_aggSkuPrefix" = dataPred_aggSkuPrefix,
              "dataAll_noAgg" = dataAll,
              "dataAll_aggSkuCode" = dataAll_aggSkuCode,
              "dataAll_aggSkuPrefix" = dataAll_aggSkuPrefix,
              "fit" = fit_list))
}

#---

###### Combined Modelling #######

mClinicaML <- function(data,
                       model_reg,
                       method_reg = "cv",
                       number_reg = 5,
                       model_binary = NULL,
                       method_class = "cv",
                       number_class = 5,
                       customMetric = "rmse_scaled",
                       useDetrending = FALSE,
                       useCorrFeatures = FALSE,
                       removeFeatures = NULL,
                       updateForecasts = TRUE,
                       n.ahead = 4) {

  useBinaryModel <- !is.null(model_binary)
  
  # Train/Test-Split according to n.ahead
  maxWeek <- max(data$Ordered_date_yearweek)
  
  data <- data %>%
    mutate(label = ifelse(Ordered_date_yearweek <= maxWeek - n.ahead, "train", "test")) 

  preprocResults <- preprocessData(data = data,
                                   removeFeatures = removeFeatures, 
                                   useCorrFeatures = useCorrFeatures, 
                                   useDetrending = useDetrending)
  
  # In order to avoid recalculating the correlation data given by 'corrData' while updating the predictions
  # we have to pass 'corrData' down as well.
  data <- preprocResults$dataPreproc
  dataTrain_reg <- preprocResults$dataTrain_reg
  dataTrain_class <- preprocResults$dataTrain_class
  corrData <- preprocResults$corrData

  customSummary <- function(data, lev = NULL, model = NULL) {
    args <- list("actuals" = data$obs, "preds" = data$pred)
    out <- fastDoCall(customMetric, args)
    names(out) <- "CUSTOM"
    return(out)
  }
  # Tuning the model-parameters using caret's internal cross-validation functionality
  fitControl_reg <- trainControl(method = method_reg,
                                 number = number_reg,
                                 summaryFunction = customSummary)

  # Regression model
  fit_reg <- train(demand ~., 
                   data = dataTrain_reg, 
                   method = model_reg, 
                   trControl = fitControl_reg, 
                   metric = "CUSTOM",
                   maximize = FALSE)
  
  if(useBinaryModel) {
    fitControl_class <- trainControl(method = method_class,
                                     number = number_class)
    
    fit_class <- train(actuals_binary ~.,
                       data = dataTrain_class,
                       method = model_binary,
                       trControl = fitControl_class,
                       metric = "Kappa")
  } else {
    fit_class <- NULL
  }
  
  if(updateForecasts) {
    
    preds_transformed <- getUpdatedPreds(fit_reg = fit_reg, 
                                         fit_class = fit_class, 
                                         data = data, 
                                         n.ahead = n.ahead,
                                         corrData = corrData)$preds
  } else {
    preds_transformed <- predict(fit_reg, newdata = data)
    
    if(!is.null(model_binary)) {
      preds_binary <- predict(fit_class, newdata = data)
      
      preds_transformed[!as.logical(preds_binary)] <- 0
    }
  }
  
  # Negative Forecasts are set to 0. In order to be able to track the 'real' forecasts for further insights
  # we create the column 'preds_real'.
  preds_real <- preds_transformed
  
  dataAll <- data %>%
    add_column(preds_real, preds_transformed) %>%
    mutate(actuals_transformed = demand,
           preds = (preds_transformed * sd2 + demandMin) * sd1 + mean)

  if(useDetrending) {
    dataAll <- dataAll %>%
      mutate(preds = preds * detrendFactor)
  }
  
  dataAll$preds[dataAll$preds < 0] <- 0
  dataAll <- dataAll %>%
    mutate(errors = actuals - preds)

  aggResults <- getAggResults(dataAll)

  results <- append(aggResults, 
                    list("fit_reg" = fit_reg,
                         "fit_class" = fit_class,
                         "useDetrending" = useDetrending,
                         "useCorrFeatures" = useCorrFeatures,
                         "model_reg" = model_reg,
                         "model_binary" = model_binary,
                         "customMetric" = customMetric,
                         "n.ahead" = n.ahead))
  
  return(results)
}

#---

###### Preprocessing ######

# This function handles all the general preprocessing steps that we have to do for every model fit.
# IMPORTANT NOTE: The feature 'demand' gets rescaled and optionally detrended during the preprocessing, because
# it is supposed to be used for training the machine-learning models. The feature 'actuals' on the other hand
# equals the original unmodified demand and is therefore supposed to be used for evaluating the modelling results
# after retransformation of the predictions.

preprocessData <- function(data,
                           removeFeatures = NULL,
                           useCorrFeatures = FALSE,
                           useDetrending = FALSE) {
  
  data <- data %>%
    mutate(actuals_binary = as.factor(demand > 0),
           actuals = demand)
  
  # For now we don't want to use the feature molecule.
  if("molecule" %in% colnames(data)) {
    data <- data %>%
      select(-molecule)
  }
  
  dataPreproc <- data 
  
  # Preprocessing Functions
  if(useDetrending) {
    dataPreproc <- detrendDemand(dataPreproc)
  }
  
  dataPreproc <- centerDemand(dataPreproc)
  dataPreproc <- scalePrices(dataPreproc)
  dataPreproc <- recalcDemandFeatures(dataPreproc)
  
  if(useCorrFeatures) {
    # Additional Correlation Features
    corrData <- getCorrelatedSkus(dataPreproc)
    dataPreproc <- addCorrLags(dataPreproc, corrData)
  } else {
    corrData <- NULL
  }

  browser()
  # In order to get our final training data we have to remove all newly added features like 'mean' etc.
  # On top of that we want to get rid of all character-features like 'sku_code' etc.
  dataTrain <- dataPreproc %>%
    filter(label == "train") %>%
    select_at(vars(colnames(data))) %>%
    select_if(.predicate = ~ !is.character(.)) %>%
    select(-c(unit_selling_price,
              Ordered_date_yearweek))
  
  if(!is.null(removeFeatures)) {
    removeFeatures <- intersect(colnames(dataTrain), removeFeatures)
    
    dataTrain <- dataTrain %>%
      select(-removeFeatures)
  }
  
  dataTrain_reg <- dataTrain %>%
    select(-c(actuals_binary, 
              actuals))
    
    dataTrain_class <- dataTrain %>%
      select(-c(demand,
                actuals))
  
  return(list("dataPreproc" = dataPreproc,
              "dataTrain_reg" = dataTrain_reg,
              "dataTrain_class" = dataTrain_class,
              "corrData" = corrData))
}

#---

detrendDemand <- function(data) {
  
  dataGroups <- data %>%
    group_by(sku_code_prefix) %>%
    group_split()
  
  dataGroups_mod <- lapply(dataGroups, function(group) {
    
    group_mod <- group %>%
      mutate(detrendFactor = pharmActive_sinceBeginning) %>%
      mutate(demand = demand / detrendFactor)
    
    return(group_mod)
  })
  data_mod <- rbindlist(dataGroups_mod)
  data_mod <- setorderv(data_mod, cols = c("sku_code_prefix", "Ordered_date_yearweek"))
  
  return(data_mod)
}

#---

# NOTE: 'centerData' is supposed to be used only on data that has been grouped by sku_code_prefix.

centerDemand <- function(data) {
  
  dataGroups <- data %>%
    group_by(sku_code_prefix) %>%
    group_split()
  
  dataGroups_mod <- lapply(dataGroups, function(group) {
    
    if("label" %in% colnames(group)) {
      groupTrain <- group %>%
        filter(label == "train")
      
    } else {
      groupTrain <- group
    }
    
    mean <- mean(groupTrain$demand)
    sd1 <- sd(groupTrain$demand)
    group_mod <- cbind(group, mean, sd1)
    
    group_mod <- group_mod %>%
      mutate(demand = (demand - mean) / sd1) %>%
      mutate(demandMin = min(demand)) %>%
      mutate(demand = demand - demandMin) %>%
      mutate(sd2 = sd(demand)) %>%
      mutate(demand = demand / sd2)
    
    return(group_mod)
  })
  data_mod <- rbindlist(dataGroups_mod)
  
  data_mod <- setorderv(data_mod, cols = c("sku_code_prefix", "Ordered_date_yearweek"))
  
  return(data_mod)
}

scalePrices <- function(data) {
  
  dataGroups <- data %>%
    group_by(sku_code_prefix) %>%
    group_split()
  
  dataGroups_mod <- lapply(dataGroups, function(group) {
    
    if("label" %in% colnames(group)) {
      groupTrain <- group %>%
        filter(label == "train")
      
    } else {
      groupTrain <- group
    }
    
    scalePrice <- mean(groupTrain$pricePerSingleUnit)
    
    # We can't just simply modify all columns matching 'price', because then we'd also modify
    # "priceRatio_min_allTime_SkuPrefix" for example. 
    group_mod <- group %>%
      mutate_at(vars(matches("meanPrice|medianPrice|maxPrice|minPrice|pricePer")),
                .funs = ~ . / scalePrice)
    
    return(group_mod)
  })
  data_mod <- rbindlist(dataGroups_mod)
  
  data_mod <- setorderv(data_mod, cols = c("sku_code_prefix", "Ordered_date_yearweek"))
  
  return(data_mod)
}

#---

getCorrelatedSkus <- function(data) {
  
  if("label" %in% colnames(data)) {
    data_Train <- data %>%
      filter(label == "train")
  } else {
    data_Train <- data
  }
  
  dataAgg <- data_Train %>%
    group_by(sku_code_prefix, Ordered_date_yearweek) %>%
    summarise(demand = sum(demand))
  
  dataToAdd <- data_Train %>%
    group_by(Ordered_date_yearweek) %>%
    slice(1) %>%
    select(Ordered_date_yearweek, pharmActive_sinceBeginning, pharmAverageOrdersMonth)
  
  dataAgg <- left_join(dataAgg, dataToAdd, by = "Ordered_date_yearweek")
  
  dataAgg <- detrendDemand(dataAgg)
  dataAgg <- centerDemand(dataAgg)
  
  dataGroups <- dataAgg %>%
    group_by(sku_code_prefix) %>%
    group_split()
  
  keys <- dataAgg %>%
    group_by(sku_code_prefix) %>%
    group_keys()
  
  corrs <- lapply(seq_along(dataGroups), function(i) {
    demand <- as.data.frame(dataGroups[[i]]$demand[-1])
    colnames(demand) <- keys$sku_code_prefix[i]
    
    demandOthers_list <- lapply(dataGroups[-i], function(group) {
      group$demand[-nrow(group)]
    })
    demandOthers <- fastDoCall("cbind", demandOthers_list)
    colnames(demandOthers) <- keys$sku_code_prefix[-i]
    
    demandMatrix <- cbind(demand, demandOthers)
    
    corrs <- cor(demandMatrix)[-1, 1]
    corrs <- sort(corrs)
    
    minPrefixes <- names(head(corrs, 3))
    maxPrefixes <- names(tail(corrs, 3))
    
    return(list("minCorrs" = minPrefixes,
                "maxCorrs" = maxPrefixes))
  })
  
  names(corrs) <- keys$sku_code_prefix
  return(corrs)
}


addCorrLags <- function(data,
                        corrData) {
  
  # We have to delete the old lag features if we call this function to recalculate the features during the 
  # updating-process of the forecasting
  
  if(any(colnames(data) == "demand_lag1_negCorr1")) {
    colsToRemove <- grepl(colnames(data), pattern = "negCorr|posCorr")
    data <- data[, !colsToRemove]
  }
  
  skuPrefixes <- unique(data$sku_code_prefix)
  
  groups_mod_list <- lapply(skuPrefixes, function(skuPrefix) {
    group <- data %>%
      filter(sku_code_prefix == skuPrefix)
    
    minPrefixes <- corrData[[skuPrefix]]$minCorrs
    maxPrefixes <- corrData[[skuPrefix]]$maxCorrs
    
    for(i in seq_along(minPrefixes)) {
      demandLag <- data %>%
        filter(sku_code_prefix == minPrefixes[i]) %>%
        group_by(Ordered_date_yearweek) %>%
        slice(1) %>%
        group_by() %>%
        select(Ordered_date_yearweek, demand_lag1_SkuPrefix)
      
      colName <- paste0("demand_lag1_negCorr", i)
      colnames(demandLag)[2] <- colName
      
      group <- left_join(group, demandLag, by = "Ordered_date_yearweek")
    }
    
    for(i in seq_along(maxPrefixes)) {
      demandLag <- data %>%
        filter(sku_code_prefix == maxPrefixes[i]) %>%
        group_by(Ordered_date_yearweek) %>%
        slice(1) %>%
        group_by() %>%
        select(Ordered_date_yearweek, demand_lag1_SkuPrefix)
      
      colName <- paste0("demand_lag1_posCorr", i)
      colnames(demandLag)[2] <- colName
      
      group <- left_join(group, demandLag, by = "Ordered_date_yearweek")
    }
    return(group)
  })
  
  groups_mod <- rbindlist(groups_mod_list)
  groups_mod <- setorderv(groups_mod, cols = c("sku_code_prefix", "Ordered_date_yearweek"))
  
  return(groups_mod)
}

#---

###### Update Predictions ######

getUpdatedPreds <- function(fit_reg,
                            fit_class = NULL,
                            data,
                            n.ahead,
                            corrData = NULL) {
  
  preds <- rep.int(0, nrow(data))
  data <- add_column(data, preds)
  
  dataTrain_indices <- data$label == "train"
  data$preds[dataTrain_indices] <- predict(fit_reg, newdata = data[dataTrain_indices, ])
  
  if(!is.null(fit_class)) {
    preds_binary <- predict(fit_class, newdata = data[dataTrain_indices, ])
    data[dataTrain_indices, "preds"][!as.logical(preds_binary)] <- 0
  }
  
  maxWeek <- max(data$Ordered_date_yearweek)
  predWeeks <- seq(maxWeek - n.ahead + 1, maxWeek, by = 1)
  
  # 1) In order to be able to call the function 'getLagDemand' to recalculate the lag-demand-features, we have to
  # replace the demand of the test-time-horizon by our current predictions. On top of that we have to remove the
  # original lag-demand-features before recalculating.
  # 2) We also have to keep the first observation of cumDemand_lag1 for each aggLevel before calling
  # 'getLagDemand'. This observation of cumDemand_lag1 has to be added to the calculated cumDemand_lag
  # features after recalculating the lag-demand-features.
  for(h in predWeeks) {
    
    dataTest_indices <- data$Ordered_date_yearweek == h
    preds <- predict(fit_reg, newdata = data[dataTest_indices, ])
    preds[preds < 0] <- 0
    
    data$preds[dataTest_indices] <- preds
    data$demand[dataTest_indices] <- preds
    
    if(!is.null(fit_class)) {
      preds_binary <- predict(fit_class, newdata = data[dataTest_indices, ])
      
      data <- addTransformedZeros(data = data,
                                  preds_binary = preds_binary,
                                  dataTest_indices = dataTest_indices)
    }
    
    if(h == max(predWeeks)) {
      return(data)
    }
    
    data <- recalcDemandFeatures(data)

    if(!is.null(corrData)) {
      data <- addCorrLags(data, corrData)
    }
  }
  
  # For all forecast steps bigger than 1 we have to update the lag demands to get replicate a forecasting procedure
  # that allows predicting several weeks from a fixed time point in the past.
  # 1) h-step = 2: In this case we have to update demand_lag1 and cumDemand_lag1 by pred_hstep1.
  # demand_lag2 is identical to the last real demand. cumDemand_lag2 also only depends on real demands.
  # 2) h-step = 3: In this case we have to update demand_lag1, demand_lag2 and cumDemand_lag1, cumDemand_lag2.
  # demand_lag1 and cumDemand_lag1 gets updated identically by the forecast of h-step 2. demand_lag2 will be 
  # identical to demand_lag1 of the week before and cumDemand_lag2 will also be identical to cumDemand_lag1 of the 
  # week before.
  #
  # 3) All in all we should be able to update the training data using the following procedure:
  # a) Update the feature 'demand' using the most recent forecast. 
  # b) Recalculate all demand-lag-features using the function getLagDemand
  # c) Create new forecast
  # Big downside: The way we create the cumDemand-lag features right now causes one problem: The cumDemand includes
  # of the early stages of the data includes weeks that are cut off because we can't create lagged price features for
  # them. Therefor cumDemand contains the demand of weeks that is not part of the current training data. 
  # BUT: We can fix that! All we have to do is use the cumDemand_lag1 of the very first week of the original-training
  # data as the correction factor that we add on the newly calculated lag-demands. 
  # 
  # 4) New idea for a procedure:
  # a) Forecast current h-step and replace the demand of that week with the current forecast
  # b) Group data per sku_code / sku_code + distributor and update the lag features. The lag-demands can easily be
  # simply recalculated. The cumDemand can also be updated using the new demand Data. It sounds most reasonable to
  # pick a fixed time point of the past and update starting from this point.
  
}

#---

# This function can be used to recalculate the lag-demand-features after transformations have been applied to
# the demand.

recalcDemandFeatures <- function(data) {
  
  demandCols_names <- data %>%
    select_at(vars(matches("demand_lag"))) %>%
    colnames()
  
  aggLevels <- list()
  if(any(grepl(x = demandCols_names, pattern = "Distr"))) {
    aggLevels <- append(aggLevels, list(c("sku_code", "distributor_id")))
  }
  if(any(grepl(x = demandCols_names, pattern = "SkuCode"))) {
    aggLevels <- append(aggLevels, list(c("sku_code")))
  }
  if(any(grepl(x = demandCols_names, pattern = "SkuPrefix"))) {
    aggLevels <- append(aggLevels, list(c("sku_code_prefix")))
  }
  
  for(aggLevel in aggLevels) {
    
    if("distributor_id" %in% aggLevel) {
      prefix <- "Distr"
    } else if("sku_code" %in% aggLevel) {
      prefix <- "SkuCode"
    } else {
      prefix <- "SkuPrefix"
    }
    
    # We have to extract the used lags in order to be able to call 'getLagDemand'.
    lagCols_names <- data %>%
      select_at(vars(matches("cumDemand_lag"))) %>%
      select_at(vars(matches("SkuPrefix"))) %>%
      colnames()
    
    lags <- as.numeric(str_extract_all(lagCols_names, "[[:digit:]]"))
    
    colsToUpdate <- data %>% 
      select(matches("demand_lag|Demand_lag")) %>%
      select(matches(prefix)) %>%
      colnames()

    data <- data %>%
      select(-colsToUpdate)
    
    data <- getLagDemand(data,
                         aggLevel = aggLevel,
                         lags = lags)
  }
  
  return(data)
}


addTransformedZeros <- function(data,
                                preds_binary,
                                dataTest_indices) {
  
  data <- data %>%
    mutate(zeros = 0)
  
  if("mean" %in% colnames(data)) {
    data <- data %>%
      mutate(zeros = ((zeros - mean) / sd1 - demandMin) / sd2)
  }
  
  zerosTransformed <- data$zeros
  zerosTransformed_toAdd <- zerosTransformed[dataTest_indices][!as.logical(preds_binary)]
  
  data$preds[dataTest_indices][!as.logical(preds_binary)] <- zerosTransformed_toAdd
  data$demand[dataTest_indices][!as.logical(preds_binary)] <- zerosTransformed_toAdd
  
  data <- data %>%
    select(-zeros)
  
  return(data)
}

#---

##### Aggregate Results ######

# This function creates a month feature that can be used to aggregate pairs of 4 weeks. We can't simply use the feature
# 'month' that we can create using 'lubridate', because one week can belong to more than one month. In order to fix that
# we artificially assume that 4 weeks always build exactly one month.
# IMPORTANT ASSUMPTION: This function starts to aggregate weeks to 4-week-pairs starting from the end of the time horizon.
# EXAMPLE: We have the weeks 1, 2, ..., 9 available. Then week 1 will belong to month 1, week 2,...,5 belongs to month 2
# and week 6,...,9 to month 3.

getMonthFeature <- function(dataAll) {
  
  allWeeks <- unique(dataAll$Ordered_date_yearweek)
  firstWeek <- min(allWeeks)
  
  monthsCount <- ceiling(uniqueN(allWeeks) / 4)
  months <- 1:monthsCount
  
  numbWeeksFirstMonth <- uniqueN(dataAll$Ordered_date_yearweek) %% 4
  
  if(numbWeeksFirstMonth > 0) {
    weeksFirstMonth <- firstWeek:(firstWeek + numbWeeksFirstMonth - 1)
    
    monthData <- data.frame("Ordered_date_yearweek" = weeksFirstMonth, 
                            "month" = rep.int(1, length(weeksFirstMonth)))
    
    allWeeks <- allWeeks[-(1:length(weeksFirstMonth))]
    months <- months[-1]
    
  } else {
    monthData <- data.frame("Ordered_date_yearweek" = numeric(), 
                            "month" = numeric())
  }
  
  for(i in months) {
    month <- rep.int(i, 4)
    monthDataToAdd <- data.frame("Ordered_date_yearweek" = allWeeks[1:4], 
                                 "month" = month)
    
    monthData <- rbind(monthData, monthDataToAdd)
    allWeeks <- allWeeks[-(1:4)]
  }
  
  dataAll <- left_join(dataAll, monthData, by = "Ordered_date_yearweek")
  
  return(dataAll)
}

#---

# This function can be used to aggregate modelling results on a monthly basis. In order for this to work the object
# 'dataAll' has to contain a feature 'month'. The easiest way to get this feature is applying the function 
# 'getMonthFeature' to 'dataAll'.

aggregateMonth <- function(dataAll) {
  
  if(!"month" %in% colnames(dataAll)) {
    dataAll <- getMonthFeature(dataAll)
  }

  # We only want to keep months that contain 4 weeks. Otherwise the computed errors are artificially low for those
  # month with less than 4 weeks.
  dataAll <- dataAll %>%
    group_by(month) %>%
    mutate(numbWeeksOfMonth = uniqueN(Ordered_date_yearweek)) %>%
    group_by() %>%
    filter(numbWeeksOfMonth == 4)
  
  if(!("label" %in% colnames(dataAll))) {
    dataAll_agg <- dataAll %>%
      group_by(sku_code_prefix, month) %>%
      summarise(actuals = sum(actuals),
                preds = sum(preds),
                errors = actuals - preds)
    
  } else {
    
    # IMPORTANT: There mustn't be any month that contains both test and training weeks! 'label' is only used as a 
    # grouping-variable to keep it after aggregation. If every month is pure in the sense of only containing either
    # only training weeks or only test weeks, 'label' doesn't have any effect on the aggregation itself.
    dataAll_agg <- dataAll %>%
      group_by(sku_code_prefix, month, label) %>%
      summarise(actuals = sum(actuals),
                preds = sum(preds),
                errors = actuals - preds,
                firstWeekMonth = min(Ordered_date_yearweek)) %>%
      as.data.frame()
  }
  
  return(dataAll_agg)
}

#---

getAggResults <- function(dataAll) {
  
  dataAll <- setorderv(dataAll, cols = c("Ordered_date_yearweek", "sku_code"))
  
  dataPred_noAgg <- dataAll %>%
    filter(label == "test")
  
  # In order to be able to evaluate how well our model predicts the overall demand of every sku_code per week
  # we aggregate all our predictions on the sku-code level.
  dataAll_aggSkuCode <- dataAll %>%
    group_by(sku_code) %>%
    group_by(Ordered_date_yearweek, sku_code_prefix, label, add = TRUE) %>%
    summarise("actuals" = sum(actuals), 
              "preds" = sum(preds),
              "errors" = actuals - preds,
              "actuals_transformed" = sum(actuals_transformed),
              "preds_transformed" = sum(preds_transformed),
              "preds_real" = sum(preds_real),
              "hasPromotion" = any(promoDiscount > 0))
  
  dataPred_aggSkuCode <- dataAll_aggSkuCode %>%
    filter(label == "test")
  
  # In order to be able to evaluate how well our models predict the overall demand of every sku_code_prefix per week
  # we aggregate all our predictions on the sku-code_prefix level.
  dataAll_aggSkuPrefix <- dataAll %>%
    group_by(Ordered_date_yearweek, sku_code_prefix, label, add = TRUE) %>%
    summarise("actuals" = sum(actuals), 
              "preds" = sum(preds),
              "errors" = actuals - preds,
              "actuals_transformed" = sum(actuals_transformed),
              "preds_transformed" = sum(preds_transformed),
              "preds_real" = sum(preds_real),
              "hasPromotion" = any(promoDiscount > 0))
  
  dataPred_aggSkuPrefix <- dataAll_aggSkuPrefix %>%
    filter(label == "test")
  
  #---
  
  # Aggregation on a monthly-sku_code_prefix-level.
  dataAll_aggMonth <- dataAll %>%
    aggregateMonth()
  
  dataPred_aggMonth <- dataAll_aggMonth %>%
    filter(label == "test")
    
  
  return(list("dataPred_noAgg" = as.data.frame(dataPred_noAgg),
              "dataPred_aggSkuCode" = as.data.frame(dataPred_aggSkuCode),
              "dataPred_aggSkuPrefix" = as.data.frame(dataPred_aggSkuPrefix),
              "dataPred_aggMonth" = as.data.frame(dataPred_aggMonth),
              "dataAll_noAgg" = as.data.frame(dataAll),
              "dataAll_aggSkuCode" = as.data.frame(dataAll_aggSkuCode),
              "dataAll_aggSkuPrefix" = as.data.frame(dataAll_aggSkuPrefix),
              "dataAll_aggMonth" = as.data.frame(dataAll_aggMonth)))
}

#---

####### Benchmark Modelling ############

benchmarkModelling <- function(demandTs,
                               model = "nnetar",
                               errorMeasures = c("rmse", 
                                                 "rmse_scaled",
                                                 "mae",
                                                 "mae_scaled",
                                                 "smape",
                                                 "mase")) {
  
  maxWeek <- max(demandTs[[1]]$Ordered_date_yearweek)
  
  registerDoParallel()
  
  resultsPerSku <- foreach(dataSku = demandTs, .packages = c("forecast", "dplyr", "Gmisc")) %dopar% {
    
    trainData <- dataSku %>%
      filter(Ordered_date_yearweek <= maxWeek - 4)
    
    testData <- dataSku %>%
      filter(Ordered_date_yearweek > maxWeek - 4)
    
    y_train <- trainData$demand
    y_test <- testData$demand
    
    fit <- fastDoCall(model, args = list("y" = y_train, "h" = length(y_test)))
    preds_train <- fit$fitted
    preds_test <- as.vector(forecast(fit, h = length(y_test))$mean)
    preds_all <- c(preds_train, preds_test)
    
    # dataPred really only contains the results of the testing
    dataPred <- data.frame("actuals" = y_test,
                           "preds" = preds_test,
                           "Ordered_date_yearweek" = testData$Ordered_date_yearweek)
    
    dataPred_agg <- dataPred %>%
      group_by(Ordered_date_yearweek) %>%
      summarise_all(sum)
    
    dataPred_agg <- data.frame(dataPred_agg, 
                               "sku_code_prefix" = unique(testData$sku_code_prefix),
                               "label_train" = "test")
    
    # dataAll contains both the training and test data and the corresponding forecasts
    dataTrain <- data.frame("actuals" = y_train,
                            "preds" = preds_train,
                            "Ordered_date_yearweek" = trainData$Ordered_date_yearweek)
    
    dataTrain_agg <- dataTrain %>%
      group_by(Ordered_date_yearweek) %>%
      summarise_all(sum)
    
    dataTrain_agg <- data.frame(dataTrain_agg, 
                                "sku_code_prefix" = unique(trainData$sku_code_prefix),
                                "label_train" = "train")
    
    dataAll_agg <- rbind(dataTrain_agg, dataPred_agg)
    
    return(list("dataPred" = dataPred_agg,
                "dataAll" = dataAll_agg))
  }
  
  dataPredPerSku <- lapply(resultsPerSku, function(result) {
    result$dataPred
  })
  dataPred <- rbindlist(dataPredPerSku)
  dataPred$sku_code_prefix <- as.character(dataPred$sku_code_prefix)
  
  dataAllPerSku <- lapply(resultsPerSku, function(result) {
    result$dataAll
  })
  dataAll <- rbindlist(dataAllPerSku)
  dataAll$sku_code_prefix <- as.character(dataAll$sku_code_prefix)
  
  return(list("dataPred" = dataPred,
              "dataAll" = dataAll))
}


benchmarkModelling_mean <- function(demandTs,
                                    errorMeasures = c("rmse", 
                                                      "mae",
                                                      "smape",
                                                      "mase")) {
  
  maxWeek <- max(demandTs[[1]]$Ordered_date_yearweek)
  
  registerDoParallel()
  
  resultsPerSku <- foreach(dataSku = demandTs, .packages = c("forecast", "dplyr", "Gmisc")) %dopar% {
    
    trainData <- dataSku %>%
      filter(Ordered_date_yearweek <= maxWeek - 4)
    
    testData <- dataSku %>%
      filter(Ordered_date_yearweek > maxWeek - 4)
    
    y_train <- trainData$demand
    y_test <- testData$demand
    
    preds <- rep.int(mean(y_train), times = length(y_test))
    
    dataPred <- data.frame("actuals" = y_test,
                           "preds" = preds,
                           "Ordered_date_yearweek" = testData$Ordered_date_yearweek)
    
    dataPred_agg <- dataPred %>%
      group_by(Ordered_date_yearweek) %>%
      summarise_all(sum)
    
    dataPred_agg <- data.frame(dataPred_agg, 
                               "sku_code_prefix" = unique(testData$sku_code_prefix))
    
    return(dataPred_agg)
  }
  dataPred <- rbindlist(resultsPerSku)
  dataPred$sku_code_prefix <- as.character(dataPred$sku_code_prefix)
  
  accuraciesPerSku <- getAccuracyPerSku(dataPred = dataPred,
                                        errorMeasures = errorMeasures)
  
  return(list("accuracy" = accuraciesPerSku,
              "dataPred" = dataPred))
}

