###### Combined Modelling #######

mClinicaML_combined2 <- function(data,
                                model,
                                method_reg = "cv",
                                number_reg = 5,
                                model_binary = NULL,
                                method_class = "cv",
                                number_class = 5,
                                removeFeatures = NULL,
                                corrFeatures = FALSE,
                                n.ahead = 4,
                                updateForecasts = TRUE) {
  
  data <- data %>%
    mutate(actuals_binary = as.factor(demand > 0),
           actuals = demand)
  
  # For now we don't want to use the feature molecule.
  if("molecule" %in% colnames(data)) {
    data <- data %>%
      select(-molecule)
  }
  
  # Train/Test-Split according to n.ahead
  maxWeek <- max(data$Ordered_date_yearweek)
  
  data <- data %>%
    mutate(label = ifelse(Ordered_date_yearweek <= maxWeek - n.ahead, "train", "test"))
  
  # Preprocessing Functions
  data <- centerDemand2(data)
  data <- scalePrices(data)
  data <- recalcDemandFeatures2(data)
  
  if(corrFeatures) {
    # Additional Correlation Features
    corrData <- getCorrelatedSkus(data)
    data <- addCorrLags(data, corrData)
  }
  
  #---
  
  # We don't want to use sku_code and sku_code_prefix as features of our model, but we need both in later steps
  # to aggregate the predictions.
  dataTrain <- data %>%
    filter(label == "train") %>%
    select(-c(sku_code, 
              sku_code_prefix,
              Ordered_date_yearweek,
              unit_selling_price,
              hasPromotion,
              mean,
              sd1,
              sd2,
              demandMin, 
              label,
              actuals_binary,
              actuals))
  
  if(!is.null(removeFeatures)) {
    removeFeatures <- intersect(colnames(dataTrain), removeFeatures)
    
    dataTrain <- dataTrain %>%
      select(-removeFeatures)
  }
  
  customSummary <- function(data, lev = NULL, model = NULL) {
    out <- rmse_scaled(actuals = data$obs, preds = data$pred)
    names(out) <- "CUSTOM"
    return(out)
  }
  # Tuning the model-parameters using caret's internal cross-validation functionality
  fitControl_reg <- trainControl(method = method_reg,
                                 number = number_reg)
  
  
  # Regression model
  fit_reg <- train(demand ~., 
                   data = dataTrain, 
                   method = model, 
                   trControl = fitControl_reg)
  
  if(!is.null(model_binary)) {
    
    # Binary model to find 0-demands
    dataTrain <- data %>%
      filter(label == "train") %>%
      select(-c(sku_code, 
                sku_code_prefix,
                Ordered_date_yearweek,
                unit_selling_price,
                hasPromotion,
                mean,
                sd1,
                sd2,
                demandMin,
                label,
                demand,
                actuals))
    
    if(!is.null(removeFeatures)) {
      removeFeatures <- intersect(colnames(dataTrain), removeFeatures)
      
      dataTrain <- dataTrain %>%
        select(-removeFeatures)
    }
    
    fitControl_class <- trainControl(method = method_class,
                                     number = number_class)
    
    fit_class <- train(actuals_binary ~.,
                       data = dataTrain,
                       method = model_binary,
                       trControl = fitControl_class,
                       metric = "Kappa")
  } else {
    fit_class <- NULL
  }
  
  if(updateForecasts) {
    
    preds_centered <- getUpdatedPreds2(fit_reg = fit_reg, 
                                      fit_class = fit_class, 
                                      data = data, 
                                      n.ahead = n.ahead)$preds
  } else {
    preds_centered <- predict(fit, newdata = data)
    
    if(!is.null(model_binary)) {
      preds_binary <- predict(fit_class, newdata = data)
      
      preds_centered[!as.logical(preds_binary)] <- 0
    }
  }
  
  # Negative Forecasts are set to 0. In order to be able to track the 'real' forecasts for further insights
  # we create the column 'preds_real'.
  
  dataAll <- data %>%
    add_column(preds_centered) %>%
    mutate(actuals_centered = demand,
           preds = preds_centered * sd1 + mean)
  
  dataAll$preds[dataAll$preds < 0] <- 0
  dataAll <- dataAll %>%
    mutate(errors = actuals - preds)
  
  #---
  
  dataAll <- setorderv(dataAll, cols = c("Ordered_date_yearweek", "sku_code"))
  
  dataPred_noAgg <- dataAll %>%
    filter(label == "test")
  
  # In order to be able to evaluate how well our model predicts the overall demand of every sku_code per week
  # we aggregate all our predictions on a weekly sku-code level.
  dataAll_aggSkuCode <- dataAll %>%
    group_by(sku_code) %>%
    group_by(Ordered_date_yearweek, sku_code_prefix, label, add = TRUE) %>%
    summarise("actuals" = sum(actuals), 
              "preds" = sum(preds),
              "actuals_centered" = sum(actuals_centered),
              "preds_centered" = sum(preds_centered),
              "hasPromotion" = any(hasPromotion)) %>%
    mutate(errors = actuals - preds)
  
  dataPred_aggSkuCode <- dataAll_aggSkuCode %>%
    filter(label == "test")
  
  # In order to be able to evaluate how well our models predict the overall demand of every sku_code_prefix per week
  # we aggregate all our predictions on a weekly sku-code_prefix level.
  dataAll_aggSkuPrefix <- dataAll %>%
    group_by(Ordered_date_yearweek, sku_code_prefix, label, add = TRUE) %>%
    summarise("actuals" = sum(actuals), 
              "preds" = sum(preds),
              "actuals_centered" = sum(actuals_centered),
              "preds_centered" = sum(preds_centered),
              "hasPromotion" = any(hasPromotion)) %>%
    mutate(errors = actuals - preds)
  
  dataPred_aggSkuPrefix <- dataAll_aggSkuPrefix %>%
    filter(label == "test")
  
  return(list("dataPred_noAgg" = as.data.frame(dataPred_noAgg),
              "dataPred_aggSkuCode" = as.data.frame(dataPred_aggSkuCode),
              "dataPred_aggSkuPrefix" = as.data.frame(dataPred_aggSkuPrefix),
              "dataAll_noAgg" = as.data.frame(dataAll),
              "dataAll_aggSkuCode" = as.data.frame(dataAll_aggSkuCode),
              "dataAll_aggSkuPrefix" = as.data.frame(dataAll_aggSkuPrefix),
              "fit_reg" = fit_reg,
              "fit_class" = fit_class))
}

###### Real Preds ######

getUpdatedPreds2 <- function(fit_reg,
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
      
      data <- addTransformedZeros2(data = data,
                                  preds_binary = preds_binary,
                                  dataTest_indices = dataTest_indices)
    }
    
    if(h == max(predWeeks)) {
      return(data)
    }

    data <- recalcDemandFeatures2(data)
    
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

###### Centering #####

centerDemand2 <- function(data) {
  
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
      mutate(demandMin = min(demand),
             sd2 = sd(demand))
    
    return(group_mod)
  })
  data_mod <- rbindlist(dataGroups_mod)
  
  data_mod <- setorderv(data_mod, cols = c("sku_code_prefix", "Ordered_date_yearweek"))
  
  return(data_mod)
}

##### Recalculate Lag Features ######

# This function can be used to recalculate the lag-demand-features after transformations have been applied to
# the demand.

recalcDemandFeatures2 <- function(data) {
  
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
      select(-colsToUpdate) %>%
      mutate(demand = (demand - demandMin) / sd2)
    
    data <- getLagDemand(data,
                         aggLevel = aggLevel,
                         lags = lags)
    
    data <- data %>%
      mutate(demand = demand * sd2 + demandMin)
  }
  
  return(data)
}

addTransformedZeros2 <- function(data,
                                 preds_binary,
                                 dataTest_indices) {
  
  data <- data %>%
    mutate(zeros = -mean / sd1) 
  
  zerosTransformed <- data$zeros
  zerosTransformed_toAdd <- zerosTransformed[dataTest_indices][!as.logical(preds_binary)]
  
  data$preds[dataTest_indices][!as.logical(preds_binary)] <- zerosTransformed_toAdd
  data$demand[dataTest_indices][!as.logical(preds_binary)] <- zerosTransformed_toAdd
  
  return(data)
}


