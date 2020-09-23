#### Recalculate Demand Lags #####

# This function was used to recalculate the demand lags during the update process of forecasting. The special thing here was
# adding 'MISSING DEMAND' to the cumDemand_lag-features. The reason behind this was that the lag-demand-features were calculated
# on the basis of earlier weeks of the training data that were later cut off.

recalculateDemandCols <- function(data,
                                  aggLevel) {
  
  # Get missing Demand
  if("distributor_id" %in% aggLevel) {
    prefix <- "Distr"
  } else if("sku_code" %in% aggLevel) {
    prefix <- "SkuCode"
  } else {
    prefix <- "SkuPrefix"
  }
  cumDemandCol <- paste0("cumDemand_lag1_", prefix)
  
  demandMissing <- data %>%
    group_by(.dots = aggLevel) %>%
    arrange(Ordered_date_yearweek) %>%
    slice(1) %>%
    select(c(aggLevel, cumDemandCol)) %>%
    rename_at(vars(cumDemandCol), .funs = ~ "demandMissing")
  
  data <- left_join(data, demandMissing, by = aggLevel)
  
  #---
  
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
  
  # Finally we add the missing demand to all cumDemand-lag-features
  colsToUpdate <- grep(x = colsToUpdate, pattern = "cum", value = TRUE)
  
  data <- data %>%
    mutate_at(vars(colsToUpdate), .funs = ~ . + demandMissing) %>%
    select(-demandMissing)
  
  return(data)
}

detrendData <- function(data_skuPrefix) {
  
  # 1) Fitting linear model
  data_train_agg <- data_skuPrefix %>%
    filter(label == "train") %>%
    group_by(Ordered_date_yearweek) %>%
    group_by(pharmActive_sinceBeginning, pharmAverageOrdersMonth, add = TRUE) %>%
    summarise(demandWeek_skuPrefix = sum(demand))
  
  lm_fit <- lm(data = data_train_agg, demandWeek_skuPrefix ~ pharmActive_sinceBeginning + pharmAverageOrdersMonth)
  coefs <- coefficients(lm_fit)
  
  # 2) 'demandTotal_skuPrefix' and 'demandTotal_skuCode' are necessary for disaggregating the computed
  # linear trend down to the product level.
  demandTotal_skuPrefix <- data_skuPrefix %>%
    filter(label == "train") %>%
    summarise(demandTotal_skuPrefix = sum(demand))
  
  demandTotal_skuPrefix <- fastDoCall("c", demandTotal_skuPrefix)
  
  data_skuPrefix_toAdd <- data_skuPrefix %>%
    filter(label == "train") %>%
    group_by(sku_code) %>%
    summarise(demandTotal_skuCode = sum(demand))
  
  data_skuPrefix_mod <- left_join(data_skuPrefix, data_skuPrefix_toAdd, by = "sku_code")
  
  # 3) Detrending demand: The feature 'trend' has to be kept, so we are able to re-transform the predictions later on
  # by simply adding the trend again. 
  data_skuPrefix_mod <- data_skuPrefix_mod %>%
    mutate(trend = (coefs[1] + 
                      coefs[2] * pharmActive_sinceBeginning + 
                      coefs[3] * pharmAverageOrdersMonth) * 
             demandTotal_skuCode / demandTotal_skuPrefix) %>%
    mutate(demand = demand - trend) 
  
  return(data_skuPrefix_mod)
}

mClinicaML_centering <- function(data,
                                 model,
                                 features = NULL,
                                 n.ahead = 4,
                                 updateForecasts = TRUE) {
  
  data <- data %>%
    mutate(actuals = demand)
  
  # For now we don't want to use the feature molecule.
  if("molecule" %in% colnames(data)) {
    data <- data %>%
      select(-molecule)
  }
  
  # Train/Test-Split according to n.ahead
  maxWeek <- max(data$Ordered_date_yearweek)
  
  #---
  
  data <- data %>%
    mutate(label = ifelse(Ordered_date_yearweek <= maxWeek - n.ahead, "train", "test"))
  
  dataGroups <- data %>%
    group_by(sku_code_prefix) %>%
    group_split()
  
  dataGroups_mod <- lapply(dataGroups, function(group) {
    group <- setorderv(group, cols = c("Ordered_date_yearweek", "sku_code"))
    group_centered <- centerData(group)
    
    # Centering of Price Data 
    trainObs <- group_centered %>%
      filter(label == "train")
    
    scalePrice <- mean(trainObs$pricePerSingleUnit)
    
    group_mod <- group_centered %>%
      mutate_at(vars(matches("meanPrice|medianPrice|maxPrice|minPrice|pricePer")),
                .funs = ~ . / scalePrice)
    
    return(group_mod)
  })
  data <- rbindlist(dataGroups_mod)
  
  # As the demand has been transformed we also have to transform the lag-demand-features!
  data <- recalcDemandFeatures(data)
  
  # We don't want to use sku_code and sku_code_prefix as features of our model, but we need both in later steps
  # to aggregate the predictions.
  dataTrain <- data %>%
    filter(label == "train") %>%
    select(-c(sku_code, 
              sku_code_prefix,
              Ordered_date_yearweek,
              unit_selling_price,
              mean,
              sd,
              label,
              actuals,
              demandMin))
  
  if(!is.null(features)) {
    dataTrain <- dataTrain %>%
      select(c(features, "demand"))
  }
  
  # Tuning the model-parameters using caret's internal cross-validation functionality
  fitControl <- trainControl(method = "cv",
                             number = 5)
  
  # Regression model
  fit <- train(demand ~., 
               data = dataTrain, 
               method = model, 
               trControl = fitControl)
  
  if(updateForecasts) {
    
    preds_centered <- getRealPreds(fit = fit,
                                   data = data,
                                   n.ahead = n.ahead)$preds
  } else {
    preds_centered <- predict(fit, newdata = data)
  }
  
  dataAll <- data %>%
    add_column(preds_centered) %>%
    mutate(actuals_centered = demand,
           preds = (preds_centered + demandMin) * sd + mean)
  
  dataAll$preds[dataAll$preds < 0] <- 0
  
  dataAll %>%
    mutate(erros = actuals - preds)
  
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
              "fit" = fit))
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
    demand <- dataGroups[[i]]$demand
    
    otherGroups <- dataGroups[-i]
    
    acfs <- sapply(otherGroups, function(group) {
      ccf(demand, group$demand, lag.max = 1, plot = FALSE)$acf[1]
    })
    names(acfs) <- keys$sku_code_prefix[-i]
    acfs <- sort(acfs)
    minPrefixes <- names(acfs[1:3])
    maxPrefixes <- names(tail(acfs, 3))
    
    return(list("minCorrs" = minPrefixes,
                "maxCorrs" = maxPrefixes))
  })
  
  names(corrs) <- keys$sku_code_prefix
  return(corrs)
}

mClinicaML_noTransformations <- function(data,
                                         model,
                                         method_reg = "cv",
                                         number_reg = 5,
                                         model_binary = NULL,
                                         method_class = "cv",
                                         number_class = 5,
                                         features = NULL,
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
  
  #---
  
  # We don't want to use sku_code and sku_code_prefix as features of our model, but we need both in later steps
  # to aggregate the predictions.
  dataTrain <- data %>%
    filter(label == "train") %>%
    select(-c(sku_code, 
              sku_code_prefix,
              Ordered_date_yearweek,
              unit_selling_price,
              label,
              actuals_binary,
              actuals))
  
  if(!is.null(features)) {
    dataTrain <- dataTrain %>%
      select(c(features, "demand"))
  }
  
  customSummary <- function(data, lev = NULL, model = NULL) {
    out <- rmse_scaled(actuals = data$obs, preds = data$pred)
    names(out) <- "CUSTOM"
    return(out)
  }
  # Tuning the model-parameters using caret's internal cross-validation functionality
  fitControl_reg <- trainControl(method = method_reg,
                                 number = number_reg,
                                 summaryFunction = customSummary)
  
  
  # Regression model
  fit <- train(demand ~., 
               data = dataTrain, 
               method = model, 
               trControl = fitControl_reg,
               metric = "CUSTOM",
               maximize = FALSE)
  
  if(!is.null(model_binary)) {
    
    # Binary model to find 0-demands
    dataTrain <- data %>%
      filter(label == "train") %>%
      select(-c(sku_code, 
                sku_code_prefix,
                Ordered_date_yearweek,
                unit_selling_price,
                label,
                demand,
                actuals))
    
    fitControl_class <- trainControl(method = method_class,
                                     number = number_class)
    
    fit_binary <- train(actuals_binary ~.,
                        data = dataTrain,
                        method = model_binary,
                        trControl = fitControl_class,
                        metric = "Kappa")
  } else {
    fit_binary <- NULL
  }
  
  if(updateForecasts) {
    preds <- getUpdatedPreds(fit = fit, 
                             fit_binary = fit_binary, 
                             data = data, 
                             n.ahead = n.ahead)$preds
  } else {
    preds <- predict(fit, newdata = data)
    
    if(!is.null(model_binary)) {
      preds_binary <- predict(fit_binary, newdata = data)
      
      preds[!as.logical(preds_binary)] <- 0
    }
  }
  
  dataAll <- data %>%
    add_column(preds) %>%
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
              "fit" = fit))
}

#---