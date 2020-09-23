
###### Import Data #####

library(doParallel)
library(foreach)
library(tidyverse)
library(forecast)
library(Gmisc)
library(data.table)

transactions <- readRDS("SavedData/transactions_clean.rds")
data <- readRDS("SavedData/data_noDistr_extrapolated.rds")
importantSkus <- readRDS("SavedData/importantSkus.rds")
model <- readRDS("SavedData/xgbTree_impSkus_combined_updated_noDetrend_noCorrCols_smapeMetric.rds")

data_Train <- data %>%
  filter(sku_code_prefix %in% importantSkus)

dataPred <- model$dataPred_aggSkuPrefix

# If we want to use monthly data we have to apply the following code to our prediction results 'dataPred':
dataPred <- dataPred %>%
  getMonthFeature() %>%
  group_by(sku_code_prefix, month) %>%
  summarise(actuals = sum(actuals),
            preds = sum(preds))

# Accuracy
acc <- getAccuracyPerSku(dataPred, errorMeasures = c("rmse", "rmse_scaled" ,"mae", "mae_scaled", "smape"))

acc %>%
  filter(mae_scaled < Inf) %>%
  summary()

#---

##### Get Revenues #####

revenueData <- transactions %>%
  filter(Ordered_date_yearweek >= min(data_Train$Ordered_date_yearweek)) %>%
  group_by(sku_code_prefix) %>%
  summarise(revenue = sum(quantity * unit_selling_price * (1 - unit_discount) * (1 - promoDiscount))) %>%
  arrange(desc(revenue)) %>%
  group_by() %>%
  mutate(revenueShare = revenue / sum(revenue)) %>%
  select(sku_code_prefix, everything()) %>%
  as.data.frame()

accRevenueData <- left_join(acc, revenueData, by = "sku_code_prefix")

nameData <- readRDS("SavedData/productNames.rds")

accRevenueData <- accRevenueData %>%
  left_join(nameData, by = "sku_code_prefix") %>%
  arrange(desc(revenue)) %>%
  select(name, everything())

#---

###### Revenue Plots ######

revenueBarPlot <- function(accRevenueData,
                           intervalSize = 0.1,
                           cutOff = 2) {
  
  inverseSize <- 1 / intervalSize
  
  accRevenueData_plot <- accRevenueData %>%
    filter(mae_scaled <= cutOff) %>%
    mutate(floor = ifelse(mae_scaled == 0, 0, floor((mae_scaled - .Machine$double.eps) * inverseSize) / inverseSize)) %>%
    mutate(interval = paste0("(", floor, ",", floor + intervalSize, "]")) %>%
    group_by(interval) %>%
    summarise(revenueShare = sum(revenueShare),
              skuCount = n(),
              maxError = max(mae_scaled))
  
  accRevenueData_plot_toAdd <- accRevenueData %>%
    filter(mae_scaled > cutOff) %>%
    mutate(interval = paste0("(", cutOff,", Inf, ]")) %>%
    group_by(interval) %>%
    summarise(revenueShare = sum(revenueShare),
              skuCount = n(),
              maxError = max(mae_scaled))
  
  accRevenueData_plot <- rbind(accRevenueData_plot, accRevenueData_plot_toAdd)
  
  ggplot(accRevenueData_plot, aes(x = interval, y = 100 * revenueShare)) +
    geom_bar(stat = "identity", fill = "steelblue", alpha = 0.9) + 
    geom_text(aes(label = skuCount), position = position_dodge(width = 0.9), vjust = -0.25, size = 3.4) + 
    labs(x = "Intervals of mean absolute percentage error", 
         y = "% of total revenue", 
         title = paste("Distribution of revenue among groups of similar accuracy")) + 
    theme_bw() +
    theme(axis.text.x = element_text(angle = 90, vjust = 0.8)) + 
    theme(plot.title = element_text(size=15))
}

revenueBarPlot(accRevenueData = accRevenueData,
               intervalSize = 0.25,
               cutOff = 1)

#---

revenueBarPlot_withSkuShare <- function(accRevenueData,
                                        intervalSize,
                                        cutOff,
                                        transactions) {
  
  inverseSize <- 1 / intervalSize
  
  accRevenueData_grouped <- accRevenueData %>%
    filter(mae_scaled <= cutOff) %>%
    mutate(lower = ifelse(mae_scaled == 0, 0, ceiling(mae_scaled * inverseSize) / inverseSize - intervalSize)) %>%
    mutate(interval = paste0("(", lower, ",", lower + intervalSize, "]")) %>%
    group_by(interval) %>%
    summarise(revenueShare = sum(revenueShare),
              skuCount = n())
  
  accRevenueData_grouped_toAdd <- accRevenueData %>%
    filter(mae_scaled > cutOff) %>%
    mutate(interval = paste0("(", cutOff, ",", Inf, "]")) %>%
    group_by(interval) %>%
    summarise(revenueShare = sum(revenueShare),
              skuCount = n())
  
  accRevenueData_grouped <- rbind(accRevenueData_grouped, accRevenueData_grouped_toAdd)
  
  accRevenueData_grouped <- accRevenueData_grouped %>%
    group_by() %>%
    mutate(skuShare = skuCount / uniqueN(transactions$sku_code_prefix))
  
  accRevenueData_plot <- accRevenueData_grouped %>%
    select(interval, revenueShare, skuShare) %>%
    gather(key = "Color", value = "value", -interval)
  
  accRevenueData_plot <- left_join(accRevenueData_plot, 
                                   accRevenueData_grouped[, c("interval", "skuCount")], 
                                   by = "interval")
  
  ggplot(accRevenueData_plot, aes(x = interval, y = 100 * value, fill = Color)) +
    geom_bar(stat = "identity", position = position_dodge(), alpha = 0.9) + 
    geom_text(aes(label = skuCount), position = position_dodge(width = 0.9), vjust = -0.25, size = 3.4) + 
    labs(x = "Intervals of mean absolute percentage error", 
         y = "% of revenue / skus", 
         title = paste("Distribution of revenue among groups of similar accuracy")) + 
    theme_bw() +
    theme(axis.text.x = element_text(angle = 90, vjust = 0.8)) + 
    theme(plot.title = element_text(size=15))
}

revenueBarPlot_withSkuShare (accRevenueData = accRevenueData, 
                             intervalSize = 0.25, 
                             cutOff = 1, 
                             transactions = transactions)

#---

revenueLinePlot <- function(accRevenueData,
                            cutOff = 1,
                            transactions) {
  
  accRevenueData_plot <- accRevenueData %>%
    filter(mae_scaled <= cutOff) %>%
    arrange(mae_scaled) %>%
    mutate(revenue = cumsum(revenueShare)) %>%
    mutate(index = 1,
           NumberOfSkus = cumsum(index) / uniqueN(transactions$sku_code_prefix))
  
  intersection <- accRevenueData_plot$revenue[tail(which(accRevenueData_plot$mae_scaled <= 0.5), 1)]
  
  accRevenueData_plot <- accRevenueData_plot %>%
    select(mae_scaled, revenue, NumberOfSkus) %>%
    gather(key = "Color", value = "value", -mae_scaled)
  
  ggplot(accRevenueData_plot, aes(x = mae_scaled, y = value, color = Color)) +
    geom_line(size = 1) + 
    geom_vline(xintercept = 0.5, linetype = "dashed") + 
    geom_hline(yintercept = intersection, linetype = "dashed") + 
    labs(x = "Mean absolute percentage error", 
         y = "Cumulative %", 
         title = paste("Distribution of Revenue among groups of similar accuracy")) + 
    theme_bw() +
    theme(axis.text.x = element_text(angle = 90, vjust = 0.8)) + 
    theme(plot.title = element_text(size=15))
  
}

revenueLinePlot(accRevenueData = accRevenueData,
                cutOff = 2,
                transactions = transactions)

#---

# Scatterplot Revenue

revenueScatterplot <- function(accRevenueData,
                               cutOffError = 0.5,
                               cutOffRevenue = 0.001,
                               maxError = 2) {
  
  accRevenueData_plot <- accRevenueData %>%
    mutate(errorCategory = ifelse(mae_scaled <= cutOffError, "lowError", "highError"),
           revenueCategory = ifelse(revenueShare >= cutOffRevenue, "highRevenue", "lowRevenue")) %>%
    mutate(category = paste0(errorCategory, ", ", revenueCategory)) %>%
    filter(mae_scaled <= maxError) %>%
    mutate(mae_scaled = 1 - mae_scaled)
  
  ggplot(accRevenueData_plot, aes(x = mae_scaled, y = revenueShare, color = category)) +
    scale_y_continuous(trans = 'log10') + 
    geom_point(shape = 16, size = 1.5) +
    theme(axis.title.x=element_blank(),
          axis.text.x=element_blank(),
          axis.ticks.x=element_blank(),
          axis.title.y=element_blank(),
          axis.text.y=element_blank(),
          axis.ticks.y=element_blank())
}

revenueScatterplot(accRevenueData = accRevenueData,
                   cutOffError = 0.5,
                   cutOffRevenue = 0.001,
                   maxError = 2)

#---

##### Get Stock-Outs ####

comments_stockOut <- grepl(transactions$comment, pattern = "STOCK|STCOK")
itemComment_stockOut <- grepl(transactions$item_comment, pattern = "STOCK")
stockOut_index <- comments_stockOut | itemComment_stockOut

stockOut <- rep(FALSE, nrow(transactions))
stockOut[stockOut_index] <- TRUE
transactions <- add_column(transactions, stockOut)

stockOutData_sku <- transactions %>%
  group_by(sku_code_prefix, sku_code) %>%
  summarise(stockOuts_sku = sum(stockOut)) %>%
  group_by() %>%
  mutate(stockOutsShare_sku = stockOuts_sku / sum(stockOuts_sku)) %>%
  group_by(sku_code_prefix) %>%
  mutate(stockOutsShare = sum(stockOutsShare_sku)) %>%
  arrange(desc(stockOutsShare)) %>%
  as.data.frame()

stockOutData <- stockOutData_sku %>%
  group_by(sku_code_prefix) %>%
  slice(1) %>%
  select(sku_code_prefix, stockOutsShare) %>%
  arrange(desc(stockOutsShare)) %>%
  select(sku_code_prefix, everything()) %>%
  as.data.frame()

accStockData <- left_join(acc, stockOutData, by = "sku_code_prefix")

#---

###### Stockout Plots #####

stockOutBarPlot <- function(accStockData,
                            intervalSize = 1,
                            cutOff = 1) {
  
  inverseSize <- 1 / intervalSize
  
  accStockData_plot <- accStockData %>%
    filter(mae_scaled <= cutOff) %>%
    mutate(floor = ifelse(mae_scaled == 0, 0, floor((mae_scaled - .Machine$double.eps) * inverseSize) / inverseSize)) %>%
    mutate(interval = paste0("(", floor, ",", floor + intervalSize, "]")) %>%
    group_by(interval) %>%
    summarise(stockOutsShare = sum(stockOutsShare),
              countSkus = n())
  
  accStockData_plot_toAdd <- accStockData %>%
    filter(mae_scaled > cutOff) %>%
    mutate(interval = paste0("(", cutOff, ", Inf]")) %>%
    group_by(interval) %>%
    summarise(stockOutsShare = sum(stockOutsShare),
              countSkus = n())
  
  accStockData_plot <- rbind(accStockData_plot, accStockData_plot_toAdd)
  
  ggplot(accStockData_plot, aes(x = interval, y = stockOutsShare)) +
    geom_bar(stat = "identity", fill = "steelblue") + 
    geom_text(aes(label = countSkus), position = position_dodge(width = 0.9), vjust = -0.25, size = 3.4) + 
    labs(x = "Intervals of mean absolut percentage error", 
         y = "% of orders with stock out", 
         title = paste("Distribution of stock-outs among groups of similar accuracy")) + 
    theme_bw() +
    theme(axis.text.x = element_text(angle = 90, vjust = 0.8)) + 
    theme(plot.title = element_text(size=15))
  
}

stockOutBarPlot(accStockData = accStockData, 
                intervalSize = 0.1,
                cutOff = 2)

#---

stockOutBarPlot_withSkuShare <- function(accStockData,
                                         intervalSize = 0.25,
                                         cutOff = 1,
                                         transactions) {
  
  inverseSize <- 1 / intervalSize
  
  accStockData_grouped <- accStockData %>%
    filter(mae_scaled <= cutOff) %>%
    mutate(lower = ifelse(mae_scaled == 0, 0, ceiling(mae_scaled * inverseSize) / inverseSize - intervalSize)) %>%
    mutate(interval = paste0("(", lower, ",", lower + intervalSize, "]")) %>%
    group_by(interval) %>%
    summarise(.stockOutShare = sum(stockOutsShare),
              skuCount = n())
  
  accStockData_grouped_toAdd <- accStockData %>%
    filter(mae_scaled > cutOff) %>%
    mutate(interval = paste0("(", cutOff, ",", Inf, "]")) %>%
    group_by(interval) %>%
    summarise(.stockOutShare = sum(stockOutsShare),
              skuCount = n())
  
  accStockData_grouped <- rbind(accStockData_grouped, accStockData_grouped_toAdd)
  
  accStockData_grouped <- accStockData_grouped %>%
    group_by() %>%
    mutate(skuShare = skuCount / uniqueN(transactions$sku_code_prefix))
  
  accStockData_plot <- accStockData_grouped %>%
    select(interval, .stockOutShare, skuShare) %>%
    gather(key = "Color", value = "value", -interval)
  
  accStockData_plot <- left_join(accStockData_plot, 
                                 accStockData_grouped[, c("interval", "skuCount")], 
                                 by = "interval")
  
  ggplot(accStockData_plot, aes(x = interval, y = 100 * value, fill = Color)) +
    geom_bar(stat = "identity", position = position_dodge(), alpha = 0.9) + 
    geom_text(aes(label = skuCount), position = position_dodge(width = 0.9), vjust = -0.25, size = 3.4) + 
    labs(x = "Intervals of mean absolute percentage error", 
         y = "% of stock-outs / skus", 
         title = paste("Distribution of StockOuts among groups of similar accuracy")) + 
    theme_bw() +
    theme(axis.text.x = element_text(angle = 90, vjust = 0.8)) + 
    theme(plot.title = element_text(size=15))
  
}

stockOutBarPlot_withSkuShare(accStockData = accStockData,
                             cutOff = 1,
                             intervalSize = 0.25,
                             transactions)
#---

##### Get Order-Frequency #####

freqData <- transactions %>%
  filter(Ordered_date_yearweek >= min(data_Train$Ordered_date_yearweek) 
         & Ordered_date_yearweek <= max(data_Train$Ordered_date_yearweek)) %>%
  group_by(sku_code_prefix) %>%
  summarise(numbOfOrders = n()) %>%
  arrange(desc(numbOfOrders))

accFreqData <- left_join(acc, freqData, by = "sku_code_prefix")

#---

##### Frequency Plots #####

frequencyBarPlot <- function(accFreqData, 
                             maxOrders = 1000,
                             intervalSize = 50) {
  
  inverseSize <- 1 / intervalSize
  
  accFreqData_plot <- accFreqData %>%
    filter(mae_scaled < Inf) %>%
    arrange(numbOfOrders) %>%
    filter(numbOfOrders <= 500) %>%
    mutate(ceiling = ceiling((numbOfOrders - .Machine$double.eps) / intervalSize) * intervalSize) %>%
    mutate(interval = paste0("(", ceiling, ",", ceiling + intervalSize, "]")) %>%
    group_by(ceiling) %>%
    summarise(avgError = mean(mae_scaled),
              countSkus = n())
  
  ggplot(accFreqData_plot, aes(x = ceiling, y = avgError)) +
    geom_bar(stat = "identity", fill = "steelblue") + 
    geom_text(aes(label = countSkus), position = position_dodge(width = 0.9), vjust = -0.15, size = 3.4) + 
    labs(x = "Number of Orders per SKU", 
         y = "Average Error", 
         title = paste("Relationship of errors and order-frequency")) + 
    theme_bw()
  
}

frequencyBarPlot(accFreqData = accFreqData,
                 maxOrders = 1000,
                 intervalSize = 50)

#---

##### Metrics Weekly ########

data <- readRDS("SavedData/data_noDistr_extrapolated.rds")
# model <- readRDS("SavedData/xgbTree_impSkus_combined_updated_noDetrend_noCorrCols_rmseMetric.rds")
dataPred <- model$dataPred_aggSkuPrefix

importantSkus <- readRDS("SavedData/importantSkus.rds")
data <- data %>%
  filter(sku_code_prefix %in% importantSkus[1:20])

errorMatrix <- getErrorSample_bt(data = data, 
                                 modelResults = model,
                                 n.ahead = 8,
                                 numbIterations = 2,
                                 monthly = TRUE)

dataMetrics <- getMetrics_weekly(dataPred,
                                 errorMatrix,
                                 lowerProb = 0.05,
                                 upperProb = 0.95,
                                 stockLevel = 0.95,
                                 commitmentLevel = 0.95)

dataMetrics %>%
  group_by(sku_code_prefix) %>%
  mutate(dist = (upper - lower) / mean(preds)) %>%
  slice(1) %>%
  select(sku_code_prefix, dist) %>%
  filter(dist < Inf) %>%
  summary()

sum(dataMetrics$checkCI) / nrow(dataMetrics)
sum(dataMetrics$lowerOriginal < 0) / nrow(dataMetrics)
sum(dataMetrics$lowerOriginal < 0 & dataMetrics$preds > 500) / sum(dataMetrics$lowerOriginal < 0)
sum(dataMetrics$lowerOriginal < 0 & dataMetrics$actuals > 100) / sum(dataMetrics$actuals > 100)

topRevenueSku <- revenueData$sku_code_prefix[1]
dataPredCI %>%
  filter(sku_code_prefix == topRevenueSku)

plotPreds(model$dataAll_aggSkuPrefix,
          topRevenueSku, 
          dataPredCI = dataPredCI)

errorMatrix %>%
  filter(sku_code_prefix == topRevenueSku) %>%
  summary(.$errors)

errors <- errorMatrix$errors[errorMatrix$sku_code_prefix == topRevenueSku]

fit.norm <- fitdistrplus::fitdist(errors, "norm")
fit.norm
mean(errors)
sd(errors)
1.25 * (sum(abs(errors)) / length(errors))

failedPreds <- dataPredCI %>%
  filter(!checkCI) %>%
  select(Ordered_date_yearweek, actuals, preds, lower, upper, errors) 

sum(failedPreds$errors < 0) / nrow(failedPreds)

#---

dataPred <- model$dataPred_aggSkuPrefix
errorMatrix <- readRDS("SavedData/errorsCV_50iter_4weeks.rds")
errorMatrix_bt <- readRDS("SavedData/errorMatrixBT_12iter_4weeks.rds")

errorMatrix <- lapply(errorMatrix_bt, function(errors) {
  c(t(errors[1:12, ]))
})

dataPred <- getCI(dataPred,
                  errorMatrix_bt,
                  lower = 0.01,
                  upper = 0.99)

sum(dataPred$checkCI) / nrow(dataPred)

topRevenueSku <- payData$sku_code_prefix[2]
dataPred %>%
  filter(sku_code_prefix == topRevenueSku)

summary(errorMatrix[[topRevenueSku]])

failedPreds <- dataPred %>%
  filter(!checkCI) %>%
  select(Ordered_date_yearweek, actuals, preds, lower, upper, errors) 

sum(failedPreds$errors < 0) /nrow(failedPreds)


#---

dataPred <- model$dataPred_aggSkuPrefix
errorMatrix <- readRDS("SavedData/errorsCV_50iter_4weeks.rds")
errorMatrix_bt <- readRDS("SavedData/errorMatrixBT_12iter_4weeks.rds")

errorMatrix <- lapply(errorMatrix_bt, function(errors) {
  c(t(errors[1:12, ]))
})

dataPred <- getCI2(dataPred,
                   errorMatrix_bt,
                   lower = 0.01,
                   upper = 0.99)

sum(dataPred$checkCI) / nrow(dataPred)

topRevenueSku <- payData$sku_code_prefix[2]
dataPred %>%
  filter(sku_code_prefix == topRevenueSku)

summary(errorMatrix[[topRevenueSku]])

failedPreds <- dataPred %>%
  filter(!checkCI) %>%
  select(Ordered_date_yearweek, actuals, preds, lower, upper, errors) 
failedPreds

sum(failedPreds$errors < 0) / nrow(failedPreds)

#---

###### Metrics Monthly ######

model <- readRDS("SavedData/xgbTree_ImpSkus_combined_updated_detrend_noCorrCols_smape_extended12Weeks.rds")
data <- readRDS("SavedData/data_noDistr_extrapolated_extended.rds")
importantSkus <- readRDS("SavedData/importantSkus.rds")
data <- data %>%
  filter(sku_code_prefix %in% importantSkus)

dataPred <- model$dataPred_aggSkuPrefix
dataAll <- model$dataAll_aggSkuPrefix

#---

# Calculate monthly errorMatrix
errorMatrix <- getErrorSample_bt(data = data, 
                                 modelResults = model,
                                 n.ahead = 4,
                                 numbIter = 20)

summary(getAccuracyPerSku(errorMatrix))

# Calculate Monthly key metrics
dataAllMonth <- aggregateMonth(dataAll)
dataPredMonth <- aggregateMonth(dataPred)

stockOutDemandMonth <- stockOutDemand %>%
  group_by(month) %>%
  filter(sku_code_prefix == "103141") %>%
  summarise(demandStockOut = sum(demandStockOut)) %>%
  as.data.frame()

dataAllMonth_103141 <- dataAllMonth %>%
  filter(sku_code_prefix == "103141") %>%
  group_by() %>%
  mutate(month = month + 10) %>%
  left_join(stockOutDemandMonth, by = "month") %>%
  group_by() %>%
  mutate(demandSold = actuals - demandStockOut,
         month = month - 10)

dataAllMonth_103141$demandSold[17:18] <- 67850
dataAllMonth_103141$demandStockOut[17:18] <- 0

dataAllMonth_103141 <- dataAllMonth_103141 %>%
  mutate(demand = actuals) %>%
  select(sku_code_prefix, month, demand, demandStockOut, demandSold)

write.csv2(dataAllMonth_103141, "SavedData/stockData_103141.csv")

dataAllMonth <- dataAllMonth[dataAllMonth$month %in% 1:12, ]

dataPredCI <- getMetrics_month(dataAllMonth,
                               errorMatrix,
                               lowerProb = 0.05,
                               upperProb = 0.95,
                               stockLevel = 0.9,
                               commitmentLevel = 0.9)

dataPredCI %>%
  group_by(sku_code_prefix) %>%
  mutate(dist = (upper - lower) / mean(preds)) %>%
  group_by() %>%
  select(dist) %>%
  summary()

months <- sort(uniqueN(dataPredCI$month))
dataPredCI_fill <- dataPredCI %>%
  filter(month %in% tail(months, 3))

sum(dataPredCI_fill$checkCI) / nrow(dataPredCI_fill)
sum(dataPredCI$lowerOriginal < 0) / nrow(dataPredCI)
sum(dataPredCI$lowerOriginal < 0 & dataPredCI$preds > 500) / sum(dataPredCI$lowerOriginal < 0)
sum(dataPredCI$lowerOriginal < 0 & dataPredCI$actuals > 100) / sum(dataPredCI$actuals > 100)

revenueData <- readRDS("SavedData/revenueData.rds")
topRevenueSkus <- revenueData$sku_code_prefix[1:200]

dataPredTopRevenue <- dataPredCI %>%
  filter(sku_code_prefix == topRevenueSkus[1]) %>%
  as.data.frame()

dataPredTopRevenue

mean(dataPredTopRevenue$fillRate[16:18])

write.csv2(dataPredTopRevenue, "SavedData/keyMetricsData_3months_103141.csv")

errorMatrix %>%
  mutate(mae_scaled = abs(errors) / actuals) %>%
  filter(mae_scaled < Inf) %>%
  summary()

#---

# Data for one SKU

levels <- seq(from = 0.5, to = 0.9, by = 0.1)

metricsData <- lapply(levels, function(level) {
  dataAllMetrics <- getCI4(dataAllMonth,
                           errorMatrix,
                           lowerProb = 0.05,
                           upperProb = 0.95,
                           stockLevel = level,
                           commitmentLevel = level)
  
  dataAllMetrics <- data.frame(dataAllMetrics, "serviceLevel" = level)
  return(dataAllMetrics)
})
metricsData <- rbindlist(metricsData)

metricsDataTopRevenue <- metricsData %>%
  filter(sku_code_prefix == topRevenueSkus[1])

write.csv2(metricsDataTopRevenue, "SavedData/keyMetricsData_3months_103141.csv")

testMonths <- tail(metricsDataTopRevenue$month, 3)

stock90 <- metricsDataTopRevenue %>%
  filter(serviceLevel == 0.9) %>%
  filter(month %in% testMonths) %>%
  select(stock) %>%
  fastDoCall("c", .) %>%
  unname(TRUE)

stockPerDistr_list <- lapply(seq_along(testMonths), function(i) {
  stockPerDistr <- distrHistory[[topRevenueSkus[1]]] %>%
    mutate(commitmentOfDistr = orderShare * stock90[i])
  
  stockPerDistr <- data.frame(stockPerDistr, 
                              "stockAgg" = stock90[i],
                              "month" = testMonths[i])
  return(stockPerDistr)
})
stockPerDistr <- rbindlist(stockPerDistr_list)

write.csv2(stockPerDistr, "SavedData/stockLevelDistr_103141.csv")

#---

##### Error Samples #####

# IMPORTANT: Even if 'monthly == TRUE', n.ahead equals the amount of future weeks we are predicting at each iteration.
# For that reason n.ahead has to be multiple of 4, because we always interprete bundles of 4 weeks as one month.

# IMPORTANT FUTURE EXTENSION: We should calculate the prediction intervals seperately for each forecast-horizon.
# In order for that to be possible we have to change the way we store the errors in the object 'errorMatrix'.
# The easiest solution most likely is to simply add another column showing the forecast-horizon of each error.

getErrorSample_bt <- function(data,
                              modelResults,
                              n.ahead = 4,
                              numbIterations = 12,
                              monthly = FALSE) {
  
  if(monthly) {
    forecastHorizonCheck <- ceiling(n.ahead / 4) == n.ahead / 4
    
    if(!forecastHorizonCheck) {
      stop("n.ahead has to be multiple of 4 if we want to calculate a sample of monthly errors.")
    }
  }
  
  # We remove all weeks that we used as test-weeks during the original modelling process.
  data <- data %>%
    filter(Ordered_date_yearweek <= max(Ordered_date_yearweek) - modelResults$n.ahead)
  
  model_reg <- modelResults$model_reg
  model_binary <- modelResults$model_binary
  useDetrending <- modelResults$useDetrending
  useCorrFeatures <- modelResults$useCorrFeatures
  tuneGrid_reg <- modelResults$fit_reg$finalModel$tuneValue
  tuneGrid_class <- modelResults$fit_class$finalModel$tuneValue
  
  fitControl <- trainControl(method = "none")
  
  set.seed(4)
  registerDoParallel()
  
  # results <- foreach(i = (numbIteratins - 1), .packages = c("caret", "tidyverse")) %dopar% {
  results <- lapply(0:(numbIterations - 1), function(i) {
    
    weeksTrain <- seq.int(from = min(data$Ordered_date_yearweek), 
                          to = max(data$Ordered_date_yearweek) - n.ahead - i,
                          by = 1)
    
    weeksTest <- seq.int(from = max(data$Ordered_date_yearweek) - (n.ahead - 1) - i, 
                         to = max(data$Ordered_date_yearweek) - i,
                         by = 1)
    
    dataAll <- data %>%
      mutate(label = ifelse(Ordered_date_yearweek %in% weeksTrain, "train", "test")) %>%
      filter(Ordered_date_yearweek <= max(weeksTest)) 
    
    preprocResults <- preprocessData(data = dataAll, 
                                     useCorrFeatures = useCorrFeatures, 
                                     useDetrending = useDetrending)
    
    dataAll <- preprocResults$dataPreproc
    dataTrain_reg <- preprocResults$dataTrain_reg
    dataTrain_class <- preprocResults$dataTrain_class
    corrData <- preprocResults$corrData
    
    if(monthly) {
      dataAll <- getMonthFeature(dataAll)
    }
    
    fit_reg <- train(demand ~., 
                     data = dataTrain_reg, 
                     method = model_reg, 
                     trControl = fitControl,
                     tuneGrid = tuneGrid_reg)
    
    fit_class <- train(actuals_binary ~.,
                       data = dataTrain_class,
                       method = model_binary,
                       trControl = fitControl,
                       tuneGrid = tuneGrid_class)
    
    preds_transformed <- getUpdatedPreds(fit_reg = fit_reg,
                                         fit_class = fit_class,
                                         data = dataAll,
                                         n.ahead = n.ahead,
                                         corrData = corrData)$preds
    
    dataTest <- dataAll %>%
      add_column(preds_transformed) %>%
      filter(label == "test") %>%
      mutate(preds = (preds_transformed * sd2 + demandMin) * sd1 + mean)
    
    if(useDetrending) {
      dataTest <- dataTest %>%
        mutate(preds = preds * detrendFactor)
    }
    
    dataTest$preds[dataTest$preds < 0] <- 0
    
    if(!monthly) {
      dataTest <- dataTest %>%
        group_by(sku_code_prefix, Ordered_date_yearweek) %>%
        summarise(actuals = sum(actuals),
                  preds = sum(preds),
                  errors = actuals - preds) %>%
        as.data.frame()
      
    } else {
      dataTest <- dataTest %>%
        group_by(sku_code_prefix, month) %>%
        summarise(actuals = sum(actuals),
                  preds = sum(preds),
                  errors = actuals - preds,
                  lastWeekMonth = max(Ordered_date_yearweek)) %>%
        as.data.frame()
    }
    
    return(dataTest)
  })
  
  errorMatrix <- rbindlist(results)
  errorMatrix <-  errorMatrix %>%
    arrange_at(vars(sku_code_prefix, matches("week|Week"))) %>%
    as.data.frame()
  
  return(errorMatrix)
}

#---

getErrorSample_cv <- function(data,
                              modelResults,
                              numbWeeksTest,
                              numbIterations = 12,
                              earliestWeek = 30) {
  
  # We remove all weeks that we used as test-weeks during the original modelling process.
  data <- data %>%
    filter(Ordered_date_yearweek <= max(Ordered_date_yearweek) - modelResults$n.ahead)
  
  model_reg <- modelResults$model_reg
  model_binary <- modelResults$model_binary
  useDetrending <- modelResults$useDetrending
  useCorrFeatures <- modelResults$useCorrFeatures
  tuneGrid_reg <- modelResults$fit_reg$finalModel$tuneValue
  tuneGrid_class <- modelResults$fit_class$finalModel$tuneValue
  
  fitControl <- trainControl(method = "none")
  
  set.seed(4)
  registerDoParallel()
  
  # results <- foreach(i = 1:numbIterations, .packages = c("caret", "tidyverse")) %dopar% {
  results <- lapply(1:numbIterations, function(i) {
    
    weeks <- tail(sort(unique(data$Ordered_date_yearweek), decreasing = FALSE), earliestWeek)
    weeksTest <- sample(x = weeks, size = numbWeeksTest, replace = FALSE)
    
    dataAll <- data %>%
      mutate(label = ifelse(Ordered_date_yearweek %in% weeksTest, "test", "train"))
    
    preprocResults <- preprocessData(data = dataAll, 
                                     useCorrFeatures = useCorrFeatures, 
                                     useDetrending = useDetrending)
    
    dataAll <- preprocResults$dataPreproc
    dataTrain_reg <- preprocResults$dataTrain_reg
    dataTrain_class <- preprocResults$dataTrain_class
    corrData <- preprocResults$corrData
    
    fit_reg <- train(demand ~., 
                     data = dataTrain_reg, 
                     method = model_reg, 
                     trControl = fitControl,
                     tuneGrid = tuneGrid_reg)
    
    fit_class <- train(actuals_binary ~.,
                       data = dataTrain_class,
                       method = model_binary,
                       trControl = fitControl,
                       tuneGrid = tuneGrid_class)
    
    dataTest <- dataAll %>%
      filter(label == "test")
    
    preds_reg <- predict(fit_reg, dataTest)
    preds_class <- predict(fit_class, dataTest)
    preds_reg[!as.logical(preds_class)] <- 0 
    
    dataTest <- dataTest %>%
      add_column(preds_reg) %>%
      mutate(preds = (preds_reg * sd2 + demandMin) * sd1 + mean)
    
    if(useDetrending) {
      dataTest <- dataTest %>%
        mutate(preds = preds * detrendFactor)
    }
    
    dataTest$preds[dataTest$preds < 0] <- 0
    
    dataTest <- dataTest %>%
      group_by(sku_code_prefix, Ordered_date_yearweek) %>%
      summarise(actuals = sum(actuals),
                preds = sum(preds),
                errors = actuals - preds) %>%
      as.data.frame()
    
    return(dataTest)
  })
  
  errorMatrix <- rbindlist(results)
  errorMatrix <-  errorMatrix %>%
    arrange_at(vars(sku_code_prefix, matches("week|Week"))) %>%
    as.data.frame()
  
  return(errorMatrix)
}

#---

###### Confidence Intervalls ######

getCI_noneParametric <- function(dataPred,
                                 errorMatrix,
                                 lowerProb = 0.025,
                                 upperProb = 0.975) {
  
  skuPrefixes <- unique(dataPred$sku_code_prefix)
  
  if(any(!skuPrefixes %in% unique(errorMatrix$sku_code_prefix))) {
    stop("All SKUs that are part of 'dataPred' have to be available in 'errorMatrix' as well")
  }
  
  ci <- lapply(skuPrefixes, function(skuPrefix) {
    
    errors <- errorMatrix %>%
      filter(sku_code_prefix == skuPrefix) %>%
      select(errors) %>%
      arrange(errors) %>%
      fastDoCall("c", .) %>%
      unname(TRUE)
    
    n <- length(errors)
    z <- qnorm(p = 0.975)
    
    m_lower <- ceiling(n * lowerProb)
    wurzel_lower <- sqrt(z^2 + 4 * m_lower / n * (1 - m_lower / n))
    
    lowerBound <- (2 * m_lower  + z^2 - z * wurzel_lower) / (2 * (n + z^2))
    lowerIndex <- ifelse(lowerBound * n < 1, 1, floor(lowerBound * n))
    lower <- errors[lowerIndex]
    
    
    m_upper <- ceiling(n * upperProb)
    wurzel_upper <- sqrt(z^2 + 4 * m_upper / n * (1 - m_upper / n))
    
    upperBound <- (2 * m_upper  + z^2 + z * wurzel_upper) / (2 * (n + z^2))
    upperIndex <- ceiling(upperBound * n)
    upper <- errors[upperIndex]
    
    # lower <- quantile(errors, type = 9, probs = lowerProb)
    # upper <- quantile(errors, type = 9, probs = upperProb)
    
    return(data.frame("sku_code_prefix" = skuPrefix, "lower" = lower, "upper" = upper,
                      stringsAsFactors = FALSE))
  })
  ci <- rbindlist(ci)
  
  dataPred <- dataPred %>% 
    left_join(ci, by = "sku_code_prefix") %>%
    mutate(lower = preds + lower,
           upper = preds + upper)
  
  dataPred <- dataPred %>%
    mutate(lowerOriginal = lower) %>%
    mutate(lower = ifelse(lower < 0, 0, lower)) %>%
    mutate(checkCI = actuals >= lower & actuals <= upper) %>%
    as.data.frame()
  
  return(dataPred)
}

#---

getMetrics_weekly <- function(dataPred,
                              errorMatrix,
                              lowerProb = 0.025,
                              upperProb = 0.975,
                              commitmentLevel = 0.95,
                              stockLevel = 0.95) {
  
  skuPrefixes <- unique(dataPred$sku_code_prefix)
  
  if(any(!skuPrefixes %in% unique(errorMatrix$sku_code_prefix))) {
    stop("All SKUs that are part of 'dataPred' have to be available in 'errorMatrix' as well")
  }
  
  ci <- lapply(skuPrefixes, function(skuPrefix) {
    
    errors <- errorMatrix %>%
      filter(sku_code_prefix == skuPrefix) %>%
      select(errors) %>%
      arrange(errors) %>%
      fastDoCall("c", .) %>%
      unname(TRUE)
    
    # mae <- mean(abs(errors)) 
    # lower <- mae * 1.25 * qnorm(lowerProb)
    # upper <- mae * 1.25 * qnorm(upperProb)
    
    fitNorm <- fitdistrplus::fitdist(errors, "norm")
    mean <- fitNorm$estimate[1]
    sd <- fitNorm$estimate[2]
    
    lower <- qnorm(lowerProb, mean = mean, sd = sd)
    upper <- qnorm(upperProb, mean = mean, sd = sd)
    
    stock <- qnorm(stockLevel, mean, sd)
    commitment <- qnorm(1 - commitmentLevel, mean, sd)
    
    return(data.frame("sku_code_prefix" = skuPrefix, 
                      "lower" = lower, 
                      "upper" = upper,
                      "stock" = stock,
                      "commitment" = commitment,
                      "maenError" = mean,
                      "sd" = sd,
                      stringsAsFactors = FALSE))
  })
  ci <- rbindlist(ci)
  
  dataPred <- dataPred %>% 
    left_join(ci, by = "sku_code_prefix") %>%
    mutate(lower = preds + lower,
           upper = preds + upper,
           stock = preds + stock,
           commitment = preds + commitment) %>%
    mutate(commitment = sum(commitment))
  
  dataPred <- dataPred %>%
    mutate(lowerOriginal = lower) %>%
    mutate(lower = ifelse(lower < 0, 0, lower)) %>%
    mutate(checkCI = actuals >= lower & actuals <= upper) %>%
    as.data.frame()
  
  return(dataPred)
}

#---

getMetrics_monthly <- function(dataPred,
                               errorMatrix,
                               lowerProb = 0.05,
                               upperProb = 0.95,
                               commitmentLevel = 0.90,
                               stockLevel = 0.90) {
  
  skuPrefixes <- unique(dataPred$sku_code_prefix)
  
  if(any(!skuPrefixes %in% unique(errorMatrix$sku_code_prefix))) {
    stop("All SKUs that are part of 'dataPred' have to be available in 'errorMatrix' as well")
  }
  
  dataPred_list <- lapply(skuPrefixes, function(skuPrefix) {
    
    dataSku <- dataPred %>%
      filter(sku_code_prefix == skuPrefix)
    
    numbMonthsTest <- uniqueN(dataSku$month[dataSku$label == "test"])
    monthsOfData <- dataSku$month
    
    errorMatrixSku <- errorMatrix %>%
      filter(sku_code_prefix == skuPrefix) 
    
    dataPred_list <- lapply(1:nrow(dataSku), function(i) {
      
      dataSinglePred <- dataSku[i, ]
      firstWeekCurrentMonth <- dataSinglePred$firstWeekMonth
      
      errors <- errorMatrixSku %>%
        filter(lastWeekMonth < firstWeekCurrentMonth) %>%
        select(errors) %>%
        fastDoCall("c", .) %>%
        unname(TRUE)
      
      if(length(errors) < 5) {
        metricsToAdd <- data.frame("sku_code_prefix" = skuPrefix, 
                                   "lowerBound" = 0, 
                                   "upperBound" = 0,
                                   "commitment" = 0,
                                   "stock" = 0,
                                   "fillRate" = 0,
                                   "meanError" = 0,
                                   "sd" = 0,
                                   stringsAsFactors = FALSE)
        
      } else {
        mean <- mean(errors)
        mae <- mean(abs(errors))
        sd <- 1.25 * mae
        
        lowerBound <- qnorm(lowerProb, mean, sd)
        upperBound <- qnorm(upperProb, mean, sd)
        
        stock <- qnorm(stockLevel, mean, sd)
        
        # NOTE: commitment is currently viewed on a single month-basis. It could for example make sense to compute 
        # this on a 3-months-aggregated basis, because we don't necessarily have to throw away none-sold items
        # after each month.
        commitment <- qnorm(1 - commitmentLevel, mean, sd)
        
        metricsToAdd <- data.frame("sku_code_prefix" = skuPrefix, 
                                   "lowerBound" = lowerBound, 
                                   "upperBound" = upperBound,
                                   "commitment" = commitment,
                                   "stock" = stock,
                                   "meanError" = mean,
                                   "sd" = sd,
                                   stringsAsFactors = FALSE)
        # Fill Rate
        stockOutUnits <- sd * 0.044
        
        dataSinglePred <- dataSinglePred %>% 
          left_join(metricsToAdd, by = "sku_code_prefix") %>%
          mutate(lower = preds + lower,
                 upper = preds + upper,
                 stock = preds + stock,
                 commitment = preds + commitment,
                 fillRate = 1 - stockOutUnits / (preds + meanError))
      }
        
        dataSinglePred <- dataSinglePred %>%
          select(sku_code_prefix, month, actuals, preds, errors, lower, upper, stock, fillRate,
                 commitment, meanError, sd)
        
        return(dataSinglePred)
      
    })
    
    dataPred <- rbindlist(dataPred_list)
    return(dataPred)
  })
  dataPred <- rbindlist(dataPred_list)
  
  dataPred <- dataPred %>%
    mutate(lowerOriginal = lower) %>%
    mutate(lower = ifelse(lower < 0, 0, lower)) %>%
    mutate(checkCI = actuals >= lower & actuals <= upper) %>%
    as.data.frame()
  
  return(dataPred)
}

#---

##### Distributor History ######

getDistrHistory <- function(transactions) {
  
  skuPrefixes <- unique(transactions$sku_code_prefix)
  
  distrHistory <- lapply(skuPrefixes, function(skuPrefix) {
    transactions %>%
      filter(sku_code_prefix == skuPrefix) %>%
      mutate(orderCount = n()) %>%
      group_by(distributor_id) %>%
      mutate(orderShare = n() / orderCount) %>%
      select(distributor_id, orderShare) %>% 
      arrange(distributor_id) %>%
      slice(1)
  })
  names(distrHistory) <- skuPrefixes
  return(distrHistory)
}

distrHistory <- getDistrHistory(transactions)






