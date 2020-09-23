######## Classic Modelling ############

transactions <- readRDS("data_processed/transactions_clean.rds")
data <- readRDS("data_processed/data_noDistr_extrapolated.rds")
# sku_codes_top <- getTopSkus(transactions, rankingByPrefix = TRUE, returnPrefix = TRUE, topPercentSkus = 0.01)

sku_codes_top <- readRDS("data_processed/importantSkus.rds")

data_Train <- data %>%
  filter(sku_code_prefix %in% sku_codes_top[1:20])

model <- mClinicaML(data = data_Train, 
                    model_reg = "rpart",
                    model_binary = "rpart",
                    number_reg = 4,
                    number_class = 4,
                    useCorrFeatures = FALSE,
                    useDetrending = TRUE,
                    n.ahead = 4,
                    updateForecasts = FALSE)

# Extract results
dataPred <- model$dataPred_aggSkuPrefix
dataAll <- model$dataAll_aggSkuPrefix

getOverUnderPercent(dataPred$errors)

accuracyPerSku <- getAccuracyPerSku(dataPred,
                                    errorMeasures = c("rmse",
                                                      "rmse_scaled",
                                                      "mae",
                                                      "mae_scaled",
                                                      "smape",
                                                      "mase"))


smape <- accuracyPerSku$smape
smape <- sort(smape)
sum(smape <= 0.4) / (length(smape))

accuracyPerSku %>%
  filter(mae_scaled < Inf) %>%
  summary()

dataPred_binary <- dataPred %>% 
  mutate(actuals_binary = as.factor(actuals > 0),
         preds_binary = as.factor(preds > 0))

confusionMatrix(data = dataPred_binary$preds_binary, reference = dataPred_binary$actuals_binary)

#---


######## Removing Features ############

transactions <- readRDS("data_processed/transactions_clean.rds")
data <- readRDS("data_processed/training_withoutDistr_extrapolated.rds")
sku_codes_top <- getTopSkus(transactions, rankingByPrefix = TRUE, returnPrefix = TRUE, topPercentSkus = 0.01)

sku_codes_top <- readRDS("data_processed/importantSkus.rds")

# featuresSelected <- c("demand",
#                       "Ordered_date_yearweek",
#                       "pharmaciesActive_sinceBeginning",
#                       "cumDemand_lag1_SkuPrefix",
#                       "demand_lag1_SkuPrefix",
#                       "pricePerSingleUnit",
#                       "quantity_perPackage",
#                       "sku_code_prefix",
#                       "sku_code")

data_Train <- data %>%
  filter(sku_code_prefix %in% sku_codes_top)

removeFeatures <- grep(colnames(data), value = TRUE, pattern = "price|Price")
removeFeatures <- setdiff(removeFeatures, c("priceRatio_median_allTime_SkuPrefix", 
                                            "priceRatio_min_allTime_SkuPrefix",
                                            "priceRatio_median_last4Weeks_SkuPrefix", 
                                            "priceRatio_min_last4Weeks_SkuPrefix"))

data_Train <- data_Train %>%
  select(matches("PriceValid")) %>%
  mutate(activeWeekdays = rowSums(.)) %>%
  select(activeWeekdays) %>%
  bind_cols(data_Train)

model <- mClinicaML_combined(data = data_Train, 
                             model = "xgbTree",
                             model_binary = "xgbTree",
                             number_reg = 3,
                             number_class = 3,
                             corrFeatures = FALSE, 
                             n.ahead = 4,
                             updateForecasts = TRUE)

# saveRDS(model, "data_processed/xgbTree_ImpSkus_combined_updated_noDetrend_noCorrCols_smapeMetric2.rds")

# Extract results
dataPred <- model$dataPred_aggSkuPrefix
dataAll <- model$dataAll_aggSkuPrefix

getOverUnderPercent(dataPred$errors)

accuracyPerSku <- getAccuracyPerSku(dataPred,
                                    errorMeasures = c("rmse",
                                                      "rmse_scaled",
                                                      "mae",
                                                      "mae_scaled",
                                                      "smape",
                                                      "mase"))


smape <- accuracyPerSku$smape
smape <- sort(smape)
sum(smape <= 0.4) / (length(smape))

accuracyPerSku %>%
  filter(mae_scaled < Inf) %>%
  summary()

dataPred_binary <- dataPred %>% 
  mutate(actuals_binary = as.factor(actuals > 0),
         preds_binary = as.factor(preds > 0))

confusionMatrix(data = dataPred_binary$preds_binary, reference = dataPred_binary$actuals_binary)

#---

###### Benchmark Model ######

weeklyDemand <- readRDS("data_processed/weeklyDemandTs.rds")
transactions <- readRDS("data_processed/transactions_clean.rds")
# sku_codes_prefix <- getTopSkus(transactions, rankingByPrefix = TRUE, returnPrefixes = TRUE, topPercentSkus = 0.3)

importantSkus <- readRDS("data_processed/importantSkus.rds")
demandPerSku <- weeklyDemand[names(weeklyDemand) %in% importantSkus]

benchModel <- benchmarkModelling(demandPerSku,
                                 model = "hw")

dataPred <- benchModel$dataPred
dataAll <- benchModel$dataAll

accuracyPerSku <- getAccuracyPerSku(dataPred)

accuracyPerSku %>%
  filter(mae_scaled < Inf) %>%
  summary()

dataPred_binary <- dataPred %>% 
  mutate(actuals_binary = as.factor(actuals > 0),
         preds_binary = as.factor(preds > 0))

confusionMatrix(data = dataPred_binary$preds_binary, reference = dataPred_binary$actuals_binary)

#---

##### Comparing Train Sizes #######

transactions <- readRDS("transactions_clean.rds")
data <- readRDS("training_withoutDistr_extrapolated.rds")

# Training top 1% of SKUs
sku_codes_top <- getTopSkus(transactions, rankingByPrefix = TRUE, returnPrefix = TRUE, topPercentSkus = 0.01)
data_Train <- data %>%
  filter(sku_code_prefix %in% sku_codes_top) 

model_1 <- mClinicaML_combined(data = data_Train, 
                             model = "xgbTree",
                             model_binary = "xgbTree",
                             n.ahead = 4,
                             updateForecasts = TRUE)

dataPred_1 <- model_1$dataPred_aggSkuPrefix
accSku_1 <- getAccuracyPerSku(dataPred_1)

#---

# Training top 5% of SKUs
sku_codes_top <- getTopSkus(transactions, rankingByPrefix = TRUE, returnPrefix = TRUE, topPercentSkus = 0.05)
data_Train <- data %>%
  filter(sku_code_prefix %in% sku_codes_top) 

model_5 <- mClinicaML_combined(data = data_Train, 
                                model = "xgbTree",
                               model_binary = "xgbTree",
                                n.ahead = 4,
                                updateForecasts = FALSE)

dataPred_5 <- model_5$dataPred_aggSkuPrefix
accSku_5 <- getAccuracyPerSku(dataPred_5)

accSku_5_fil <- accSku_5 %>%
  filter(sku_code_prefix %in% accSku_1$sku_code_prefix)

summary(accSku_1[, 1:6] - accSku_5_fil[, 1:6])

plotData <- stack(list("1" = accSku_1$mae_scaled, "5" = accSku_5_fil$mae_scaled))

ggplot(plotData, aes(x = ind, y = values)) +
  geom_boxplot(fill = "steelblue") + 
  labs(x = "Model", 
       y = "Error", 
       title = "Distribution of errors among all fitted models") + 
  theme_bw()

#---

# Training top 10% of SKUs
sku_codes_top <- getTopSkus(transactions, rankingByPrefix = TRUE, returnPrefix = TRUE, topPercentSkus = 0.1)
data_Train <- data %>%
  filter(sku_code_prefix %in% sku_codes_top) 

model_10 <- mClinicaML_combined(data = data_Train, 
                                model = "xgbTree",
                                model_binary = "xgbTree",
                                n.ahead = 4,
                                updateForecasts = FALSE)

dataPred_10 <- model_10$dataPred_aggSkuPrefix
accSku_10 <- getAccuracyPerSku(dataPred_10)

accSku_10_fil <- accSku_10 %>%
  filter(sku_code_prefix %in% accSku_5$sku_code_prefix)

summary(accSku_5[, 1:6] - accSku_10_fil[, 1:6])

plotData <- stack(list("5" = accSku_5$mae_scaled, "10" = accSku_10_fil$mae_scaled))

ggplot(plotData, aes(x = ind, y = values)) +
  geom_boxplot(fill = "steelblue") + 
  labs(x = "Model", 
       y = "Error", 
       title = "Distribution of errors among all fitted models") + 
  theme_bw()


#---

# Training top 20% of SKUs
sku_codes_top <- getTopSkus(transactions, rankingByPrefix = TRUE, returnPrefix = TRUE, topPercentSkus = 0.2)
data_Train <- data %>%
  filter(sku_code_prefix %in% sku_codes_top) 

model_20 <- mClinicaML_combined(data = data_Train, 
                                model = "xgbTree",
                                model_binary = "xgbTree",
                                n.ahead = 4,
                                updateForecasts = FALSE)

dataPred_20 <- model_20$dataPred_aggSkuPrefix
accSku_20 <- getAccuracyPerSku(dataPred_20)

accSku_20_fil <- accSku_20 %>%
  filter(sku_code_prefix %in% accSku_10$sku_code_prefix)

summary(accSku_10[, 1:6] - accSku_20_fil[, 1:6])

plotData <- stack(list("10" = accSku_10$mae_scaled, "20" = accSku_20_fil$mae_scaled))

ggplot(plotData, aes(x = ind, y = values)) +
  geom_boxplot(fill = "steelblue") + 
  labs(x = "Model", 
       y = "Error", 
       title = "Distribution of errors among all fitted models") + 
  theme_bw()


#---

accSku_20_fil <- accSku_20 %>%
  filter(sku_code_prefix %in% accSku_5$sku_code_prefix)

summary(accSku_5[, 1:6] - accSku_20_fil[, 1:6])

plotData <- stack(list("5" = accSku_5$mae_scaled, "20" = accSku_20_fil$mae_scaled))

ggplot(plotData, aes(x = ind, y = values)) +
  geom_boxplot(fill = "steelblue") + 
  labs(x = "Model", 
       y = "Error", 
       title = "Distribution of errors among all fitted models") + 
  theme_bw()

#---

##### 0-Demands Plot #####

dataPred <- model$dataPred_aggSkuCode

dataPred_binary <- dataPred %>% 
  mutate(actuals_binary = as.factor(ifelse(actuals > 0, "> 0", "0")),
         preds_binary = as.factor(ifelse(preds > 0, "> 0", "0")))

confMatrix <- confusionMatrix(data = dataPred_binary$preds_binary, reference = dataPred_binary$actuals_binary)

fourfoldplot(confMatrix$table, color = c("#CC6666", "#99CC99"),
             conf.level = 0, margin = 1, main = "Confusion Matrix", std = "all.max")

#---

##### Time Series Plots #####

# Top-Revenue SKUs

dataAll <- model$dataAll_aggSkuPrefix
topRevenueSkus <- revenueData$sku_code_prefix[1:2]

plotPreds(dataAll, 
          sku_code_prefix = topRevenueSkus[1],
          includeTraining = TRUE)

plotPreds(dataAll, 
          sku_code_prefix = topRevenueSkus[2],
          includeTraining = TRUE)

#---

# Top StockOut SKUs

dataAll <- model$dataAll_aggSkuPrefix
topStockOutsSkus <- stockOutData$sku_code_prefix[1:2]

plotPreds(dataAll, 
          sku_code_prefix = topStockOutsSkus[1],
          includeTraining = TRUE)

plotPreds(dataAll, 
          sku_code_prefix = topStockOutsSkus[2],
          includeTraining = TRUE)

#---

# Inf SKUs

dataAll <- model$dataAll_aggSkuPrefix
dataPred <- model$dataPred_aggSkuPrefix

accOrdered <- dataPred %>%
  getAccuracyPerSku() %>%
  arrange(desc(mae_scaled))

infSkus <- accOrdered$sku_code_prefix[1:2]

plotPreds(dataAll, 
          sku_code_prefix = infSkus[1],
          includeTraining = TRUE)

plotPreds(dataAll, 
          sku_code_prefix = infSkus[2],
          includeTraining = TRUE)

#---

# Worst SKUS

dataAll <- model$dataAll_aggSkuPrefix
dataPred <- model$dataPred_aggSkuPrefix

accOrdered_fil <- accOrdered %>%
  filter(mae_scaled < Inf)

worstSkus <- accOrdered_fil$sku_code_prefix[1:2]

plotPreds(dataAll, 
          sku_code_prefix = worstSkus[1],
          includeTraining = TRUE)

plotPreds(dataAll, 
          sku_code_prefix = worstSkus[2],
          includeTraining = TRUE)

#---

###### Low Demand SKUs ######

data <- readRDS("data_processed/training_withoutDistr_extrapolated.rds")
importantSkus <- readRDS("data_processed/importantSkus.rds")

data_Train <- data %>%
  filter(sku_code_prefix %in% importantSkus)

transactions <- readRDS("data_processed/transactions_clean.rds")

model <- readRDS("data_processed/xgbTree_impSkus_combined_updated_noDetrend_noCorrCols_newMetric2.rds")
dataPred <- model$dataPred_aggSkuPrefix
dataAll <- model$dataAll_aggSkuPrefix
acc <- getAccuracyPerSku(dataPred)

acc %>%
  filter(mae_scaled > 2) # 4x Inf, 4x über 10, 28x unter 10

skusBad_all <- acc %>%
  filter(mae_scaled > 2) %>%
  arrange(desc(mae_scaled)) %>%
  select(sku_code_prefix) %>%
  fastDoCall("c", .) %>%
  unname(TRUE)

skusInf <- acc %>%
  filter(mae_scaled == Inf) %>%
  arrange(desc(mae_scaled)) %>%
  select(sku_code_prefix) %>%
  fastDoCall("c", .) %>%
  unname(TRUE)

skusBad <- acc %>%
  filter(mae_scaled > 2 & mae_scaled < Inf) %>%
  arrange(desc(mae_scaled)) %>%
  select(sku_code_prefix) %>%
  fastDoCall("c", .) %>%
  unname(TRUE)

skusPerfect <- acc %>%
  filter(mae_scaled == 0) %>%
  select(sku_code_prefix) %>%
  fastDoCall("c", .) %>%
  unname(TRUE)

# ERKENNTNISSE: 
# 1) Die Plots zeigen vor allem eine wichtige Tatsache auf: Unser Modell nutzt zur Vorhersage von 0-Demands
# sehr stark die vorliegenden Lags. In dne Fällen, die wir gut vorhergesagt haben, lag stets ein 0-Demand direkt vor 
# Beginn des Testzeitraumes vor. Diesen kann das Modell als Indikator nutzen, um weitere 0-Demands vorherzusagen.
# Es stellt sich die Frage, ob unser Modell erheblich besser performed, was 0-Demands angeht, wenn wir ohne updaten
# arbeiten, denn dann hat es die Möglichkeit, auftretende 0-Demands im Testzeitraum als neue wichtige Information
# nutzen zu können
# 2) Durch das Inkludieren der wichtigsten StockOut-SKUs kommen auch SKUs dazu, welche extrem selten bestellt wurden,
# bspw. 102407 oder 105686, wobei letztere sogar in den letzten 35 Wochen gar nicht mehr bestellt wurde. 
# Es macht wohl keinen Sinn, 80% aller StockOut-SKUs inkludieren zu wollen, evtl. sollte man hier auf 50% reduzieren,
# denn dadurch wird bereits eine erhebliche Anzahl an SKUs gespart.
# Ebenfalls sollte man darüber nachdenken, SKUs, welche seit mehreren Wochen gar nicht mehr bestellt wurden, ab einem
# gewissen CutOff einfach rauszuschmeißen.

# SKUs mit Infinity MAE_sclaed
plotPreds_main(model$dataAll_noAgg,
          sku_code_prefix = skusInf[1])

plotPreds(dataAll,
          sku_code_prefix = skusInf[2])

plotPreds(dataAll,
          sku_code_prefix = skusInf[3])

plotPreds(dataAll,
          sku_code_prefix = skusInf[4])


# SKUs mit perfektem MAE_scaled

plotPreds(dataAll,
          sku_code_prefix = skusPerfect[1])

plotPreds_main(model$dataAll_noAgg,
          sku_code_prefix = skusPerfect[2])

plotPreds(dataAll,
          sku_code_prefix = skusPerfect[3])

plotPreds(dataAll,
          sku_code_prefix = skusPerfect[4])

plotPreds_main(model$dataAll_noAgg,
          sku_code_prefix = skusPerfect[5])

plotPreds(dataAll,
          sku_code_prefix = skusPerfect[6])

plotPreds(dataAll,
          sku_code_prefix = skusPerfect[7])

plotPreds(dataAll,
          sku_code_prefix = skusPerfect[8])

plotPreds(dataAll,
          sku_code_prefix = skusPerfect[9])

plotPreds(dataAll,
          sku_code_prefix = skusPerfect[10])

#---

plotPreds(dataAll,
          sku_code_prefix = skusBad[1])

plotPreds(dataAll,
          sku_code_prefix = skusBad[2])

plotPreds(dataAll,
          sku_code_prefix = skusBad[3])

plotPreds(dataAll,
          sku_code_prefix = skusBad[4])

plotPreds(dataAll,
          sku_code_prefix = skusBad[5])

# Hierbei handelt es sich um eines der meistgeorderten Produkte überhaupt. Die gewaltige Überschätzung ist durchaus
# problematisch
plotPreds_main(model$dataAll_noAgg,
          sku_code_prefix = 103412)

#---

# VERGLEICH BAD AND NONE BAD SKUS

orderData_bad <- transactions %>%
  filter(Ordered_date_yearweek > 65) %>%
  filter(sku_code_prefix %in% skusBad_all) %>%
  group_by(sku_code_prefix) %>%
  group_by(distributor_id, add = TRUE) %>%
  summarise(ordersPerDistr = n()) %>%
  group_by(sku_code_prefix) %>%
  mutate(totalOrders = sum(ordersPerDistr),
         sharePerDistr = ordersPerDistr / totalOrders,
         maxSharePerDistr = max(sharePerDistr)) %>%
  mutate(numbDistr = uniqueN(distributor_id)) %>%
  as.data.frame()

orderData_good <- transactions %>%
  filter(Ordered_date_yearweek > 65) %>%
  filter(!sku_code_prefix %in% skusBad_all & sku_code_prefix %in% data_Train$sku_code_prefix) %>%
  group_by(sku_code_prefix) %>%
  group_by(distributor_id, add = TRUE) %>%
  summarise(ordersPerDistr = n()) %>%
  group_by(sku_code_prefix) %>%
  mutate(totalOrders = sum(ordersPerDistr),
         sharePerDistr = ordersPerDistr / totalOrders,
         maxSharePerDistr = max(sharePerDistr)) %>%
  mutate(numbDistr = uniqueN(distributor_id)) %>%
  as.data.frame()

zeroDemandData <- dataAll %>%
  group_by(sku_code_prefix) %>%
  summarise(numbWeeksZero = sum(actuals == 0))

orderData_bad <- left_join(orderData_bad, zeroDemandData, by = "sku_code_prefix")
orderData_good <- left_join(orderData_good, zeroDemandData, by = "sku_code_prefix")

# Die Anzahl der Order pro Produkt ist erheblich geringer, was eben auch zu erwarten war. Mit Produkt 103412
# gibt es allerdings eine Ausnahme, denn dieses befindet sich höchstwahrscheinlich sogar unter den Top 10
# meist georderten Produkten. 
# Evtl. könnte bei diesem Produkt Detrending helfen, wer weiß.
orderData_bad %>%
  group_by(sku_code_prefix) %>%
  slice(1) %>%
  select(totalOrders) %>%
  summary()

orderData_good %>%
  group_by(sku_code_prefix) %>%
  slice(1) %>%
  select(totalOrders) %>%
  summary()

# Es gibt keine größere Gewichtung auf einen einzelnen Distributor, sodass das Risiko, dass das Produkt gar nicht 
# verfügbar ist, dahingehend nicht steigt. Die Möglichkeit der nicht-Verfügbarkeit wird lediglich durch die geringere
# Anzahl an Orders realistischer.
summary(orderData_bad$maxSharePerDistr)
summary(orderData_good$maxSharePerDistr)

# Auch bei der Anzahl der liefernden Wholesaler gibt es kaum Unterschiede.
summary(orderData_bad$numbDistr)
summary(orderData_good$numbDistr)

# Zero Demands: Hier liegen gravierende Unterschiede vor, was man natürlich auch so erwarten konnte. Die Anzahl der
# 0-Wochen ist erheblich höher, im Median um den Faktor 2.5
orderData_bad %>%
  group_by(sku_code_prefix) %>%
  slice(1) %>%
  select(numbWeeksZero) %>%
  summary()

orderData_good %>%
  group_by(sku_code_prefix) %>%
  slice(1) %>%
  select(numbWeeksZero) %>%
  summary()

# Plotting

orderDataBad_totalOrders <- orderData_bad %>%
  group_by(sku_code_prefix) %>%
  slice(1) %>%
  group_by() %>%
  mutate(countAllSkus = n()) %>%
  mutate(upper = ceiling(totalOrders / 100) * 100) %>%
  group_by(upper) %>%
  mutate(share = n() / countAllSkus) %>%
  slice(1) %>%
  arrange(upper) %>%
  select(upper, share) %>%
  as.data.frame()

orderDataGood_totalOrders <- orderData_good %>%
  group_by(sku_code_prefix) %>%
  slice(1) %>%
  group_by() %>%
  mutate(countAllSkus = n()) %>%
  mutate(upper = ceiling(totalOrders / 100) * 100) %>%
  group_by(upper) %>%
  mutate(share = n() / countAllSkus) %>%
  slice(1) %>%
  arrange(upper) %>%
  select(upper, share) %>%
  as.data.frame()

#---

# VERGLEICH BAD UND PERFECT SKUS

orderData_bad <- transactions %>%
  filter(Ordered_date_yearweek > 65) %>%
  filter(sku_code_prefix %in% skusBad_all) %>%
  group_by(sku_code_prefix) %>%
  group_by(distributor_id, add = TRUE) %>%
  summarise(ordersPerDistr = n()) %>%
  group_by(sku_code_prefix) %>%
  mutate(totalOrders = sum(ordersPerDistr),
         sharePerDistr = ordersPerDistr / totalOrders,
         maxSharePerDistr = max(sharePerDistr)) %>%
  mutate(numbDistr = uniqueN(distributor_id)) %>%
  as.data.frame()

orderData_perfect <- transactions %>%
  filter(Ordered_date_yearweek > 65) %>%
  filter(sku_code_prefix %in% skusPerfect) %>%
  group_by(sku_code_prefix) %>%
  group_by(distributor_id, add = TRUE) %>%
  summarise(ordersPerDistr = n()) %>%
  group_by(sku_code_prefix) %>%
  mutate(totalOrders = sum(ordersPerDistr),
         sharePerDistr = ordersPerDistr / totalOrders,
         maxSharePerDistr = max(sharePerDistr)) %>%
  mutate(numbDistr = uniqueN(distributor_id)) %>%
  as.data.frame()

zeroDemandData <- dataAll %>%
  group_by(sku_code_prefix) %>%
  summarise(numbWeeksZero = sum(actuals == 0))

orderData_bad <- left_join(orderData_bad, zeroDemandData, by = "sku_code_prefix")
orderData_perfect <- left_join(orderData_perfect, zeroDemandData, by = "sku_code_prefix")

# Hier ist die Anzahl an Orders bei den perfekten SKUs sogar noch etwas geringer
summary(orderData_bad$totalOrders)
summary(orderData_perfect$totalOrders)

# Es liegt zumindest eine etwas stärkere Gewichtung auf einen einzelnen Wholesaler bei den perfekten SKUs vor.
summary(orderData_bad$maxSharePerDistr)
summary(orderData_perfect$maxSharePerDistr)

# Wie zu erwarten ist die Anzahl der liefernden Wholesaler ebenfalls etwas geringer bei den perfekten SKUs.
summary(orderData_bad$numbDistr)
summary(orderData_perfect$numbDistr)

# Zero Demands: Die Anzahl der 0-Demand Wochen ist bei den perfekten SKUs nochmals erheblich höher, im Median und
# 3.Quantil über 50%.
summary(orderData_bad$numbWeeksZero)
summary(orderData_perfect$numbWeeksZero)

#---

##### No Demand SKUs #####

noDemandData <- data %>%
  filter(Ordered_date_yearweek >= max(Ordered_date_yearweek) - 9) %>%
  group_by(sku_code_prefix) %>%
  summarise(noDemand = all(demand == 0)) %>%
  as.data.frame()

noDemandSkus <- noDemandData %>%
  filter(noDemand) %>%
  select(sku_code_prefix) %>%
  fastDoCall("c", .) %>%
  unname(TRUE)

sum(importantSkus %in% noDemandSkus) # Lediglich 4 SKUs sind in die "importantSKUs" gerutscht, welche in den letzten 0 
# Wochen gar nicht bestellt wurden. Diese sollte man dennoch evtl. entfernen. 

#---

###### Corr Revenue / Accuracy ######

# Unsere These ist, dass überdurchschnittler Umsatz mit besserer Forecast-Performance korreliert. Dann testen wir das mal
# und hoffen, dass wir tatsächlich schöne Ergebnisse rauskommen
# Wir sollten diese Behauptung einmal innerhalb aller SKUs testen und dann nochmal innerhalb der Top-Payment SKUs, 
# welcher wir am besten identisch zu unseren Thresholds im Scatterplot definieren.

revenueAccData <- read.csv2("data_processed/revenueAccData.csv", stringsAsFactors = FALSE)[, -1]

revenueAccData_filtered <- revenueAccData %>%
  filter(mae_scaled < Inf) %>%
  mutate(revenueShare = log10(revenueShare))

cor(revenueAccData_filtered$mae_scaled, log10(revenueAccData_filtered$revenueShare))

cor(revenueAccData$smape, revenueAccData$revenueShare)

# Monthly
revenueAccData_month <- read.csv2("data_processed/revenueAccData_month.csv", stringsAsFactors = FALSE)[, -1]

revenueAccData_filtered <- revenueAccData_month %>%
  filter(mae_scaled < Inf)
cor(revenueAccData_filtered$mae_scaled, revenueAccData_filtered$revenueShare)

cor(revenueAccData_month$smape, revenueAccData_month$revenueShare)

# Building groups
revenueAccData_top <- revenueAccData %>%
  filter(revenueShare >= 0.001)
cor(revenueAccData_top$rmse_scaled, revenueAccData_top$revenue)

revenueAccData %>%
  filter(revenueShare >= 0.001) %>%
  summary(.$mae_scaled)

revenueAccData %>%
  filter(revenueShare < 0.001) %>%
  summary(mae_scaled)

#---



