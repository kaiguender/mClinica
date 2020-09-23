###### Packages #####

library(tidyverse)
library(sjmisc)
library(Gmisc)
library(fasttime)
library(doParallel)
library(foreach)
library(lubridate)
library(data.table)
library(batman)

#---

# There are two ways to get prices. As there is a high chance that we get price data later on we have to go about this
# in such a way that we can easily include this price data later on. Because of this the first step is always creating
# a data frame of price data in a standardized form. 
# a) GOAL: The data frame should contain the following columns:
# 1) sku_code
# 2) sku_code_prefix
# 3) date (no time)
# 4) distributor_1,...,distributor_7: These columns should be named according to the number of the distributor
# 5) Area (optional): We would have to investigate whether one distributor can offer a different price in different regions.
#    At least reading the stock-files provided in Google Drive, this doesn't seem to be the case.
# 6) unit_selling_price_updated
# 7) discount
# 8) quantity_per_package (constant for every sku_code)

# How To (applying to transaction data):
# 1) The top-level are the sku_codes. For every sku_code we have to create one data frame that contains a row for every day of
# the data.
# 2) The sku_code_prefix will simply be attached as a constant column
# 3) We order the transaction data by sku_code and Ordered_date. This way we can simply extract the prices paid on a daily basis.
# 4) If a sku_code wasn't bought on a specific day, we simply use the most recent price as the current price.

# Special remarks:
# 1) QUESTION: In order to determine a price for a sku_code during the time window before it has been ordered
# the first time we simply extrapolate the first appearing price backward. Does this make sense? 
# 2) The distributor_x columns are boolean and determine to which wholesaler the price of the row belongs.
# Therefor always exactly one of the distributor_x columns is TRUE, all the others are FALSE.
# 3) In almost all cases there is only one price offered per day by each wholesaler for one sku_code.
# In numbers: For the top 20% sku_codes for 2910509 combinations of sku_code, day and wholesaler,
# only one price was available. Only in 2955 cases there is more than one price available per day.
# NOTE: By price we are refering to a combination of unit_selling_price and unit_discount.
# 4) When there is more than one price offered per day, we keep track of all of them. In the most extreme
# case there have been 11 

# Jede aufgegebene Order in den Transaktionsdaten gibt uns Informationen ?ber den aktuellen Preis, der f?r einen
# sku_code von einem bestimmten Wholesaler angeboten wird.
# Hierbei ergeben sich 2 Probleme:
# 1) Nicht jeden Tag wird eine Order f?r einen sku_code aufgegeben --> keinerlei Preisinformation --> 
# Extrapolation n?tig
# 2) Wir sehen in jedem Fall immer nur den besten Preis und nicht die angebotenen Preise der anderen Wholesaler -->
# Preise der anderen Wholesaler m?ssen durch vorherige Orders extrapoliert werden

# Grunds?tzlich gibt die Funktion getPriceDataPerTransactions folgendes aus:
# 1) F?r jeden Tag, der Teil der Transaktions-Daten ist, wird f?r jeden sku_code mindestens ein Preis angegeben.
# 2) Wenn f?r einen sku_code keine Order an einem bestimmten Tag vorlag, so benutzen wir als Preis die zuletzt
# aufgegebene Order.

# Die Parameter 'interpolatePerDistributor' und 'extrapolatePerSkuCodePrefix' determinieren, was wir letztendlich
# extrapolieren und welche Informationsdichte in Bezug auf die Preise der ausgegebene Dataframe am Ende hat:
# 1) Wenn 'interpolatePerDistributor == TRUE' gilt, so extrapolieren wir f?r jeden sku_code und jeden Wholesaler,
# der jemals diesen sku_code geliefert hat, einen Preis. 
# Wenn hingegen 'interpolatePerDistributor == FALSE' gilt, so extrapolieren wir nur auf der Gesamtheit aller
# Wholesaler. Letztendlich extrapolieren wir damit stets einfach den Best-Preis. Damit enth?lt der finale
# Dataframe f?r jeden sku_code und f?r jeden Tag den zugeh?rigen Bestpreis. 
# WICHTIG: Es kann auch hier trotzdem durchaus vorkommen, dass an einem Tag mehrere Eintr?ge f?r einen sku_code 
# vorhanden sind. Dies ist genau dann der Fall, wenn am gleichen Tag Orders zu unterschiedlichen Preisen
# aufgetreten sind
# 2) Wenn 'extrapolatePerSkuCodePrefix == TRUE' gilt, so extrapolieren wir nicht auf der Ebene des sku_codes,
# sondern auf der Ebene des sku_codes_prefix. Wenn folglich nun an einem Tag fuer einen sku_code_prefix keine Order
# vorliegt, so geben wir als aktuellen Preis fuer diesen sku_code_prefix den letzten Preis dieses sku_code_prefixes an.

# Sonderfall Promotions:
# Es macht keinen Sinn, Promotions zu extrapolieren. Wenn wir auf der Ebene der sku_codes arbeiten, so k?nnen wir 
# dieses Problem einfach loesen, indem wir alle Eintraege loeschen, die sowohl eine Promotion enthalten als auch
# extrapoliert wurden.
# Allerdings kann dies durchaus Probleme bereiten, wenn wir auf der Ebene der sku_code_prefixes extrapolieren.
# Durch das nachträgliche Löschen würden wir nämlich hierauf möglicherweise Tage enthalten, die keinen
# Eintrag besitzen.

# OUTPUT
# columns: sku_code_prefix, sku_code, Ordered_date_date, Ordered_date_dayOfWeek, Ordered_date_yearweek,
#          molecule, quantity_perPackage, quantity, unit_selling_price_updated, unit_discount_updated
#          promoDiscount, extrapolared, daysNoOrder
#
# In jedem Fall existiert für jeden Tag und jeden sku_code_prefix mindestens ein Preiseintrag. Wenn wir auf der Ebene
# der distributor extrapolieren, so existieren an jedem Tag mindestens so viele Preiseinträge wie es Wholesaler gibt,
# die diesen sku_code verkaufen.



relevantColumns <- c("Ordered_date_date", "Ordered_date_yearweek",
                     "sku_code", "distributor_id", "quantity",
                     "unit_selling_price_updated", "unit_discount_updated")

transactions <- readRDS("transactions_clean.rds")

#---

####### Execution of Functions ##########

transactions <- readRDS("SavedData/transactions_clean.rds")

data_withDistr_extrapolated <- getTrainingMain(transactions,
                                               topPercentSkus = 0.3,
                                               interpolatePerDistributor = TRUE,
                                               noExtrapolation = FALSE,
                                               getMolFeatures = FALSE,
                                               dateStart = "2019-03-04",
                                               dateEnd = "2020-05-03")
saveRDS(data_withDistr_extrapolated, "data_withDistr_extrapolated.rds")

data_noDistr_extrapolated <- getTrainingMain(transactions = transactions,
                                             topPercentSkus = 1,
                                             interpolatePerDistributor = FALSE,
                                             noExtrapolation = FALSE,
                                             numbExtraWeeks = 12,
                                             getMolFeatures = FALSE,
                                             dateStart = "2019-03-04",
                                             dateEnd = "2020-05-03")
saveRDS(data_noDistr_extrapolated, "SavedData/data_noDistr_extrapolated_extended.rds")

data_noDistr_NotExtrapolated <- getTrainingMain(transactions = transactions,
                                                topPercentSkus = 0.3,
                                                interpolatePerDistributor = FALSE,
                                                noExtrapolation = TRUE,
                                                getMolFeatures = FALSE,
                                                dateStart = "2019-03-04",
                                                dateEnd = "2020-05-03")
saveRDS(data_noDistr_NotExtrapolated, "data_noDistr_withoutDistr.rds")

countsWeek <- transactions %>%
  group_by(Ordered_date_yearweek) %>%
  summarise(count = n())

#---

############ Training Data - Main Function ###################

# The lagged price features create NAs for the first weeks depending on the lags used, which we have to drop.
# The final dataset therefor contains less weeks than specified by 'dateStart'.

getTrainingMain <- function(transactions,
                            topPercentSkus = 0.1,
                            interpolatePerDistributor = FALSE,
                            noExtrapolation = FALSE,
                            lags = c(1, 2, 3, 4),
                            numbExtraWeeks = 0,
                            getMolFeatures = FALSE,
                            dateStart = NULL,
                            dateEnd = NULL) {
  
  # The pharmacy-Covariates are always supposed to be calculated using the whole time horizon of the available data,
  # simply because it only makes sense to calculate the amount of active pharmacies that way.
  # NOTE: 'getPharmacyCovariates' computes the count of active pharmacies on a daily basis, which is the reason, why
  # we have to use the 'slice'-function to create useful weekly data.
  pharmacyCovariates <- getPharmacyCovs(transactions)
  pharmacyCount_startOfWeek <- pharmacyCovariates %>%
    group_by(Ordered_date_yearweek) %>% 
    slice(1)

  transactions <- transactions %>%
    filter(Ordered_date >= as.Date(dateStart) & Ordered_date <= as.Date(dateEnd)) 
  
  priceData_daily <- getPriceDataPerTransactions(transactions,
                                                 topPercentSkus = topPercentSkus,
                                                 interpolatePerDistributor = interpolatePerDistributor)
  
  
  data <- getAggregatedData(priceData_daily = priceData_daily,
                            interpolatePerDistributor = interpolatePerDistributor,
                            noExtrapolating = noExtrapolation,
                            lags = lags)
  
  # We have to put 'pharmacyCovariates[, -1]' because we don't want add the column 'Ordered_date_date'.
  data <- left_join(data, pharmacyCount_startOfWeek[, -1], by = "Ordered_date_yearweek")

  if(numbExtraWeeks > 0) {
    data <- getExtendedData(data,
                            numbExtraWeeks)
  }

  # IMPORTANT NOTE: 
  # The function 'getAdditionalPriceData' creates lagged price features. Obviously no lagged price features can
  # be computed for the earlierst weeks (depending on the used lag). Therefor we cut off all weeks, for which no
  # lagged price features could be calculated and loose a certain amount of weeks of training data depending on
  # the lag.
  if(interpolatePerDistributor) {
    data <- getLagPrices(data,
                         aggLevel = c("sku_code",
                                      "distributor_id"),
                         lags = c(1, 2, 3, 4))
  }
  
  data <- getLagPrices(data,
                       aggLevel = "sku_code",
                       lags = c(1, 2, 3, 4))
  
  data <- getLagPrices(data,
                       aggLevel = "sku_code_prefix",
                       lags = c(1, 2, 3, 4))
  
  data <- data %>%
    drop_na()
  
  # Get lagged demand
  if(interpolatePerDistributor) {
    data <- getLagDemand(data,
                         aggLevel = c("sku_code",
                                      "distributor_id"),
                         lags = lags)
  }
  
  data <- getLagDemand(data,
                       aggLevel = "sku_code",
                       lags = lags)
  
  data <- getLagDemand(data,
                       aggLevel = "sku_code_prefix",
                       lags = lags)
  
  # Feature tracking the amount of wholesalers selling each product. Only used if 'interpolatePerDistributor == FALSE'.
  if(!interpolatePerDistributor) {
    data <- getDistributorFeatures(transactions = transactions,
                                   data = data)
  }
  
  # Adding holiday Data
  data <- getHolidays(data)
  
  if(getMolFeatures) {
    data <- getMoleculeFeature(data)
  }
  
  return(data)
}

#---

############# Price Data via Transactions ######################

getPriceDataPerTransactions <- function(transactions,
                                        firstDay = NULL,
                                        lastDay = NULL,
                                        topPercentSkus = 0.1,
                                        interpolatePerDistributor = FALSE,
                                        extrapolatePerSkuCodePrefix = FALSE) {

  if(topPercentSkus != 1) {
    skuPrefixes <- getTopSkus(transactions = transactions, 
                            topPercentSkus = topPercentSkus, 
                            rankingByPrefix = TRUE,
                            returnPrefixes = TRUE)
    
    transactions <- transactions %>%
      filter(sku_code_prefix %in% skuPrefixes)
  }
  
  # General data that we need to built the price data frame
  distributor_ids <- sort(unique(transactions$distributor_id))
  
  if(is.null(firstDay)) {
    firstDay <- as.Date(min(transactions$Ordered_date_date))
  }
  
  if(is.null(lastDay)) {
    lastDay <- as.Date(max(transactions$Ordered_date_date))
  }
  
  # We have to consider the data of every sku_code seperarely. If 'interpolatePerDistributor == TRUE' we consider
  # the subgroups created by every wholesaler for every sku_code instead. 
  if(interpolatePerDistributor) groupingCols <- c("sku_code", "distributor_id") else groupingCols <- "sku_code"
  
  # For the following considerations it is important that the transaction-data is ordered according to the times of
  # the placement of the orders.
  transactions_ordered <- setorderv(transactions, cols = groupingCols, order = 1)
  
  # In an important first step we aggregate all orders of one sku_code that appeared on the same day and
  # had the same price. Columns like 'molecule' are only included in the groupingCols because we want to keep 
  # some static information that is constant within every group. If we want to keep any other of the static
  # informations of a sku_code, we can simply include it in 'groupingCols' as well.
  aggCols <- c("sku_code", 
               "Ordered_date_date", 
               "unit_selling_price_updated",
               "unit_discount_updated", 
               "promoDiscount", 
               "molecule",
               "sku_code_prefix",
               "quantity_perPackage")
  
  # If 'interpolatePerDistributor == TRUE' we aggregated the quantity of every combination of sku_code and price
  # seperately for every distributor. 
  if(interpolatePerDistributor) {
    aggCols <- append(aggCols, "distributor_id", after = 1)
  } 
  
  transactions_agg <- transactions_ordered %>%
    group_by(.dots = aggCols) %>%
    summarise("quantity_updated" = sum(quantity_updated))
  
  transactions_groups <- transactions_agg %>%
    group_by(.dots = groupingCols) %>%
    group_split()
  
  priceData_singleSku <- lapply(transactions_groups, function(transactions_group) {
    
    # Here we can't use the 'group_split' function to split the data of the current sku in to different groups.
    # Instead we need to monitor the development of the prices and discounts over time and grab every index
    # at which a price change occured.
    i <- 1
    priceChange_index <- 1
    
    while(i < nrow(transactions_group)) {
      
      if(transactions_group$unit_selling_price_updated[i] != transactions_group$unit_selling_price_updated[i + 1] ||
         transactions_group$unit_discount_updated[i] != transactions_group$unit_discount_updated[i + 1] ||
         transactions_group$promoDiscount[i] != transactions_group$promoDiscount[i + 1]) {
        priceChange_index <- c(priceChange_index, i + 1)
      }
      i <- i + 1
    }
    
    priceData_singlePrice <- lapply(seq_along(priceChange_index), function(j) {
      
      index_start <- priceChange_index[j]
      
      # if 'j == length(priceChange_index)' is TRUE, we are looking at the latest price change visible in the
      # data. Because of that reason the last index of all orders that possessed the same price is given by
      # nrow(transactions_group).
      # If j is smaller than length(priceChange_index) the last index is simply given by priceChange_index[j + 1] - 1.
      if(j == length(priceChange_index)) {
        index_end <- nrow(transactions_group)
        
      } else {
        index_end <- priceChange_index[j + 1] - 1
      }
      
      # We want to extrapolate the current price of the day to all other days before a new order with a new price
      # came in. For that reason we have to define date_start and date_end to specify the time horizon during
      # which the current price was valid.
      # If we look at the first observation of the current sku_code we also extrapolate the price backwards until
      # the first day of the transaction history given by 'firstDay'.
      if(j == 1) {
        date_start <- firstDay
      } else {
        date_start <- transactions_group$Ordered_date_date[index_start]
      }
      
      if(j == length(priceChange_index)) {
        date_end <- lastDay
      } else {
        
        # When there has been more than one order of the same sku with different prices per day we have to be careful
        # how we set 'date_end', which corresponds to the day until we extrapolate the current price. 
        date_end_currentPrice <- transactions_group$Ordered_date_date[index_end]
        date_start_nextPrice <- transactions_group$Ordered_date_date[index_end + 1]
        
        if(date_end_currentPrice < date_start_nextPrice) {
          date_end <- date_start_nextPrice - 1
        } else {
          date_end <- date_end_currentPrice
        }
      }
      
      # transactions_price contains all oberservations of the current price until the next price change occured.
      transactions_price <- transactions_group[index_start:index_end, ]
      
      # The function 'getExtrapolatedPrices' does the whole job of extrapolating the prices to missing days and creating
      # copies of the static columns like molecule etc.
      # The output is simply a data.frame that contains one row for every day given by the time horizon that is 
      # specified by date_start and date_end.
      extrapolatedData <- getExtrapolatedPrices(transactions_price = transactions_price,
                                                interpolatePerDistributor = interpolatePerDistributor,
                                                date_start = date_start,
                                                date_end = date_end)
      return(extrapolatedData)
      
    })
    priceData_singleSku <- rbindlist(priceData_singlePrice)
    
    return(priceData_singleSku)
    
  })
  priceData <- rbindlist(priceData_singleSku)
  priceData <- priceData[order(priceData$sku_code, priceData$Ordered_date_date), ]
  
  # It makes no sense to extrapolate promotions. This could deteriorate the price quality massively. 
  # We fix this by simply removing all rows that are both extrapolated and have a promotion
  priceData <- priceData[!(priceData$promoDiscount > 0 & priceData$extrapolated), ]
  
  # We order the price Data
  if(interpolatePerDistributor) {
    ordering <- order(priceData$sku_code_prefix, priceData$Ordered_date_date, 
                      priceData$sku_code, priceData$distributor_id)
  } else {
    ordering <- order(priceData$sku_code_prefix, priceData$Ordered_date_date, 
                      priceData$sku_code)
  }
  
  priceData <- priceData[ordering, ]
  
  return(as.data.frame(priceData))
}

#---

############## Training Data #################

getAggregatedData <- function(priceData_daily,
                              noExtrapolating = FALSE,
                              interpolatePerDistributor = FALSE,
                              lags = c(1, 2, 3, 4)) {
  
  if(noExtrapolating) {
    priceData <- priceData_daily %>%
      filter(extrapolated == FALSE)
  }
  
  # Colnames of the day-dummies we create in the next step
  dayDummies_names <- paste0("PriceValid_Day", 1:7)
  
  # As we want to aggregate the data to weekly data later on we need the day-dummies that tell us on which
  # weekday the prices were active. 
  dayDummies <- model.matrix(~ as.factor(Ordered_date_dayOfWeek) - 1, priceData_daily)
  colnames(dayDummies) <- dayDummies_names
  priceData_daily <- cbind(priceData_daily, dayDummies)
  
  # We aggregate the data to weekly data. Our goal is it to get a data frame that for all sku_codes contains all 
  # prices that have been active during every week of the data.
  # It is important to use the correct hierarchy of the grouping structure:
  # 1) On the top level are the sku_codes
  # 2) On the next level we group by the yearweek of every row. 
  # 3) Now we look at all the distributors that covered the sku_code during the chosen week.
  # 4) As it is possible for one distributor to change his price during the week we group the price data by the
  # prices and discounts on the next level.
  # 5) As we want to keep static information of the sku_code we also group by sku_code_prefix, molecule and 
  # quantity_perPackage
  groupingCols <- c("sku_code", 
                    "Ordered_date_yearweek", 
                    "unit_selling_price", 
                    "unit_discount",
                    "promoDiscount", 
                    "pricePerSingleUnit")
  
  if(interpolatePerDistributor) {
    groupingCols <- append(groupingCols, "distributor_id", after = 1)
  }
  
  staticCols <- c("sku_code_prefix",
                  "molecule",
                  "quantity_perPackage")
  
  priceData_weekly <- priceData_daily %>%
    group_by(.dots = groupingCols) %>%
    group_by(.dots = staticCols, add = TRUE) %>%
    summarise_at(vars(c(dayDummies_names, quantity)), .funs = sum)
  
  # We change the name to 'demand' instead of 'quantity'.
  colnames(priceData_weekly)[ncol(priceData_weekly)] <- "demand"
  
  # In the next step we create the covariate 'demandOfUnits', which replaces the original demand.
  demandOfUnits <- priceData_weekly$demand * priceData_weekly$quantity_perPackage
  priceData_weekly$demand <- demandOfUnits
  
  # Features concerning the time of the month
  if(interpolatePerDistributor) aggLevel <- c("sku_code", "distributor_id") else aggLevel <- "sku_code"
  
  priceData_weekly <- getMonthData(priceData_daily = priceData_daily,
                                   priceData_weekly = priceData_weekly,
                                   aggLevel = aggLevel)
  
  
  # Just some repositioning of the columns
  colnames <- colnames(priceData_weekly)
  
  colnames <- colnames[-which(colnames == "sku_code_prefix")]
  colnames <- c("sku_code_prefix", colnames)
  
  colnames <- colnames[-which(colnames == "molecule")]
  colnames <- c(colnames, "molecule")
  
  colnames <- colnames[-which(colnames == "demand")]
  colnames <- append(colnames, "demand", after = 3)
  
  priceData_weekly <- priceData_weekly[, colnames]
  
  return(as.data.frame(priceData_weekly))
}

#---

####### Week of month feature ########

# 1) The function requieres daily price data as input because the desired features 'beginOfMonth' and 'endOfMonth'
# requieres daily information about the prices.
# 2) The created features are created seperately for each combination of price, sku_code and distributor (if considered).
# Basically we track on which exact days the price for one sku_code was active during the current week and then
# check whether any of the days was part of the first 7 or last 7 days of a month. 

# aggLevel is either "sku_code, distributor_id" or only "sku_code".

getMonthData <- function(priceData_daily,
                         priceData_weekly,
                         aggLevel) {
  
  priceData_split <- priceData_daily %>% 
    group_by(.dots = aggLevel) %>%
    group_by(unit_selling_price, unit_discount, promoDiscount, add = TRUE) %>%
    group_by(Ordered_date_yearweek, add = TRUE) %>%
    group_split()
  
  keys <- priceData_daily %>% 
    group_by(.dots = aggLevel) %>%
    group_by(unit_selling_price, unit_discount, promoDiscount, add = TRUE) %>%
    group_by(Ordered_date_yearweek, add = TRUE) %>%
    group_keys()
  
  monthData_list <- lapply(priceData_split, function(group) {
    
    dates <- group$Ordered_date_date
    daysOfMonth <- day(dates)
    
    if(any(daysOfMonth >= 24)) endOfMonth <- TRUE else endOfMonth <- FALSE
    if(any(daysOfMonth <= 7)) beginOfMonth <- TRUE else beginOfMonth <- FALSE
    
    monthData <- data.frame(beginOfMonth, endOfMonth)
    return(monthData)
  })
  monthData <- rbindlist(monthData_list)
  monthData <- cbind(keys, monthData)

  priceData_weekly <- left_join(priceData_weekly, monthData, by = colnames(keys))
  
  return(priceData_weekly)
}

#---

###### Extend Features ######

# Here we try to extend all used features into the future in some logical way. We go about this using the following 
# heuristic:
# 1) Time Features: All time features like 'beginOfMonth' can simply be added by the function 'getMonthData'.
# All we have to do is call it after adding the additional weeks. The same applies to the holiday data.
# 2) Demand Features: All demand features are given the entry 0 for future observations. The necessary lag-demand-features
# will be created step-by-step using our forecasts as the necessary demand. All this will be done within the modelling
# function automatically.
# 3) Price Features: Those will simply be extended from the last month. Week 120 becomes 124, week 121 becomes 125 etc.
# We keep doing this in a 4-weeks manner for as many month as we want.
# The additional price features are simply computed exactly the same way as before by calling the 'getAdditionalPriceData'
# function. This also includes the lagged price features. 
# 4) PharmacyCovs: Here we will use a simple linear regression to extend our data into the future. At least in the past
# this explained the data really well (R^2 of 0.97)

getExtendedData <- function(data,
                            numbExtraWeeks = 12) {

  lastWeek <- max(data$Ordered_date_yearweek)
  weeksToExtend <- seq(lastWeek + 1, lastWeek + numbExtraWeeks, by = 1)
  
  # In a first step we extrapolate the number of active pharmacies into the feature
  lmData <- data %>%
    group_by(Ordered_date_yearweek) %>%
    slice(1) %>%
    select(Ordered_date_yearweek, pharmActive_sinceBeginning)
  
  lmFit <- lm(data = lmData, formula = pharmActive_sinceBeginning ~ Ordered_date_yearweek)
  
  pharmActive_sinceBeginning <- lmFit$coefficients[1] + weeksToExtend * lmFit$coefficients[2]
  pharmData <- data.frame(Ordered_date_yearweek = weeksToExtend,
                          pharmActive_sinceBeginning = pharmActive_sinceBeginning)
  
  # In the next step we extend discounts, prices etc. into the future
  last4Weeks <- (lastWeek - 3):lastWeek
  correspondingWeek <- rep_len(last4Weeks, length.out = length(weeksToExtend))
  
  dataCloned_list <- lapply(seq_along(weeksToExtend), function(i) {
    dataCloned <- data %>%
      filter(Ordered_date_yearweek == correspondingWeek[i]) %>%
      select(-pharmActive_sinceBeginning) %>%
      mutate(Ordered_date_yearweek = weeksToExtend[i])
    
    return(dataCloned)
  })
  dataCloned <- rbindlist(dataCloned_list)
  dataExtended <- left_join(dataCloned, pharmData, by = "Ordered_date_yearweek")
  
  data <- rbind(data, dataExtended)
  data <- setorderv(data, cols = c("sku_code", "Ordered_date_yearweek"))
  
  return(data)
  
}

#---

############# Pharmacy Covariate ############

getPharmacyCovs <- function(transactions) {
  
  transactions <- as.data.table(transactions)
  
  first_date <- min(transactions$Ordered_date_date)
  last_date <- max(transactions$Ordered_date_date)
  days <- seq(first_date, last_date, by = 1)
  
  pharmacyCovariates <- lapply(seq_along(days), function(i) {
    timeHorizon <- seq(from = days[i] - 30, to = days[i] - 1, by = 1)
    
    # pharmActive_month <- transactions %>% 
    #   filter(Ordered_date_date %in% timeHorizon) %>%
    #   select(pharmacy_id) %>%
    #   uniqueN()
    
    pharmActive_sinceBeginning <- transactions %>% 
      filter(Ordered_date_date < days[i]) %>%
      select(pharmacy_id) %>%
      uniqueN()
    
    # pharmOrdersMonth <- transactions %>%
    #   filter(days[i] - 28 <= Ordered_date_date  & Ordered_date_date < days[i]) %>%
    #   group_by(pharmacy_id) %>%
    #   summarise(countOrders = n())
    
    # If there haven't been any orders yet for any pharmacy (first day of data), we have to treat this case differently
    # if(nrow(pharmOrdersMonth) == 0) {
    #   pharmAverageOrdersMonth <- 0
    # } else {
    #   pharmAverageOrdersMonth <- mean(pharmOrdersMonth$countOrders) 
    # } 
    # 
    # return(data.frame(pharmActive_month, pharmActive_sinceBeginning, pharmAverageOrdersMonth))
    return(data.frame(pharmActive_sinceBeginning))
  })
  
  pharmacyCovariates <- rbindlist(pharmacyCovariates)
  
  # ---
  
  # The following steps are only necessary to get the Ordered_date_yearweek for the whole time horizon.
  Ordered_date_date <- days
  Ordered_date_year <- year(Ordered_date_date)
  
  Ordered_date_yearweek <- (Ordered_date_year == 2018)*0 + 
    (Ordered_date_year == 2019)*52 + (Ordered_date_year == 2020)*104 + isoweek(Ordered_date_date)
  
  # The date '2019-12-30' for example has the isoweek 1 and would therefor receive the Ordered_date_yearweek 
  # value of 53 despite the fact that '2019-12-30' actually belongs to week 105.
  # The following 2 lines of code keep track of that.
  endOfYearCorrection_index <- month(Ordered_date_date) == 12 & isoweek(Ordered_date_date) == 1
  Ordered_date_yearweek[endOfYearCorrection_index] <- Ordered_date_yearweek[endOfYearCorrection_index] + 52
  
  covariates <- cbind(Ordered_date_date, Ordered_date_yearweek, pharmacyCovariates)
  
  return(as.data.frame(covariates))
}

#---

############ Top sku_codes #############

getTopSkus <- function(transactions,
                       rankingByPrefix = TRUE,
                       returnPrefixes = FALSE,
                       topPercentSkus) {

  if(!rankingByPrefix & returnPrefixes) {
    warning("If 'returnPrefixes == TRUE', rankingByPrefix has to be TRUE as well. 
            Changed 'rankingByPrefix' to TRUE.")
    rankingByPrefix <- TRUE
  }
  
  if(rankingByPrefix) sku_feature <- "sku_code_prefix" else sku_feature <- "sku_code"
  
  countPerSku <- transactions %>%
    group_by(.dots = sku_feature) %>%
    summarise(count = n()) %>%
    arrange(desc(count))
  
  sku_codes_top <- countPerSku %>%
    filter(count >= quantile(countPerSku$count, 1 - topPercentSkus)) %>%
    select(sku_feature) %>%
    fastDoCall("c", .) 
  names(sku_codes_top) <- NULL
  
  if(returnPrefixes == TRUE) {
    return(sku_codes_top)
  }
  
  # If we have been working with sku_codes_prefixes we will grab all sku_codes that belong to the
  # determined top sku_code_prefixes. 
  if(!rankingByPrefix) {
    return(sku_codes_top) 
    
    # We only get down here if we want to return skuCodes but created the ranking by skuPrefixes.
  } else {
    topSkuCodes <- transactions %>%
      filter(sku_code_prefix %in% sku_codes_top) %>%
      select(sku_code) %>%
      fastDoCall("c", .)
    names(topSkuCodes) <- NULL
    
    return(topSkuCodes)
  }
}

getTopSkus(transactions, 
           topPercentSkus = 0.3,
           rankingByPrefix = TRUE,
           returnPrefixes = FALSE)

#---

############# Extrapolate Price #############

# This function is used to extrapolate the current price data to days at which no order occured. 'date_start' and 
# 'date_end' determine the time horizon of the extrapolation process.
# 'transactions_price' is supposed to be a set of orders that all possess the exact same price and discount data.

getExtrapolatedPrices <- function(transactions_price,
                                  date_start = min(transactions_price$Ordered_date_date),
                                  date_end,
                                  interpolatePerDistributor = FALSE) {
  
  # Ordered_date_date contains all days to which we extrapolate the current price.
  Ordered_date_date <- seq(date_start, date_end, by = 1)
  
  Ordered_date_year <- year(Ordered_date_date)
  
  Ordered_date_yearweek <- (Ordered_date_year == 2018)*0 + 
    (Ordered_date_year == 2019)*52 + (Ordered_date_year == 2020)*104 + isoweek(Ordered_date_date)
  
  # The date '2019-12-30' for example has the isoweek 1 and would therefor receive the Ordered_date_yearweek 
  # value of 53 despite the fact that '2019-12-30' actually belongs to week 105.
  # The following 2 lines of code keep track of that.
  endOfYearCorrection_index <- month(Ordered_date_date) == 12 & isoweek(Ordered_date_date) == 1
  Ordered_date_yearweek[endOfYearCorrection_index] <- Ordered_date_yearweek[endOfYearCorrection_index] + 52
  
  # Weekday is needed later on when we aggreate the daily data to weekly data.
  Ordered_date_dayOfWeek <- lubridate::wday(Ordered_date_date, week_start = 1, label = FALSE)
  
  # Static information of the current observations given by transactions_price.
  sku_code <- transactions_price$sku_code[1]
  sku_code_prefix <- transactions_price$sku_code_prefix[1]
  quantity_perPackage <- transactions_price$quantity_perPackage[1]
  unit_selling_price <- transactions_price$unit_selling_price_updated[1]
  unit_discount <- transactions_price$unit_discount_updated[1]
  promoDiscount <- transactions_price$promoDiscount[1]
  molecule <- transactions_price$molecule[1]
  pricePerSingleUnit <- unit_selling_price[1] * (1 - unit_discount[1]) * (1 - promoDiscount[1]) / quantity_perPackage[1]
  
  # We put all the static data into a data frame and create a copy for every day of the time horizon given by
  # date_start and date_end.
  data <- data.frame(sku_code, 
                     sku_code_prefix,
                     pricePerSingleUnit,
                     unit_selling_price, 
                     unit_discount,
                     promoDiscount,
                     quantity_perPackage,
                     molecule)
  
  rep.int(data, times = length(Ordered_date_date))
  
  extrapolated <- rep.int(FALSE, times = length(Ordered_date_date))
  extrapolated[!Ordered_date_date %in% transactions_price$Ordered_date_date] <- TRUE
  
  quantityToAdd <- transactions_price %>%
    group_by(Ordered_date_date) %>%
    summarise(quantity = sum(quantity_updated))
  
  # quantity <- left_join(data.frame(Ordered_date_date), quantity, by = "Ordered_date_date")$quantity
  # quantity[is.na(quantity)] <- 0
  quantity <- rep.int(0, times = length(Ordered_date_date))
  quantity[Ordered_date_date %in% quantityToAdd$Ordered_date_date] <- quantityToAdd$quantity
  
  # daysNoOrder keeps track of how many days we've been extrapolating the price already.
  daysNoOrder <- getNumbDaysWithoutOrders(extrapolated)
  
  extrapolatedData <- data.table(sku_code_prefix, 
                                 sku_code,
                                 Ordered_date_date, 
                                 Ordered_date_dayOfWeek, 
                                 Ordered_date_yearweek,
                                 quantity,
                                 quantity_perPackage, 
                                 pricePerSingleUnit,
                                 unit_selling_price, 
                                 unit_discount, 
                                 promoDiscount, 
                                 molecule,
                                 extrapolated, 
                                 daysNoOrder,
                                 stringsAsFactors = FALSE)
  
  # If we extrapolate by distributor as well we also have to add copies of distributor_id.
  if(interpolatePerDistributor) {
    distributor_id <- rep_len(transactions_price$distributor_id[1], length.out = length(Ordered_date_date))
    extrapolatedData <- add_column(extrapolatedData, distributor_id, .after = "sku_code")
  }
  # } else {
  #   
  #   distributor_id <- unique(transactions_price$distributor_id)
  #   distributor_columns <- lapply(distributor_ids, function(id) {
  #     if(id == distributor_id) {
  #       return(rep_len(TRUE, length.out = length(Ordered_date_date)))
  #     } else {
  #       return(rep_len(FALSE, length.out = length(Ordered_date_date)))
  #     }
  #   })
  #   names(distributor_columns) <- paste0("distributor_", distributor_ids)
  #   distributor_columns <- fastDoCall("cbind", distributor_columns)
  # }
  
  return(extrapolatedData)
}

#---

############ Days without orders - count ############

getNumbDaysWithoutOrders <- function(daysWithoutOrder) {
  
  daysWithoutOrder <- as.numeric(daysWithoutOrder)
  daysNoOrder <- cumsum(daysWithoutOrder)
  
  if(length(daysWithoutOrder) > 1) {
    
    for(i in 2:length(daysWithoutOrder)) {
      if(isTRUE(daysNoOrder[i] == daysNoOrder[i - 1])) {
        daysNoOrder[i] <- 0
        
        if(i < length(daysWithoutOrder)) {
          daysNoOrder[(i+1):length(daysNoOrder)] <- daysNoOrder[(i+1):length(daysNoOrder)] - daysNoOrder[i - 1]
        }
      }
    }
  }
  
  return(daysNoOrder)
  
  # daysWithoutOrder <- as.numeric(daysWithoutOrder)
  # 
  # if(length(daysWithoutOrder) == 1) {
  #   return(daysWithoutOrder)
  # }
  # 
  # daysOrder_indices <- which(daysWithoutOrder == 0)
  # 
  # diffs <- diff(daysOrder_indices)
  # points <- which(diffs > 1)
  # 
  # daysNoOrder <- rep(0, length(daysWithoutOrder))
  # for(i in seq_along(points)) {
  #   startPoint <- daysOrder_indices[i] + 1
  #   endPoint <- daysOrder_indices[i + 1] - 1
  #   daysNoOrder[startPoint:endPoint] <- 1:(endPoint - startPoint + 1)
  # }
  # 
  # if(daysWithoutOrder[length(daysWithoutOrder)] != 0) {
  #   startPoint <- max(daysOrder_indices) + 1
  #   endPoint <- length(daysWithoutOrder)
  # 
  #   daysNoOrder[startPoint:endPoint] <- 1:(endPoint - startPoint + 1)
  # }
  # 
  # return(daysNoOrder)
  
  return(daysNoOrders)
}

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


######### Lagged demand #########

# This function calculates the cumulated demand of the past for every day of the time horizon present in priceData.

getLagDemand <- function(priceData,
                         aggLevel = "sku_code",
                         lags = c(1, 2, 3, 4)) {
  
  # Creating a suitable suffix for the columns we are going to create
  lagNames_suffix <- ""
  if("sku_code" %in% aggLevel) {
    lagNames_suffix <- paste0(lagNames_suffix, "SkuCode", sep = "")
  } else if("sku_code_prefix" %in% aggLevel) {
    lagNames_suffix <- paste0(lagNames_suffix, "SkuPrefix", sep = "")
  }
  
  if("distributor_id" %in% aggLevel) {
    lagNames_suffix <- paste0(lagNames_suffix, "Distr", sep = "")
  }
  if(any(c("unit_selling_price", "unit_discount") %in% aggLevel)) {
    lagNames_suffix <- paste0(lagNames_suffix, "Price", sep = "")
  } 
  
  groups <- priceData %>%
    group_by(.dots = aggLevel) %>%
    group_by(Ordered_date_yearweek, add = TRUE) %>%
    summarise(demand = sum(demand))
  
  groups <- groups %>%
    group_by(.dots = aggLevel, add = FALSE) %>%
    group_split()
  
  # Just needed for a special case during the lapply-call.
  Ordered_date_yearweek <- sort(unique(priceData$Ordered_date_yearweek))
  
  groups <- lapply(groups, function(group) {
    
    # In case a promotion is present we haven't set the demand to 0 on weeks when there were no orders. 
    # Because of that the demand time series doesn't have the same length as Ordered_date_yearweek.
    # This case needs a special treatment!
    if(!all(Ordered_date_yearweek %in% group$Ordered_date_yearweek)) {
      
      weekAvailable_index <- Ordered_date_yearweek %in% group$Ordered_date_yearweek
      
      demand_extended <- rep.int(0, length(Ordered_date_yearweek))
      demand_extended[weekAvailable_index] <- group$demand
      
      lagDemands <- lagDemand_singleSku(demand_extended,
                                        lags = lags,
                                        lagNames_suffix = lagNames_suffix)
      
      group <- cbind(group[, c("Ordered_date_yearweek", aggLevel)], lagDemands[weekAvailable_index, ])
      
    } else {
      lagDemands <- lagDemand_singleSku(group$demand,
                                        lags = lags,
                                        lagNames_suffix = lagNames_suffix)
      
      group <- cbind(group[, c("Ordered_date_yearweek", aggLevel)], lagDemands)
    }
    
    return(group)
  })
  dataWithLags <- rbindlist(groups)
  
  priceData_lags <- left_join(priceData, dataWithLags, by = c(aggLevel, "Ordered_date_yearweek"))
  
  return(priceData_lags)
}

#---

# Help function to create the desired lags

lagDemand_singleSku <- function(demand,
                                lags = c(1, 2, 3, 4),
                                lagNames_suffix) {
  
  if(max(lags) >= length(demand)) {
    stop("The maximum lag mustn't be bigger than the length of the time horizon.")
  }
  
  demand_lag <- sapply(lags, function(lag) {
    c(rep.int(0, times = lag), demand[1:(length(demand) - lag)])
    
  })
  colnames(demand_lag) <- paste0("demand_lag", lags, "_" ,lagNames_suffix)
  
  cumDemand <- cumsum(demand)
  cumDemand_lag <- sapply(lags, function(lag) {
    c(rep.int(0, times = lag), cumDemand[1:(length(cumDemand) - lag)])
  })
  
  colnames(cumDemand_lag) <- paste0("cumDemand_lag", lags, "_" ,lagNames_suffix)
  
  return(as.data.frame(cbind(demand_lag, cumDemand_lag)))
}

#---

getLagPrices <- function(data,
                         aggLevel = "sku_code",
                         lags = c(1, 2, 3, 4)) {
  
  # Creating a suitable suffix for the columns we are going to create. We have to start off with priceNames_suffix <- ""
  # because if neither sku_code nor sku_code_prefix nor distributor_id is part of the current aggLevel, we don't want
  # to add any suffix. 
  priceNames_suffix <- ""
  if("sku_code" %in% aggLevel) {
    priceNames_suffix <- paste0(priceNames_suffix, "SkuCode", sep = "")
  } else if("sku_code_prefix" %in% aggLevel) {
    priceNames_suffix <- paste0(priceNames_suffix, "SkuPrefix", sep = "")
  }
  
  if("distributor_id" %in% aggLevel) {
    priceNames_suffix <- paste0(priceNames_suffix, "Distr", sep = "")
  } 
  
  pricesGroup <- data %>%
    group_by(.dots = aggLevel) %>%
    group_by(Ordered_date_yearweek, add = TRUE) %>%
    summarise("meanPrice" = mean(pricePerSingleUnit),
              "medianPrice" = median(pricePerSingleUnit),
              "maxPrice" = max(pricePerSingleUnit),
              "minPrice" = min(pricePerSingleUnit))
  
  for(i in (length(aggLevel) + 2): ncol(pricesGroup)) {
    colnames(pricesGroup)[i] <- paste0(colnames(pricesGroup)[i], priceNames_suffix)
  }
  
  data <- left_join(data, pricesGroup, by = c(aggLevel, "Ordered_date_yearweek"))
  
  # Creating lagged price features
  for(i in lags) {
    pricesGroup_lagged <- pricesGroup %>%
      mutate(Ordered_date_yearweek = Ordered_date_yearweek + i)
    
    lagNames_suffix <- paste0("_lag", i)
    
    for(i in (length(aggLevel) + 2): ncol(pricesGroup)) {
      colnames(pricesGroup_lagged)[i] <- paste0(colnames(pricesGroup_lagged)[i], lagNames_suffix)
    }
    
    data <- left_join(data, pricesGroup_lagged, by = c(aggLevel, "Ordered_date_yearweek"))
  }
  
  # First try of creating features that track the price difference of the current price in comparison with
  # past prices.
  # We create 2 type of features here:
  # 1) The first ones track the difference of the current price with the prices of the last 4 weeks
  # 2) The others track the difference of the current price with the prices of the whole time horizon
  
  # PROBLEMS: 
  # 1) We obviously can't create features tracking the price history for the first weeks depending on
  # which time horizon we choose for the lags (usually 4 weeks). The same problem applies for the lagged 
  # prices above. The easiest solution is simply to choose a bigger time horizon of the whole dataset and deleting 
  # the resulting NA-weeks.
  # 2) In case we work with either none-interpolated data or have a promotion at hand it can happen that there
  # hasn't been any observation during last 4 weeks that we could use for creating a reasonable price difference
  # feature. It is quite hard to figure out a smooth solution for the problem at hand:
  # a) One option could be to only create the price difference features on the product level (sku_code_prefix)
  # instead of the packaging or even distributor level. This has the advantage, that the promotions shouldn't cause
  # any problems. This doesn't fix the problem for none-interpolated data, though.
  # b) It probably makes most sense to create a second function 'getAdditionalPriceData_noInterpol', which
  # is used for none-interpolated data. 
  # c) A completely different solution could be to set the price-ratio-features to 1 when there is no lagged
  # price available for comparison. This would have the big advantage that it could be used on a package level.
  # This could have one major downside, though: Many promotions would receive the price ratio 1 on package level
  # while still receiving huge amounts of orders. Maybe this isn't such a big downside like it first seems, who knows.
  # It would probably only make sense to use this for interpolated data. 
  
  # For now we stick to solution (a) + (b). All we have to do for this solution to work smoothly is to
  # set the price-ratio-features to NA when there is no week available that is smaller than the current 
  # considered week.
  if("sku_code_prefix" %in% aggLevel) {
    
    
    dataGroups <- data %>%
      group_by(.dots = aggLevel) %>%
      group_split()
    
    data_list <- lapply(dataGroups, function(group) {
      
      group_mod_list <- lapply(unique(group$Ordered_date_yearweek), function(week) {
        
        if(all(group$Ordered_date_yearweek >= week)) {
          group_mod <- group %>%
            filter(Ordered_date_yearweek == week) %>%
            mutate(priceRatio_median_allTime = NA,
                   priceRatio_min_allTime = NA,
                   priceRatio_median_last = NA,
                   priceRatio_min_last = NA)
          
        } else {
          
          prices_allTime <- group %>%
            filter(Ordered_date_yearweek < week) %>%
            summarise(priceMedian = median(pricePerSingleUnit),
                      priceMin = min(pricePerSingleUnit))
          
          prices_lastWeeks <- group %>%
            filter(Ordered_date_yearweek < week & Ordered_date_yearweek >= week - max(lags)) %>%
            summarise(priceMedian = median(pricePerSingleUnit),
                      priceMin = min(pricePerSingleUnit))
          
          group_mod <- group %>%
            filter(Ordered_date_yearweek == week) %>%
            mutate(priceRatio_median_allTime = pricePerSingleUnit / prices_allTime$priceMedian,
                   priceRatio_min_allTime = pricePerSingleUnit / prices_allTime$priceMin,
                   priceRatio_median_last = pricePerSingleUnit / prices_lastWeeks$priceMedian,
                   priceRatio_min_last = pricePerSingleUnit / prices_lastWeeks$priceMin)
        }
        
        group_mod <- group_mod %>%
          rename_at(vars(matches("last")), .funs = ~ paste0(., max(lags), "Weeks")) %>%
          rename_at(vars(matches("Ratio")), .funs = ~ paste0(., "_", priceNames_suffix))
        
        return(group_mod)
        
      })
      group_mod <- rbindlist(group_mod_list)
      return(group_mod)
      
    })
    data <- rbindlist(data_list)
    
  }
  
  return(data)
}

#---

##### Holidays #####

getHolidays <- function(priceData) {
  
  
  days <- seq(from = as.Date("2018-01-01"), to = as.Date("2020-12-31"), by = 1)
  
  # The following steps are only necessary to get Ordered_date_yearweek for the whole time horizon.
  Ordered_date_date <- days
  Ordered_date_year <- year(Ordered_date_date)
  
  Ordered_date_yearweek <- (Ordered_date_year == 2018)*0 + 
    (Ordered_date_year == 2019)*52 + (Ordered_date_year == 2020)*104 + isoweek(Ordered_date_date)
  
  # The date '2019-12-30' for example has the isoweek 1 and would therefor receive the Ordered_date_yearweek 
  # value of 53 despite the fact that '2019-12-30' actually belongs to week 105.
  # The following 2 lines of code keep track of that.
  endOfYearCorrection_index <- month(Ordered_date_date) == 12 & isoweek(Ordered_date_date) == 1
  Ordered_date_yearweek[endOfYearCorrection_index] <- Ordered_date_yearweek[endOfYearCorrection_index] + 52
  
  # We create the categorial feature 'holiday', which is either TRUE or FALSE depending on whether there is a holiday
  # present on the current day or not.
  holiday <- rep.int(FALSE, length(days))
  dailyHolidayData <- data.frame(Ordered_date_date, Ordered_date_yearweek, holiday)
  
  # Holidays of year 2018
  dailyHolidayData <- dailyHolidayData %>%
    mutate(holiday = replace(holiday, Ordered_date_date == "2018-01-01", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2018-02-16", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2018-03-17", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2018-03-30", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2018-04-14", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2018-05-01", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2018-05-10", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2018-05-29", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2018-06-01", TRUE),
           holiday = replace(holiday, Ordered_date_date %in% seq(as.Date("2018-06-11"), as.Date("2018-06-16"), 1), TRUE),
           holiday = replace(holiday, Ordered_date_date %in% seq(as.Date("2018-06-18"), as.Date("2018-06-20"), 1), TRUE),
           holiday = replace(holiday, Ordered_date_date == "2018-06-27", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2018-08-17", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2018-08-22", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2018-09-11", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2018-11-20", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2018-12-24", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2018-12-25", TRUE))
  
  # Holidays of year 2019
  dailyHolidayData <- dailyHolidayData %>%
    mutate(holiday = replace(holiday, Ordered_date_date == "2019-01-01", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2019-02-05", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2019-03-07", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2019-04-03", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2019-04-19", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2019-05-01", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2019-05-19", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2019-05-30", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2019-06-01", TRUE),
           holiday = replace(holiday, Ordered_date_date %in% seq(as.Date("2019-06-03"), as.Date("2019-06-07"), 1), TRUE),
           holiday = replace(holiday, Ordered_date_date == "2019-08-11", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2019-08-17", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2019-09-01", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2019-11-09", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2019-12-24", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2019-12-25", TRUE))
  
  # Holidays of year 2020
  dailyHolidayData <- dailyHolidayData %>%
    mutate(holiday = replace(holiday, Ordered_date_date == "2020-01-01", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2020-01-25", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2020-03-22", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2020-03-25", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2020-04-10", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2020-05-01", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2020-05-07", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2020-05-21", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2020-05-22", TRUE),
           holiday = replace(holiday, Ordered_date_date %in% seq(as.Date("2020-05-24"), as.Date("2020-05-25"), 1), TRUE),
           holiday = replace(holiday, Ordered_date_date == "2020-06-01", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2020-07-31", TRUE),
           holiday = replace(holiday, Ordered_date_date == "2020-08-17", TRUE),
           holiday = replace(holiday, Ordered_date_date %in% seq(as.Date("2020-08-20"), as.Date("2020-08-21"), 1), TRUE),
           holiday = replace(holiday, Ordered_date_date %in% seq(as.Date("2020-10-28"), as.Date("2020-10-30"), 1), TRUE),
           holiday = replace(holiday, Ordered_date_date %in% seq(as.Date("2020-12-24"), as.Date("2020-12-25"), 1), TRUE),
           holiday = replace(holiday, Ordered_date_date %in% seq(as.Date("2020-08-28"), as.Date("2020-08-31"), 1), TRUE))
  
  weeklyHolidayData <- dailyHolidayData %>%
    group_by(Ordered_date_yearweek) %>%
    summarise(holidayInWeek = any(holiday),
              nHolidaysInWeek = sum(holiday)) %>%
    as.data.frame()
  
  # We want to add teh number of holidays of the previous week. The number of holidays of the previous week of week 1
  # has to be added by hand and is 1 (new Year's Eve).
  nHolidaysPrevWeek <- c(1, weeklyHolidayData$nHolidaysInWeek[-nrow(weeklyHolidayData)])
  
  # We want to add the number of holidays of the following week. The number of holidays of the following week of the last
  # week has to be added by hand and is 1 (new Year's Day).
  nHolidaysNextWeek <- c(weeklyHolidayData$nHolidaysInWeek[-1], 1)
  
  weeklyHolidayData <- cbind(weeklyHolidayData, nHolidaysPrevWeek, nHolidaysNextWeek)
  
  priceData <- left_join(priceData, weeklyHolidayData, by = "Ordered_date_yearweek")
  return(priceData)
}


getMoleculeFeature <- function(data) {
  
  molData <- data %>%
    group_by(sku_code) %>%
    slice(1) %>%
    select(sku_code, molecule)
  
  molList <- lapply(1:nrow(molData), function(i) {
    molecule <- molData[i, "molecule"]
    molSplit <- str_split(molecule, pattern = ",\\s|;\\s|;", simplify = TRUE)
    molSplit <- toupper(molSplit)
    
    emptyIndex <- !grepl(molSplit, pattern = "[[:alnum:]]")
    molSplit <- molSplit[!emptyIndex]
    
    if(length(molSplit) == 0) {
      return(NULL)
    }
    
    molSplit <- gsub(molSplit, pattern = "-|[+]", replacement = " ")
    
    isolatedDigits_index <- grepl(molSplit, pattern = "\\s[[:digit:]]+")
    
    if(any(isolatedDigits_index)) {
      molSplit[isolatedDigits_index] <- str_split(molSplit[isolatedDigits_index],
                                                  pattern = "\\s[[:digit:]]+",
                                                  simplify = TRUE)[, 1]
    }
    
    molSplit_mod <- paste0(" ", molSplit, " ")
    vitaminIndex <- grepl(molSplit_mod, pattern = "\\s[ABCDEK]{1}[0-9]*\\s")
    # molSplit_mod[vitaminIndex] <- str_extract(molSplit_mod[vitaminIndex], pattern = "\\s[A-E]{1}[0-9]*\\s")
    molSplit[vitaminIndex] <- "VITAMIN"
    
    molSplit <- unlist(str_split(molSplit, pattern = " ", simplify = FALSE))
    
    # molSplit <- unique(molSplit)
    
    return(molSplit)
  })
  names(molList) <- molData$sku_code
  mols <- unique(unlist(molList))
  mols <- mols[!is.na(mols)]
  
  molsCombs <- data.frame("mol" = character(), 
                          "molsFound" = character(),
                          stringsAsFactors = FALSE)
  
  for(i in seq_along(mols)) {
    molsToCompare <- mols[-(1:i)]
    mol <- mols[i]
    
    if(mol %in% molsCombs$molsFound) {
      next
    }
    
    dist <- adist(mol, molsToCompare)
    cutOff <- (nchar(molsToCompare) + nchar(mol)) / 5
    
    molsFound <- molsToCompare[dist <= cutOff]
    
    if(length(molsFound) > 0) {
      
      for(molFound in molsFound) {
        if(!molFound %in% molsCombs$molsFound) {
          molsCombs <- rbind(molsCombs, data.frame("mol" = mol, 
                                                   "molsFound" = molFound,
                                                   stringsAsFactors = FALSE))
        }
      }
    } 
  }
  
  molList_mod <- lapply(molList, function(molSku) {
    
    for(i in length(molSku)) {
      molCheck <- molsCombs$molsFound == molSku[i]
      
      if(any(molCheck)) {
        molSku[i] <- molsCombs$mol[molCheck]
        
        if(sum(molCheck) > 1) {
          browser()
        }
      }
    }
    return(molSku)
  })
  
  # count <- 0
  # molList_mod <- for(j in seq_along(molList)) {
  #   
  #   molSku <- molList[[j]]
  #   
  #   for(i in length(molSku)) {
  #     
  #     #browser()
  #     molCheck <- molsCombs$molsFound == molSku[i]
  #     if(any(molCheck)) {
  #       mols[i] <- molsCombs$mol[molCheck]
  #       count <- count + 1
  #       
  #       if(sum(molCheck) > 1) {
  #         browser()
  #       }
  #     }
  #   }
  # }
  # count
  
  
  mols <- unique(unlist(molList_mod))
  
  molCheck <- sapply(mols, function(mol) {
    
    molCheck <- vapply(molList_mod, FUN.VALUE = logical(1), function(molsSku) {
      mol %in% molsSku
    })
    return(sum(molCheck))
  })
  
  molsForCols <- names(which(molCheck / length(molList_mod) >= 0.01))
  
  molFeatures_list <- lapply(molsForCols, function(mol) {
    molCheck <- vapply(molList_mod, FUN.VALUE = logical(1), function(molsSku) {
      mol %in% molsSku
    })
    molFeature <- data.frame(molCheck, stringsAsFactors = FALSE)
    colnames(molFeature)[1] <- mol
    return(molFeature)
  })
  molFeatures <- fastDoCall("cbind", molFeatures_list)
  molFeatures <- cbind(data.frame("sku_code" = names(molList_mod),
                                  stringsAsFactors = FALSE),
                       molFeatures)
  
  left_join(data, molFeatures, by = "sku_code")
}

#---

##### Distributor Features #####

# This function is only called when 'interpolatePerDistributor == FALSE'.
# REMARK: When this function is called by 'getTrainingMain', the 'transactions'-object has already been filtered
# to only contain data from the user specified time horizon.

getDistributorFeatures <- function(transactions,
                                   data) {
  
  distrData <- transactions %>%
    group_by(sku_code) %>%
    summarise(numbOfDistributors = uniqueN(distributor_id))
  
  data <- left_join(data, distrData, by = "sku_code")
  
  return(data)
}

#---

##### Stockout-Data ######

getStockoutData <- function(transactions) {
  
  allWeeks <- sort(unique(transactions$Ordered_date_yearweek))
  
  comments_stockOut <- grepl(transactions$comment, pattern = "STOCK|STCOK")
  itemComment_stockOut <- grepl(transactions$item_comment, pattern = "STOCK")
  stockOut_index <- comments_stockOut | itemComment_stockOut
  
  stockOut <- rep(FALSE, nrow(transactions))
  stockOut[stockOut_index] <- TRUE
  transactions <- add_column(transactions, stockOut)
  
  stockOutDemand <- transactions %>%
    filter(stockOut) %>%
    filter(sku_code_prefix == "103141") %>%
    group_by(sku_code_prefix, Ordered_date_yearweek) %>%
    mutate(demand = quantity_perPackage * quantity) %>%
    summarise(demandStockOut = sum(demand)) %>%
    as.data.frame()
  
  missingWeeks <- setdiff(allWeeks, stockOutDemand$Ordered_date_yearweek)
  missingWeeks <- c(missingWeeks, 125:134)
  dataToAdd <- data.frame("sku_code_prefix" = "103141", 
                          "Ordered_date_yearweek" = missingWeeks,
                          "demandStockOut" = 0,
                          stringsAsFactors = FALSE)
  
  stockOutDemand <- stockOutDemand %>%
    bind_rows(dataToAdd) %>%
    arrange(Ordered_date_yearweek)
  
  stockOutDemand <- getMonthPartition(stockOutDemand)
  
  return(stockOutDemand)
}

#---

##### Rescale Data #####

# This step is only optional. The function 'getRescaledData' transforms the given demand data (and also the lags)
# 

#---

##### Name List ######

# This part ist not used for creating the training data, it is only needed for plotting purposes later on. I simply
# don't know any better place to store this code.

productNames <- transactions %>%
  group_by(sku_code_prefix) %>%
  mutate(nameLength = nchar(name)) %>%
  arrange(nameLength) %>%
  slice(1) %>%
  select(sku_code_prefix, name)

saveRDS(productNames, "SavedData/productNames.rds")



