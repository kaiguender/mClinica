getwd()
source("getTrainingData.R", chdir = TRUE)

library(testthat)
library(tidyverse)
library(sjmisc)
library(Gmisc)
library(fasttime)
library(doParallel)
library(foreach)
library(lubridate)
library(data.table)
library(batman)

test_that("priceData_case1", {
  transactions <- readRDS("transactions_clean.rds")
  
  # Very simple case with only one observation
  transactions_test <- transactions[1, ]
  priceData <- getPriceDataPerTransactions(transactions_test,
                                           lastDay = transactions_test$Ordered_date_date + 3)
  
  columnsSame <- c("sku_code", "sku_code_prefix", "molecule", "quantity_perPackage", "hasPromotion", "promoDiscount")
  
  for(column in columnsSame) {
    expect_equal(length(unique(priceData[, column])), 1)
    expect_equal(unique(priceData[, column]), transactions_test[[column]])
  }
  
  expect_equal(length(unique(priceData[["unit_selling_price"]])), 1)
  expect_equal(unique(priceData[["unit_selling_price"]]), transactions_test[["unit_selling_price_updated"]])
  
  expect_equal(length(unique(priceData[["unit_selling_price"]])), 1)
  expect_equal(unique(priceData[["unit_discount"]]), transactions_test[["unit_discount_updated"]])
})

test_that("priceData_case2_1", {
  
  # We construct another basic case that already covers much of the functionality of the function 'getPriceDataPerTransactions'.
  # obs1_1, obs1_2, obs1_3 are copies of each other, but the distributor of obs1_2 and the Ordered_date_date
  # of obs1_3 has been modified
  obs1_1 <- transactions[1, ]
  
  obs1_2 <- obs1_1
  obs1_2[["distributor_id"]] <- 8
  
  obs1_3 <- obs1_1
  obs1_3[["Ordered_date_date"]] <- obs1_1[["Ordered_date_date"]] + 3
  
  obs2 <- transactions[2, ]
  
  transactions_test <- rbind(obs1_1, obs1_2, obs1_3, obs2)
  priceData <- getPriceDataPerTransactions(transactions_test,
                                           topPercentSkus = 1,
                                           extrapolatePerDistributor = FALSE)
  
  # There should be exactly 2 sku_codes in the price data
  expect_equal(length(unique(priceData$sku_code)), 2)
  
  # There should be exactly 2 combinations of distributor and sku_code in the price Data
  groups <- priceData %>%
    group_by(sku_code) %>%
    group_split
  
  expect_equal(length(groups), 2)
  
  # For every sku_code there should be 4 observations as the time horizon is given by '2018-06-29 - 2018-07-02'.
  # For every combination of sku_code and distributor there should be 4 observations as the time horizon is 
  # given by '2018-06-29 - 2018-07-02'.
  timeHorizon <- seq(from = as.Date("2018-06-29"),
                     to = as.Date("2018-07-02"),
                     by = 1)
  
  for(group in groups) {
    expect_equal(nrow(group), 4)
    expect_equal(group$Ordered_date_date, timeHorizon)
  }
  
  # 'extrapolated == TRUE' should be identical to 'quantitiy == 0'
  expect_equal(priceData$extrapolated, priceData$quantity == 0)

  # There should be 3 none-extrapolated rows
  expect_equal(sum(!priceData$extrapolated), 3)
  
  # We check if the none-extrapolated rows are given by the expected observations
  priceData1 <- priceData %>%
    filter(sku_code == obs1_1$sku_code)
  
  priceData2 <- priceData %>%
    filter(sku_code == obs2$sku_code)
  
  realObs1 <- priceData1 %>%
    filter(Ordered_date_date %in% c(obs1_1$Ordered_date_date, obs1_3$Ordered_date_date))
  expect_false(unique(realObs1$extrapolated))
  
  realObs2 <- priceData2 %>%
    filter(Ordered_date_date %in% c(obs2$Ordered_date_date))
  expect_false(unique(realObs2$extrapolated))
  
  # We check if every static column is indeed constant for a fixed sku_code
  columnsSame <- c("sku_code_prefix", "molecule", "quantity_perPackage", "hasPromotion", "promoDiscount")
  
  for(column in columnsSame) {
    expect_equal(length(unique(priceData1[, column])), 1)
    expect_equal(unique(priceData1[, column]), obs1_1[[column]][1])
    
    expect_equal(length(unique(priceData2[, column])), 1)
    expect_equal(unique(priceData2[, column]), obs2[[column]][1])
  }
  
  # The price was also static in our case.
  expect_equal(length(unique(priceData1[["unit_selling_price"]])), 1)
  expect_equal(unique(priceData1[["unit_selling_price"]]), obs1_1[["unit_selling_price_updated"]][1])
  
  expect_equal(length(unique(priceData2[["unit_selling_price"]])), 1)
  expect_equal(unique(priceData2[["unit_selling_price"]]), obs2[["unit_selling_price_updated"]][1])
  
  # The discount was also static in our case
  expect_equal(length(unique(priceData1[["unit_selling_price"]])), 1)
  expect_equal(unique(priceData1[["unit_discount"]]), obs1_1[["unit_discount_updated"]][1])
  
  expect_equal(length(unique(priceData2[["unit_selling_price"]])), 1)
  expect_equal(unique(priceData2[["unit_discount"]]), obs2[["unit_discount_updated"]][1])
  
})

test_that("priceData_case2_2", {
  
  # We construct another basic case that already covers much of the functionality of the function 'getPriceDataPerTransactions'.
  # obs1_1, obs1_2, obs1_3 are copies of each other, but the distributor of obs1_2 and the Ordered_date_date
  # of obs1_3 has been modified
  obs1_1 <- transactions[1, ]
  
  obs1_2 <- obs1_1
  obs1_2[["distributor_id"]] <- 8
  
  obs1_3 <- obs1_1
  obs1_3[["Ordered_date_date"]] <- obs1_1[["Ordered_date_date"]] + 3
  
  obs2 <- transactions[2, ]
  
  # Now we set 'extrapolatePerDistributor = TRUE', which should lead to a different extrapolation result than above.
  priceData <- getPriceDataPerTransactions(transactions_test,
                                           topPercentSkus = 1,
                                           extrapolatePerDistributor = TRUE)
  
  # There should be exactly 2 sku_codes in the price data
  expect_equal(length(unique(priceData$sku_code)), 2)
  
  # There should be exactly 3 combinations of distributor and sku_code in the price Data
  groups <- priceData %>%
    group_by(sku_code, distributor_id) %>%
    group_split
  
  expect_equal(length(groups), 3)
  
  # For every combination of sku_code and distributor there should be 4 observations as the time horizon is 
  # given by '2018-06-29 - 2018-07-02'.
  timeHorizon <- seq(from = as.Date("2018-06-29"),
                     to = as.Date("2018-07-02"),
                     by = 1)
  
  for(group in groups) {
    expect_equal(nrow(group), 4)
    expect_equal(group$Ordered_date_date, timeHorizon)
  }
  
  # 'extrapolated == TRUE' should be identical to 'quantitiy == 0'
  expect_equal(priceData$extrapolated, priceData$quantity == 0)
  
  # There should be 3 none-extrapolated rows
  expect_equal(sum(!priceData$extrapolated), 4)
  
  # We check if the none-extrapolated rows are given by the expected observations
  priceData1_1 <- priceData %>%
    filter(sku_code == obs1_1$sku_code, distributor_id == obs1_1$distributor_id)
  
  priceData1_2 <- priceData %>%
    filter(sku_code == obs1_1$sku_code, distributor_id == obs1_2$distributor_id)
  
  priceData2 <- priceData %>%
    filter(sku_code == obs2$sku_code)
  
  realObs1_1 <- priceData1_1 %>%
    filter(Ordered_date_date %in% c(obs1_1$Ordered_date_date, obs1_3$Ordered_date_date))
  expect_false(unique(realObs1$extrapolated))
  
  realObs1_1 <- priceData1_1 %>%
    filter(Ordered_date_date %in% c(obs1_2$Ordered_date_date))
  expect_false(unique(realObs1$extrapolated))
  
  realObs2 <- priceData2 %>%
    filter(Ordered_date_date %in% c(obs2$Ordered_date_date))
  expect_false(unique(realObs2$extrapolated))
  
  # We check if every static column is indeed constant for a fixed sku_code
  columnsSame <- c("sku_code_prefix", "molecule", "quantity_perPackage", "hasPromotion", "promoDiscount")
  
  for(column in columnsSame) {
    expect_equal(length(unique(priceData1_1[, column])), 1)
    expect_equal(unique(priceData1_1[, column]), obs1_1[[column]][1])
    
    expect_equal(length(unique(priceData1_2[, column])), 1)
    expect_equal(unique(priceData1_2[, column]), obs1_2[[column]][1])
    
    expect_equal(length(unique(priceData2[, column])), 1)
    expect_equal(unique(priceData2[, column]), obs2[[column]][1])
  }
  
  # The price was also static in our case.
  expect_equal(length(unique(priceData1_1[["unit_selling_price"]])), 1)
  expect_equal(unique(priceData1_1[["unit_selling_price"]]), obs1_1[["unit_selling_price_updated"]][1])
  
  expect_equal(length(unique(priceData1_2[["unit_selling_price"]])), 1)
  expect_equal(unique(priceData1_2[["unit_selling_price"]]), obs1_2[["unit_selling_price_updated"]][1])
  
  expect_equal(length(unique(priceData2[["unit_selling_price"]])), 1)
  expect_equal(unique(priceData2[["unit_selling_price"]]), obs2[["unit_selling_price_updated"]][1])
  
  # The discount was also static in our case
  expect_equal(length(unique(priceData1[["unit_selling_price"]])), 1)
  expect_equal(unique(priceData1[["unit_discount"]]), obs1_1[["unit_discount_updated"]][1])
  
  expect_equal(length(unique(priceData1_2[["unit_selling_price"]])), 1)
  expect_equal(unique(priceData1_2[["unit_discount"]]), obs1_2[["unit_discount_updated"]][1])
  
  expect_equal(length(unique(priceData2[["unit_selling_price"]])), 1)
  expect_equal(unique(priceData2[["unit_discount"]]), obs2[["unit_discount_updated"]][1])
  
  
})


test_that("priceData_case3", {
  
  # We construct another basic case that already covers much of the functionality of the function 'getPriceDataPerTransactions'.
  # obs1_1, obs1_2, obs1_3 are copies of each other, but the distributor and date of obs1_2, the date of obs1_3, 
  # the price of obs1_4 and the price and date of obs1_5 has been modified.
  obs1_1 <- transactions[1, ]
  
  obs1_2 <- obs1_1
  obs1_2[["unit_selling_price_updated"]] <- obs1_1[["unit_discount"]] + 0.10
  
  obs1_3 <- obs1_1
  obs1_3[["distributor_id"]] <- 8
  obs1_3[["Ordered_date_date"]] <- obs1_3$Ordered_date_date + 2
  
  obs1_4 <- obs1_1
  obs1_4[["Ordered_date_date"]] <- obs1_1[["Ordered_date_date"]] + 5
  obs1_4[["unit_selling_price_updated"]] <- obs1_1[["unit_selling_price_updated"]] - 20000
  
  obs1_5 <- obs1_1
  obs1_5[["Ordered_date_date"]] <- obs1_1[["Ordered_date_date"]] + 8
  
  
  transactions_test <- rbind(obs1_1, obs1_2, obs1_3, obs1_4, obs1_5)
  priceData <- getPriceDataPerTransactions(transactions_test,
                                           topPercentSkus = 1,
                                           extrapolatePerDistributor = FALSE)
  
  # There should be exactly 1 sku_code in the price data
  expect_equal(length(unique(priceData$sku_code)), 1)
  
  # The time horizon of the data should be given by '2018-06-29 - 2018-07-02' and should already be ordered.
  timeHorizon <- seq(from = as.Date("2018-06-29"),
                     to = as.Date("2018-07-07"),
                     by = 1)
  
  expect_equal(unique(priceData$Ordered_date_date), timeHorizon)
  
  # There should be 10 obervations instead of 9 (length of time horizon), because there have been 2 different prices on 2018-06-29.
  expect_equal(nrow(priceData), length(timeHorizon) + 1)
  
  # There should be 2 obervations on 2018-06-29
  numbObs <- priceData %>%
    filter(Ordered_date_date == "2018-06-29") %>%
    nrow()
  expect_equal(numbObs, 2)
  
  # 'extrapolated == TRUE' should be identical to 'quantitiy == 0'
  expect_equal(priceData$extrapolated, priceData$quantity == 0)
  
  # There should be 3 different price groups.
  groups <- priceData %>%
    group_by(sku_code, unit_selling_price, unit_discount) %>%
    group_split()
  expect_equal(length(groups), 3)
  
  # There should be 5 none-extrapolated rows
  expect_equal(sum(!priceData$extrapolated), 5)
  
  # The observations on the days 
  timeHorizon_noObs <- timeHorizon[-c(1, 3, 6, 9)]
  
  priceData_noObs <- priceData %>%
    filter(Ordered_date_date %in% timeHorizon_noObs)
  expect_true(unique(priceData_noObs$extrapolated))
  
  # We check if every static column is indeed constant for a fixed sku_code
  columnsSame <- c("sku_code_prefix", "molecule", "quantity_perPackage", "hasPromotion", "promoDiscount")
  
  for(column in columnsSame) {
    expect_equal(length(unique(priceData[, column])), 1)
    expect_equal(unique(priceData[, column]), unique(transactions_test[[column]]))
  }
})


test_that("priceData_case_promotions", {
  
  # We construct another basic case that already covers much of the functionality of the function 'getPriceDataPerTransactions'.
  # obs1_1, obs1_2, obs1_3 are copies of each other, but the distributor and date of obs1_2, the date of obs1_3, 
  # the price of obs1_4 and the price and date of obs1_5 has been modified.
  obs1_1 <- transactions[1, ]
  
  obs1_2 <- obs1_1
  obs1_2[["sku_code"]] <- "ID121884-10"
  obs1_2[["hasPromotion"]] <- TRUE
  obs1_2[["promoDiscount"]] <- 0.2
  obs1_2[["Ordered_date_date"]] <- obs1_1$Ordered_date_date + 2  
  
  obs1_3 <- obs1_1
  obs1_3[["Ordered_date_date"]] <- obs1_3$Ordered_date_date + 5
  
  transactions_test <- rbind(obs1_1, obs1_2, obs1_3)
  priceData <- getPriceDataPerTransactions(transactions_test,
                                           topPercentSkus = 1,
                                           extrapolatePerDistributor = FALSE)
  
  # There should be exactly 2 sku_codes in the price data
  expect_equal(length(unique(priceData$sku_code)), 2)
  
  # The time horizon of the data should be given by '2018-06-29 - 2018-07-04' and should already be ordered.
  timeHorizon <- seq(from = as.Date("2018-06-29"),
                     to = as.Date("2018-07-04"),
                     by = 1)
  
  expect_equal(unique(priceData$Ordered_date_date), timeHorizon)
  
  # There should be 7 obervations instead of 6 (length of time horizon), because there has been one observation with a promotion
  # on 2018-07-01. This observation shouldn't get extrapolated.
  expect_equal(nrow(priceData), length(timeHorizon) + 1)
  
  # There should be 2 obervations on 2018-07-01
  numbObs <- priceData %>%
    filter(Ordered_date_date == "2018-07-01") %>%
    nrow()
  expect_equal(numbObs, 2)
  
  # 'extrapolated == TRUE' should be identical to 'quantitiy == 0'
  expect_equal(priceData$extrapolated, priceData$quantity == 0)
  
  # There should be 2 different price groups.
  groups <- priceData %>%
    group_by(sku_code, unit_selling_price, unit_discount) %>%
    group_split()
  expect_equal(length(groups), 2)
  
  # There should be 3 none-extrapolated rows
  expect_equal(sum(!priceData$extrapolated), 3)
  
  # The observations on the days 
  timeHorizon_noObs <- timeHorizon[-c(1, 3, 6)]
  
  priceData_noObs <- priceData %>%
    filter(Ordered_date_date %in% timeHorizon_noObs)
  expect_true(unique(priceData_noObs$extrapolated))
  
  # We check if every static column is indeed constant for a fixed sku_code
  columnsSame <- c("sku_code_prefix", "molecule", "quantity_perPackage")
  
  for(column in columnsSame) {
    expect_equal(length(unique(priceData[, column])), 1)
    expect_equal(unique(priceData[, column]), unique(transactions_test[[column]]))
  }
})


test_that("trainingData_distributors", {
  
  priceData <- getPriceDataPerTransactions(transactions[60000:61000, ],
                                           extrapolatePerDistributor = TRUE)
  
  getTrainingData_distributors(priceData, noExtrapolating = FALSE)
})


test_that("getTopSkus_skuCode", {
  sku_codes <- c(rep.int("ID1-1", 4),
                 rep.int("ID1-2", 3),
                 rep.int("ID2-1", 5))
  sku_codes_prefix <- str_split(sku_codes, pattern = "ID|-", 
                                 simplify = TRUE)[, 2]
  
  transactions <- data.frame("sku_code" = sku_codes,
                             "sku_code_prefix" = sku_codes_prefix,
                             stringsAsFactors = FALSE)
  
  topSkus <- getTopSkus(transactions = transactions, 
                        topPercentSkus = 0.1, 
                        rankingByPrefix = FALSE)
  expect_equal(topSkus, "ID2-1")
  
  topSkus <- getTopSkus(transactions = transactions, 
                        topPercentSkus = 0.1, 
                        rankingByPrefix = TRUE)
  expect_equal(topSkus, c("ID1-1", "ID1-2"))
})


test_that("cumulated_demand", {
  priceData <- getPriceDataPerTransactions(transactions[60000:65000, ])
  
  priceData <- priceData %>%
    filter(sku_code == unique(priceData$sku_code)[1])
  
  groupingCols <- c("sku_code", 
                    "distributor_id")
  
  priceData_agg <- (priceData %>%
    group_by(.dots = groupingCols) %>%
    group_split())[[1]]
  
  
  priceData_agg
  
  test <- getAdditionalPriceData(priceData_agg,
                       aggLevel = c("sku_code",
                                    "distributor_id"))
  test
  
  as.data.frame(getTrainingData_distributors(priceData))[80:86, ]

})


test_that("getExtrapolatedData", {
  transactions <- readRDS("transactions_clean.rds")
  
  # Simple Test with just one observation
  transactions_test <- transactions[1, ]
  transactions_test$Ordered_date_date <- as.Date("2020-01-20")
  
  extraData <- getExtrapolatedData(transactions_test,
                                   date_start = as.Date("2020-01-18"),
                                   date_end = as.Date("2020-01-23"))
  
  # The entries of the following columns have to be constant
  columnsIdentical <- c("sku_code_prefix", "sku_code", "distributor_id", "quantity_perPackage", "pricePerSingleUnit",
                        "unit_selling_price", "unit_discount", "hasPromotion", "promoDiscount", "molecule")
  
  for(column in columnsIdentical) {
  expect_equal(length(unique(extraData[[column]])), 1)
  }
  
  # The time horizon has to be identical to range given by date_start and date_end.
  timeHorizon <- seq(from = as.Date("2020-01-18"), to = as.Date("2020-01-23"), by = 1)
  expect_equal(extraData$Ordered_date_date, timeHorizon)
  
  # Quantity being different from 0 for one day implies that there has been an order on that day. Therefor 'daysNoOrder'
  # has to be 0 for that day.
  expect_equal(extraData$quantity != 0, extraData$daysNoOrder == 0)
  
  
  # More complex test with one combination of sku_code, distributor and price.
  sku_code <- transactions$sku_code[10000]
  distributor_id <- transactions$distributor_id[10000]
  unit_selling_price <- transactions$unit_selling_price_updated[10000]
  unit_discount <- transactions$unit_discount_updated[10000]
  
  transactions_test <- transactions[transactions$sku_code == sku_code &
                                      transactions$distributor_id == distributor_id &
                                      transactions$unit_selling_price == unit_selling_price &
                                      transactions$unit_discount == unit_discount, ]
  
  extraData <- getExtrapolatedData(transactions_test,
                                   date_end = max(transactions_test$Ordered_date_date))
  
  columnsIdentical <- c("sku_code_prefix", "sku_code", "distributor_id", "quantity_perPackage", "pricePerSingleUnit",
                        "unit_selling_price", "unit_discount", "hasPromotion", "promoDiscount", "molecule")
  
  for(column in columnsIdentical) {
    expect_equal(length(unique(extraData[[column]])), 1)
  }
  
  timeHorizon <- seq(from = min(transactions_test$Ordered_date_date), to = max(transactions_test$Ordered_date_date), by = 1)
  expect_equal(extraData$Ordered_date_date, timeHorizon)
  
  expect_equal(extraData$quantity != 0, extraData$daysNoOrder == 0)
  
  # Every quantity should be sum of all quantities ordered on a specific day.
  quantities_expected <- transactions_test %>%
    group_by(Ordered_date_date) %>%
    summarise("quantity" = sum(quantity))
  quantities_expected <- quantities_expected$quantity
  
  expect_equal(extraData$quantity[extraData$quantity != 0], quantities_expected)
})


test_that("getNumbDaysWithoutOrders", {
  pattern <- c(0, 1, 1, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 1)
  result_expected <- c(0, 1, 2, 3, 4, 0, 1, 2, 0, 1, 0, 1, 2, 3)
  expect_equal(getNumbDaysWithoutOrders(pattern), result_expected)
  
  pattern <- c(0, 1, 0)
  result_expected <- c(0, 1, 0)
  expect_equal(getNumbDaysWithoutOrders(pattern), result_expected)
  
  pattern <- c(0)
  result_expected <- c(0)
  expect_equal(getNumbDaysWithoutOrders(pattern), result_expected)
  
  pattern <- c(1)
  result_expected <- c(1)
  expect_equal(getNumbDaysWithoutOrders(pattern), result_expected)
  
  pattern <- c(1, 0, 0, 1, 0, 1, 1, 1, 0)
  result_expected <- c(1, 0, 0, 1, 0, 1, 2, 3, 0)
  expect_equal(getNumbDaysWithoutOrders(pattern), result_expected)
  
  pattern <- c(1, 1, 1, 0)
  result_expected <- c(1, 2, 3, 0)
  expect_equal(getNumbDaysWithoutOrders(pattern), result_expected)
})
