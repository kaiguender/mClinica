########## Plotting Difference Between Clean and Unclean Code ################

transactions_unclean <- readRDS("transactions_unclean.rds")
transactions <- readRDS("transactions_clean.rds")

# Calculating the the most impacted sku_code_prefixes by the amount of rows that have been deleted.
countPerSku_unclean <- transactions_unclean %>%
  group_by(sku_code_prefix) %>%
  summarise(count = n())

countPerSku <- transactions %>%
  group_by(sku_code_prefix) %>%
  summarise(count = n())

sku_codes_prefix <- unique(transactions$sku_code_prefix)

diffsSkuCount <- sapply(sku_codes_prefix, function(sku_code_prefix) {
  count <- countPerSku[countPerSku$sku_code_prefix == sku_code_prefix, ]$count
  count_unclean <- countPerSku_unclean[countPerSku_unclean$sku_code_prefix == sku_code_prefix, ]$count
  
  return(count_unclean - count)
})
names(diffsSkuCount) <- sku_codes_prefix

which(diffsSkuCount > 65)

plotCompareDeletedSkus(transactions,
                       transactions_unclean,
                       sku_code_prefix = diffSkuCount[which.max(diffsSkuCount)],
                       granularity = "week",
                       onePlot = FALSE)

#---

# Calculate the sku_code_prefixes that were most impacted by cleansing
sku_codes_prefix <- unique(transactions$sku_code_prefix)

diffsSku <- sapply(sku_codes_prefix, function(sku_code_prefix) {
  maxDiff <- max(transactions_unclean_ts[[sku_code_prefix]] - transactions_ts[[sku_code_prefix]])
  meanClean <- mean(transactions_ts[[sku_code_prefix]])
  return(return(maxDiff / meanClean))
})
names(diffsSku) <- sku_codes_prefix

# There are a couple of sku_codes_prefixes with massive differences
diffsSku[which(diffsSku > 10)]

plotCompareDeletedSkus(transactions,
                       transactions_unclean,
                       sku_code_prefix = 103257,
                       granularity = "week",
                       onePlot = TRUE)


################## Correlations ##############

# Strong Positive Correlations

plotTimeSeries(correlations_top,
               sku_code1 = 118595,
               sku_code2 = 119522,
               "week")

ts_109199 <- correlations_top$demandsPerSku[["109199"]]
ts_124034 <- correlations_top$demandsPerSku[["124034"]]


plotData_109199 <- data.frame("time" = seq_along(ts_109199), 
                              "demand" = ts_109199, 
                              label = "IMBOOST FORCE Soho")

plotData_124034 <- data.frame("time" = seq_along(ts_124034), 
                              "demand" = ts_124034, 
                              label = "IMBOOST FORCE KIDS")

plotData_posCorr <- rbind(plotData_109199, plotData_124034)

#############################

itemComment <- transactions_unclean$item_comment
stockOut_index <- which(grepl(itemComment, pattern = "STOCK"))
stockOut_distributor <- str_extract(itemComment[stockOut_index], pattern = "[:digit:]+")

stockOut_distributorID <- transactions_clean$distributor_id[stockOut_index]
stockOut_comparison <- cbind(as.numeric(stockOut_distributorID), as.numeric(stockOut_distributor))

test <- apply(stockOut_comparison, MARGIN = 1, function(row) {
  row[1] != row[2]
})

distributorChanging <- stockOut_index[which(test)]
transactions_clean[distributorChanging, ][, c("Ordered_date", "PO_deleted", "delivered_at","distributor_id", "sku_code",
                                              "item_comment", "comments")]


stockOut_rows <- transactions_clean[stockOut_index, ]
which(!is.na(stockOut_rows$delivered_at))
stockOut_rows[9000:9200, ]

stockOut_rows$delivered_at[8000:9000]

####

comments <- transactions_unclean$comments
stockOut_names <- rownames(transactions_unclean[which(grepl(comments, pattern = "STOCK")), ])
transactions_clean[stockOut_index2, ]

transactions_unclean[setdiff(stockOut_index2, stockOut_index), ]
transactions_unclean[transactions_unclean$PO_id == 16258, ]
transactions_clean[transactions_clean$PO_id == 16258, ]


##########
comments <- transactions_unclean$comments
itemcomment <- transactions_unclean$item_comment

stockOut_names_unclean <- rownames(transactions_unclean[which(grepl(comments, pattern = "STOCK")), ])
stockOut_names_unclean2 <- rownames(transactions_unclean[which(grepl(itemcomment, pattern = "STOCK")), ])
stockOut_names_unclean <- union(stockOut_names_unclean, stockOut_names_unclean2)


comments <- transactions_clean$comments
itemcomment <- transactions_clean$item_comment

stockOut_names_clean <- rownames(transactions_clean[which(grepl(comments, pattern = "STOCK")), ])
stockOut_names_clean2 <- rownames(transactions_clean[which(grepl(itemcomment, pattern = "STOCK")), ])
stockOut_names_clean <- union(stockOut_names_clean, stockOut_names_clean2)

transactions_unclean[setdiff(stockOut_names_unclean, stockOut_names_clean), ]

transactions_unclean[transactions_unclean$PO_id == 23154, ]


######################### 

transactions <- readRDS("transactions_clean.rds")
transactions_complete <- transactions[transactions$po_status == "completed", ]
unique(transactions_complete$invoice_status)
unique(transactions$invoice_status)

transactions_comp_pen <- transactions_complete[transactions_complete$invoice_status == "claimed", ]
transactions_comp_pen

missingSkus <- transactions_unclean[which(is.na(transactions_unclean$sku_code)), ]
which(is.na(missingSkus$PO_deleted))


#######################

sum(grepl(transactions_noDoubleOrders$item_comment, pattern = "DOUBLE") | grepl(transactions_noDoubleOrders$comments, pattern = "DOUBLE"))

################# Plotting different sku_codes

countPerSku <- transactions %>%
  group_by(sku_code_prefix) %>%
  summarise(count = n())

sku_codes_prefix <- countPerSku %>%
  filter(count >= quantile(countPerSku$count, 0.8)) %>%
  select(sku_code_prefix) %>%
  fastDoCall("c", .)

sku_codes_prefix[1]

plotTimeseriesDemand(transactions,
                     sku_code_prefix1 = sku_codes_prefix[1],
                     granularity = "month")

plotTimeseriesDemand(transactions,
                     sku_code_prefix1 = sku_codes_prefix[2],
                     granularity = "month")

plotTimeseriesDemand(transactions,
                     sku_code_prefix1 = sku_codes_prefix[3],
                     granularity = "month")

plotTimeseriesDemand(transactions,
                     sku_code_prefix1 = sku_codes_prefix[4],
                     granularity = "month")

ts <- getTimeseriesDemand(transactions,
                          sku_codes_prefix = sku_codes_prefix[1],
                          granularity = "day")
ts
