library(tidyverse)
library(Gmisc)
library(fasttime)
library(doParallel)
library(foreach)
library(lubridate)

transactions_clean <- read.rds("transactions_clean.csv")
transactions_unclean <- read.rds("transactions_clean.csv")


duplicatesStockOut <- function(transactions) {
  
  columnsToCheck <- c("Ordered_date_week", 
                      "pharmacy_id", 
                      "quantity",
                      "sku_code_prefix")
  
  transactions_mod <- transactions[, columnsToCheck]
  
  stockOut_itemComment <- grepl(transactions$item_comment, pattern = "STOCK")
  stockOut_comments <- grepl(transactions$comments, pattern = "STOCK")
  stockOut_indices <- stockOut_itemComment | stockOut_comments
  
  areaNotCov_itemComment <- grepl(transactions$item_comment, pattern = "AREA")
  areaNotCov_comments <- grepl(transactions$comments, pattern = "AREA")
  areaNotCov_indices <- areaNotCov_itemComment | areaNotCov_comments
  
  minInvoice_itemComment <- grepl(transactions$item_comment, pattern = "MIN")
  minInvoice_comments <- grepl(transactions$comments, pattern = "MIN")
  minInvoice_indices <- minInvoice_itemComment | minInvoice_comments
  
  specialCase_indices <- stockOut_indices | areaNotCov_indices | minInvoice_indices
  specialCase <- transactions[specialCase_indices, ]
  specialCase_mod <- specialCase[, columnsToCheck]
  
  noSpecialCase <- transactions[!stockOut_indices, ]
  noSpecialCase_mod <- noSpecialCase[, columnsToCheck]
  
  # The goal is to find all rows among the Stock-out instances for which another row exists that is identical in terms of
  # all the columns of 'columnsToCheck'.
  rowsToCheck <- rbind(noSpecialCase_mod, specialCase_mod)
  duplicationExists_index <- tail(x = duplicated(rowsToCheck), n = nrow(specialCase_mod))
  specialCase_duplicate <- specialCase[duplicationExists_index, ]
  
  po_ids_duplicates <- unique(specialCase_duplicate$PO_id)
  
  # For each of the duplicate rows we reverse the procedure and thus try to find all rows now that are
  # a duplicate of the original duplicate. By doing this we get a pool of candidate rows that are potential duplicate orders
  # and could therefor be deleted. To finally decide on which rows to delete we apply a certain decision rule later on that
  # can be reenacted by looking at the code below.
  
  duplicateCombs <- lapply(po_ids_duplicates, function(po_id) {
    
    order <- specialCase_duplicate[specialCase_duplicate$PO_id == po_id, ]
    order_mod <- order[, columnsToCheck]
    
    orderedDate_week <- unique(order_mod$Ordered_date_week)
    sku_codes_prefix <- order_mod$sku_code_prefix
    pharmacy_id <- unique(order_mod$pharmacy_id)
    quantities <- order_mod$quantity
    
    # Before applying the 'duplicated' function yet again we first restrict the amount of transactions that we look
    # into to find duplicates by requiring the po_id to be different, the order-day to be the same etc.
    # IMPORTANT: Because we require the PO_id to be different the pool of candidates can't contain the 
    # same rows as our current 'order'-object.
    candidatesIndices <- transactions_mod$Ordered_date_week == orderedDate_week & 
      transactions$PO_id != po_id &
      transactions_mod$sku_code_prefix %in% sku_codes_prefix &
      transactions_mod$pharmacy_id == pharmacy_id &
      transactions_mod$quantity %in% quantities
    
    duplicates <- transactions[candidatesIndices, ]
    
    duplicateComb <- rbind(order, duplicates)
    duplicateComb <- duplicateComb[!duplicated(duplicateComb), ]
    
    
    duplicateComb_list <- lapply(1:nrow(order), function(i) {
      row <- order[i, ]
      duplicate <- duplicates[duplicates$sku_code_prefix == row$sku_code_prefix, ]
      
      chronology <- duplicate$Ordered_date >= row$Ordered_date
      
      if(any(chronology)) {
        return(rbind(row, duplicate[chronology, ]))
      }
      
      return(NULL)
    })
    
    duplicateComb <- fastDoCall("rbind", duplicateComb_list)
    return(duplicateComb)
  })
  nullCheck <- sapply(duplicateCombs, function(x) {
    !is.null(x)
  })
  duplicateCombs <- duplicateCombs[nullCheck]
  
  return(duplicateCombs)
}

duplicateCombs <- duplicatesStockOut(transactions_clean)

