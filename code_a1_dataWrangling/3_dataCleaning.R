############## Packages needed ####################

library(tidyverse)
library(Gmisc)
library(fasttime)
library(doParallel)
library(foreach)
library(lubridate)

# ---

############ Execution of Functions #############

transactions_unclean <- readRDS("transactions_unclean.rds")
cleansing_Results <- dataCleansing_main(transactions_unclean)

transactions_clean <- cleansing_Results$transactions
deletionCounts <- cleansing_Results$deletionCounts
deletionCounts

# write.csv2(transactions_clean, "transactions_clean.csv")
saveRDS(transactions_clean, "transactions_clean.rds")

# ---

############### Main Function ##################

# It is important to apply the functions in the same order that is used in the main function.
# The numeration of the rows stays unchanged, so it is possible to track which rows have been deleted
# in the process.

dataCleansing_main <- function(transactions) {
  
  # 'deletionCounts' keeps track of the rows deleted by the applied functions.
  deletionCounts <- c()
  
  transactions_Merge <- deleteByMerge(transactions)
  deletionCounts <- c(deletionCounts, nrow(transactions) - nrow(transactions_Merge))
  names(deletionCounts)[length(deletionCounts)] <- "merge"
  print("merge...")
  
  transactions_noMissingValues <- deleteByMissingValues(transactions_Merge)
  deletionCounts <- c(deletionCounts, nrow(transactions_Merge) - nrow(transactions_noMissingValues))
  names(deletionCounts)[length(deletionCounts)] <- "MissingValues"
  print("missingValues...")
  
  transactions_noDoubleSkus <- deleteByDoubleSkus(transactions_noMissingValues)
  deletionCounts <- c(deletionCounts, nrow(transactions_noMissingValues) - nrow(transactions_noDoubleSkus))
  names(deletionCounts)[length(deletionCounts)] <- "doubleSkus"
  print("doubleSkus...")
  
  transactions_noDoubleOrders <- deleteByDoubleOrder(transactions_noDoubleSkus)
  deletionCounts <- c(deletionCounts, nrow(transactions_noDoubleSkus) - nrow(transactions_noDoubleOrders))
  names(deletionCounts)[length(deletionCounts)] <- "doubleOrders"
  print("doubleOrders...")
  
  transactions_noDoubleOrdersWeek <- deleteByDoubleOrdersWeek(transactions_noDoubleOrders)
  deletionCounts <- c(deletionCounts, nrow(transactions_noDoubleOrders) - nrow(transactions_noDoubleOrdersWeek))
  names(deletionCounts)[length(deletionCounts)] <- "doubleOrdersWeek"
  
  transactions_noCancellations <- deleteByCancellations(transactions_noDoubleOrdersWeek)
  deletionCounts <- c(deletionCounts, nrow(transactions_noDoubleOrdersWeek) - nrow(transactions_noCancellations))
  names(deletionCounts)[length(deletionCounts)] <- "cancellations"
  
  transactions_noPOdeleted <- deleteByPODeleted(transactions_noCancellations)
  deletionCounts <- c(deletionCounts, nrow(transactions_noCancellations) - nrow(transactions_noPOdeleted))
  names(deletionCounts)[length(deletionCounts)] <- "PODeleted"
  print("POdeleted...")
  
  transactions <- transactions_noPOdeleted
  
  # Other non-deletion cleaning steps
  transactions <- quantityCleaning(transactions)
  transactions <- priceCleaning(transactions)
  transactions <- discountCleaning(transactions)
  
  return(list("transactions" = transactions,
              "deletionCounts" = deletionCounts))
}

# ---  

############### Merge Deletion ##################

# a) Every merged_to entry specifies a PO_id. For every line given in 'mergeEntryRows_List' we look for all rows that
# possess the PO_id specified by the merged_to entry. 
# b) In the first step we check if any of the found rows contains the same sku_code as our original row. If such a row exists,
# we add it to the original row using 'rbind'. If no such row exists, we add an arbitrary row of the found rows instead.
# c) In the next step we check if the found rows also contain a merged_to entry. If so, we - yet again - try to find 
# all rows that possess the specified PO_id.
# d) We proceed exactly as in step (b) until there is either no new merged_to entry given in the newly found lines 
# or the given merged_to entry refers to a PO_id that is not part of our data.

deleteByMerge <- function(transactions) {
  
  # We extract all rows that have a merge_to entry and sku_code given at the same time. 
  # All those rows serve as a starting point of a chain (in the above mentioned sense) that is formed 
  # by the merged_to entry.
  mergeEntryRows <- transactions[!is.na(transactions$sku_code) & !is.na(transactions$merged_to), ]
  mergeEntryRows_List <- lapply(1:nrow(mergeEntryRows), function(rownumber) {
    return(mergeEntryRows[rownumber, ])
  })
  
  mergeCombinations_Chain <- lapply(mergeEntryRows_List, function(initialRow) {
    # mergedRows refers to all rows that are part of the current chain of rows. That's why we
    # initiate this by 'mergedRows <- initialRow'. 
    # The same concept applies to 'merge_PoId' and 'merge_skuPrefix'.
    mergedRows <- initialRow
    merge_PoId <- initialRow$merged_to
    
    initial_skuPrefix <- initialRow$sku_code_prefix
    
    # The while-loop is used to create the desired chain for the current initial row.
    while(merge_PoId %in% transactions$PO_id) {
      
      # We extract all rows that belong to the PO_id that is given by the current merge_to entry, which
      # we call 'merge_PoId'.
      # 'mergeCandidates' are all rows that belong to the Po_Id of the current 'merge_PoId'.
      mergeCandidates <- transactions[transactions$PO_id == merge_PoId, ]
      mergeCandidates_MergeTo <- unique(mergeCandidates$merged_to[!is.na(mergeCandidates$merged_to)])
      
      # We grab all rows of 'mergeCandidates' that contain the sku_code_prefix of the initial row.
      if(initial_skuPrefix %in% mergeCandidates$sku_code_prefix) {
        indexSkuPrefix <- which(mergeCandidates$sku_code_prefix == initial_skuPrefix)
        
        rowToMerge <- mergeCandidates[indexSkuPrefix, ]
        mergedRows <- rbind(mergedRows, rowToMerge)
        
        if(any(!is.na(rowToMerge$merged_to))) {
          merge_PoId <- rowToMerge$merged_to
        } else {
          merge_PoId <- -9999999
        }
        
        # NOTE: There is always maximal one unique 'merged_to' per PO_id. 
      } else if(length(mergeCandidates_MergeTo) > 0) {
        merge_PoId <- mergeCandidates_MergeTo
        rowToMerge <- mergeCandidates[which(!is.na(mergeCandidates$merged_to)), ][1, ]
        
        mergedRows <- rbind(mergedRows, rowToMerge)
        
      } else {
        mergedRows <- rbind(mergedRows, mergeCandidates[1, ])
        merge_PoId <- -9999999
      }
    } 
    return(mergedRows)
  })

  # The idea is now to remove all initial rows who possess a merged_to entry, that linked to PO_ids, who
  # contained the same sku_code_prefix as the initial row. This means that the original order was being
  # transacted at the linked PO_id and would thus be part of the data set twice if we didn't remove it.
  rowsToDelete <- unlist(sapply(mergeCombinations_Chain, function(chain) {
    sku_code_available <-  chain[-1, ]$sku_code_prefix == chain[1, ]$sku_code_prefix
    
    if(isTRUE(any(sku_code_available))) {
      pharmacy_identical <- chain[-1, ]$pharmacy_id[sku_code_available] == chain[1, ]$pharmacy_id
      
      if(any(pharmacy_identical)) {
        return(row.names(chain)[1]) 
      }
    } else {
      return(NULL)
    }
  }))
  transactions <- transactions[!(rownames(transactions) %in% rowsToDelete), ]
  
  #rownames(transactions) <- 1:nrow(transactions)
  return(transactions)
}

# ---

########### Delete by missing Values ###########

# When there is a quantity or price of 0 given we simply delete the given row. It makes no sense to give a quantity
# of 0 because even if the costumer cancels the order the original quantity is supposed to be kept and instead
# there should be a comment indicating a cancellation instead of simply setting the quantity to 0.

deleteByMissingValues <- function(transactions) {
  
  # Missing sku_code
  transactions <- transactions[which(!is.na(transactions$sku_code)), ]
  
  # Missing price or quantity data
  noPrice_index <- which(transactions$unit_selling_price == 0)
  noPrice_index <- c(noPrice_index, which(is.na(transactions$unit_selling_price)))
  
  noQuantity_index <- which(transactions$quantity == 0)
  noQuantity_index <- c(noQuantity_index, which(is.na(transactions$quantity)))
  
  if(length(noPrice_index) > 0 | length(noQuantity_index) > 0) {
    transactions <- transactions[-c(noPrice_index, noQuantity_index), ]
  }

  return(transactions)
}

#---

############### Double sku_codes ##################

# The main idea of this function is that every sku_code should appear at most one time per PO_id. For that reason we
# go through each PO_id independently and try to find all cases where this rule is violated. 
# If one sku_code appears several times this is a clear indication that some of the rows are just unnecessary duplicates
# of each other. By applying a certain decision rule - that can easily be reenacted by going through the code - we 
# label those rows as duplicates and delete all of them afterwards.

deleteByDoubleSkus <- function(transactions) {
  
  registerDoParallel()
  
  # The following steps are done seperately and independently for each PO_id.
  # rowsToDelete <- unlist(foreach(po_id = unique(transactions$PO_id), .packages = "stringr") %dopar% {
  rowsToDelete <- unlist(lapply(unique(transactions$PO_id), function(po_id) {
    
    order <- transactions[transactions$PO_id == po_id, ]
    sku_codes <- order$sku_code
    
    # After extracting all rows belonging to the current PO_id we try to find all sku_codes that appear more than
    # one time in the current order.
    doubleSkuCodes_Check <- sapply(sku_codes, function(sku_code) {
      return(sum(sku_codes == sku_code) > 1)
    })
    
    if(any(doubleSkuCodes_Check)) {
      doubleSkuCodes <- unique(sku_codes[doubleSkuCodes_Check])
    } else {
      return(NULL)
    }
    
    # The following steps are done independently and seperately for each sku_code that appeared more than one time.
    rowsToDelete <- unlist(lapply(doubleSkuCodes, function(sku_code) {
      
      # IMPORTANT: orderSku really only contains all rows that possess the same sku_code!
      orderSku <- order[order$sku_code == sku_code, ]
      
      # rowsToDelete keeps track of all rows we've found so far that are duplicates and can be deleted. In order
      # to spot those duplicates we go through several if-conditions that are applied in a hierarchical(!) manner.
      rowsToDelete <- c()
      
      # When at least one of the rows has been delivered and at least one of the rows hasn't we delete all rows
      # that haven't been delivered. 
      if(any(!is.na(orderSku$delivered_at)) && any(is.na(orderSku$delivered_at))) {
        
        rowsToDelete <- c(rowsToDelete, row.names(orderSku[is.na(orderSku$delivered_at), ]))
        rowsToDelete <- unique(rowsToDelete)
        
        # The ultimate goal is to end with only one row that won't get deleted in the end. So once we found
        # nrow(orderSku) - 1 rows to delete, we can stop the process.
        if(length(rowsToDelete) == (nrow(orderSku) - 1)) {
          return(rowsToDelete)
        }
      } 
      
      # Same idea as the 'delivered' if-condition.
      if(any(!is.na(orderSku$PO_deleted)) && any(is.na(orderSku$PO_deleted))) { 
        
        rowsToDelete <- c(rowsToDelete, row.names(orderSku[!is.na(orderSku$PO_deleted), ]))
        rowsToDelete <- unique(rowsToDelete)
        
        if(length(rowsToDelete) == (nrow(orderSku) - 1)) {
          return(rowsToDelete)
        }
      } 
      
      # Same idea as the 'delivered' if-condition.
      if(any(!is.na(orderSku$POI_deleted)) && any(is.na(orderSku$POI_deleted))) { 
        
        rowsToDelete <- c(rowsToDelete, row.names(orderSku[!is.na(orderSku$POI_deleted), ]))
        rowsToDelete <- unique(rowsToDelete)
        
        if(length(rowsToDelete) == (nrow(orderSku) - 1)) {
          return(rowsToDelete)
        }
      }
      
      # Same idea as the 'delivered' if-condition.
      if(any(!is.na(orderSku$invoice_input_time)) && any(is.na(orderSku$invoice_input_time))) {
        
        rowsToDelete <- c(rowsToDelete, row.names(orderSku[is.na(orderSku$invoice_input_time), ]))
        rowsToDelete <- unique(rowsToDelete)
        
        if(length(rowsToDelete) == (nrow(orderSku) - 1)) {
          return(rowsToDelete)
        }
      } 
      
      # Same idea as the 'delivered' if-condition.
      if(any(!is.na(orderSku$invoice_deleted)) && any(is.na(orderSku$invoice_deleted))) {
        rowsToDelete <- c(rowsToDelete, row.names(orderSku[!is.na(orderSku$invoice_deleted), ]))
        
        rowsToDelete <- unique(rowsToDelete)
        if(length(rowsToDelete) == (nrow(orderSku) - 1)) {
          return(rowsToDelete)
        }
      } 
      
      # Same idea as the 'delivered' if-condition.
      if(any(!is.na(orderSku$paid_date)) && any(is.na(orderSku$paid_date))) {
        rowsToDelete <- c(rowsToDelete, row.names(orderSku[is.na(orderSku$paid_date), ]))
        
        rowsToDelete <- unique(rowsToDelete)
        if(length(rowsToDelete) == (nrow(orderSku) - 1)) {
          return(rowsToDelete)
        }
      }
      
      # For the following steps we want to get rid of all rows that have been labeled as duplicates already.
      if(length(rowsToDelete) > 0) {
        orderSku_reduced <- orderSku[!row.names(orderSku) %in% rowsToDelete, ]
      } else {
        orderSku_reduced <- orderSku
      }
      
      # Our next take is to look at the item_comment of the rows. Comments including the phrases "QTY", "FROM" 
      # and "REGULER" show the final quantity of the sku_code. If one row contains a different quantity
      # it is with high probability a duplicated order.
      # Comments containing the phrase "WRONG" indicate a wrongly placed order and thus also strongly hint at 
      # a duplication being on hand. 
      if(any(!is.na(orderSku_reduced$item_comment))) {
        
        pattern_wrong <- "WRONG"
        pattern_qty <- c("QTY\\s*[[:alpha:]]*\\s*[JADI]*\\s*[0-9]+",
                         "[[:alpha:]]*\\s*[KUANTITI]\\s*[[:alpha:]]*\\s*[0-9]+")
        pattern_qty <- paste0(pattern1, collapse = "|")
        
        pattern_wrong_indey <- grepl(orderSku_reduced$item_comment, pattern = pattern_wrong)
        pattern_qty_index <- grepl(orderSku_reduced$item_comment, pattern = pattern_qty)
        
        rowsToDelete_New <- c()
        if(any(pattern_qty_index)) {
          finalQuantity <- as.numeric(str_extract(orderSku_reduced$item_comment[pattern_qty], pattern = "[:digit:]+"))[1]
          rowsToDelete_New <- row.names(orderSku_reduced[!orderSku_reduced$quantity == finalQuantity, ])
        }
        
        # If all rows have been labeled duplicates usually the following thing happened: All rows have been handled identically
        # by mClinica despite all but one row being just unnecessary duplicates. For that reason we simply keep one row
        # arbitrarily but we at least keep one that has one of the item_comments attached to it.
        if(length(rowsToDelete_New) == nrow(orderSku_reduced)) {
          keptRowsCandidates <- orderSku_reduced[pattern_qty_index, ]
          keptRow <- row.names(keptRowsCandidates)[1]
          rowsToDelete_New <- setdiff(row.names(orderSku_reduced), keptRow)
        }
        
        # If all rows contain the item_comment 'WRONG ORDER' we treat this case in a similar fashion like above.
        # Keeping the first row is arbitrary as we view all rows to be identical duplicates of each other in this case. 
        # Therefor we could also choose the second or the third etc. 
        if(any(pattern4)) {
          
          if(sum(pattern4) == nrow(orderSku_reduced)) {
            rowsToDelete_New <- row.names(orderSku_reduced)[-1]
          }
          
          else {
            rowsToDelete_New <- row.names(orderSku_reduced[pattern4, ])
          }
        }
        rowsToDelete <- c(rowsToDelete, rowsToDelete_New)
        rowsToDelete <- unique(rowsToDelete)
        
        # We get rid of all duplicated rows yet again.
        if(length(rowsToDelete_New) > 0) {
          orderSku_reduced <- orderSku_reduced[!(row.names(orderSku_reduced) %in% rowsToDelete_New), ]
        } 
      }
      
      # When there are still rows with the same sku_code left that couldn't be deleted by applying the former
      # decision rules we simply delete all of the left over rows apart from the first one.
      # The idea is yet again that all rows left at this point are identical duplicates of each other and thus we want 
      # to keep exactly one of them. 
      if(nrow(orderSku_reduced) > 1 && length(unique(orderSku_reduced$quantity)) == 1) {
        rowsToDelete <- c(rowsToDelete, row.names(orderSku_reduced[-1, ]))
      }
      
      if(length(rowsToDelete) < nrow(orderSku) - 1) {
        browser()
      }
      
      return(rowsToDelete)
    }))
    return(rowsToDelete)
  }))
  
  transactions_reduced <- transactions[!(rownames(transactions) %in% rowsToDelete), ]
  
  # rownames(transactions_reduced) <- 1:nrow(transactions_reduced)
  return(transactions_reduced)
}


# ---

############### Double Orders ##################

# IMPORTANT: This function should only be used AFTER applying the function 'deleteByDoubleSkus'.

# The idea behind the function 'deleteByDoubleOrder' is the following: We try to spot all orders that are identical
# in a certain sense. In our case we regard two orders as identical if they fulfill the following conditions:
# 1) Ordered on the same day
# 2) Identical sku_code
# 3) Identical pharmacy_id
# 4) Identical quantity, unit_selling_price and discount

# All orders that have been identified as identical by those conditions serve as potential(!) duplicate candidates of 
# each other. In order to determine whether found rows are indeed duplicates of each other we apply a certain decision
# rule that can easily be reenacted by looking at the code.

deleteByDoubleOrder <- function(transactions) {
  
  # columnsToCheck comprises all columns that have to be identical for us to regard two rows as potential duplicates.
  columnsToCheck <- c("Ordered_date_date", 
                      "pharmacy_id", 
                      "sku_code", 
                      "quantity", 
                      "unit_selling_price", 
                      "unit_discount")
  
  # The suffix 'mod' is supposed to imply that the given object contains only the relevant columns specified by
  # 'columnsToCheck'. 
  transactions_mod <- transactions[, columnsToCheck]
  
  # This step initiates the main idea behind the following steps: We look for all rows that are a duplicate of another row
  # when reduced to the above mentioned relevant columns.
  duplicatesIndices <- which(duplicated(transactions_mod))
  duplicates <- transactions[duplicatesIndices, ]
  
  po_ids_duplicates <- unique(duplicates$PO_id)
  table_dups <- transactions[transactions$PO_id %in% po_ids_duplicates, ]
  
  # For each of the duplicate rows we reverse the procedure and thus try to find all rows now that are
  # a duplicate of the original duplicate. By doing this we get a pool of candidate rows that are potential double orders
  # and could therefor be deleted. To finally decide on which rows to delete we apply a certain decision rule later on that
  # can be reenacted by looking at the code below.
  registerDoParallel()
  duplicateCombs <- foreach(po_id = po_ids_duplicates) %dopar% {
    # duplicateCombs <- lapply(po_ids_duplicates,function(po_id) {
    order <- duplicates[duplicates$PO_id == po_id, ]
    order_mod <- order[, columnsToCheck]
    
    orderedDate <- unique(order_mod$Ordered_date_date)
    sku_codes <- order_mod$sku_code
    pharmacy_id <- unique(order_mod$pharmacy_id)
    quantities <- order_mod$quantity
    
    # Before applying the 'duplicated' function yet again we first restrict the amount of transactions that we look
    # into to find duplicates by requiring the po_id to be different (only possible because we used the function
    # 'deleteByDoubleSkus' before hand), the order-day to be the same etc.
    candidatesIndices <- transactions_mod$Ordered_date_date == orderedDate & 
      transactions_mod$sku_code %in% sku_codes &
      transactions$PO_id != po_id &
      transactions_mod$pharmacy_id == pharmacy_id &
      transactions_mod$quantity %in% quantities
    
    candidates <- transactions[candidatesIndices, ]
    candidates_mod <- candidates[, columnsToCheck]
    
    tableToCheck <- rbind(order, candidates)
    tableToCheck_mod <- rbind(order_mod, candidates_mod)
    
    # The object duplicatesOrder contains all rows that are duplicates of our original duplicate row.
    # Note: We have to use the 'tail' function because there could theoretically also be duplicates
    # among the rows of 'order' itself.
    duplicates_indices <- tail(duplicated(tableToCheck_mod), n = nrow(candidates))
    duplicatesOrder <- candidates[duplicates_indices, ]
    
    return(rbind(order, duplicatesOrder))
  }
  
  # After having found potential candidates for double orders we have to apply the above mentioned decision rule now
  # for determining the desired double orders.
  # Every entry of the list 'duplicateCombs' is a table that contains matching double order candidates.
  rowsToDelete <- unlist(foreach(comb = duplicateCombs, .packages = "stringr") %dopar% {
  # rowsToDelete <- unlist(lapply(duplicateCombs, function(comb) {
    sku_codes <- unique(comb$sku_code)
    
    # We apply the following steps seperately and independently for each sku_code
    rowsToDelete <- unlist(lapply(sku_codes, function(sku_code) {
      
      combSku <- comb[comb$sku_code == sku_code, ]
      
      rowsToDelete <- c()
      # In order to determine double orders we first do the obvious: We exploit the information given by the features
      # 'item_comment' and 'comments'.
      if(any(!is.na(combSku$item_comment) | !is.na(combSku$comments))) {
        
        # If a row contains the comment 'DOUBLE ORDER' etc. it is very likely that the regarded row is indeed a double order.
        doubleOrderIndex1 <- grepl(combSku$item_comment, pattern = "DOUBLE")
        doubleOrderIndex2 <- grepl(combSku$item_comment, pattern = "DOBEL")
        doubleOrderIndex3 <- grepl(combSku$item_comment, pattern = "RETUR")
        doubleOrderIndex4 <- grepl(combSku$comments, pattern = "DOUBLE")
        doubleOrderIndex <- doubleOrderIndex1 | doubleOrderIndex2 | doubleOrderIndex3 | doubleOrderIndex4
        
        # If all rows of our candidates contained comments hinting at a double order it is reasonable to assume that 
        # mClinica that a mistake here: If there is a double order taking place then there has to be an original order
        # to begin with that is no double order.
        # Therefor we don't delete all rows in this case and instead keep one of them.
        if(all(doubleOrderIndex)) {
          # When there is at least one of the double orders that has been delivered we want to keep one of them
          # because it is reasonable to assume that this line has wrongly been marked as a double order.
          if(any(!is.na(combSku$delivered_at))) {
            noDeletion <- row.names(combSku[!is.na(combSku$delivered_at), ][1, ])
            rowsToDelete_New <- setdiff(row.names(combSku), noDeletion)
            rowsToDelete <- c(rowsToDelete, rowsToDelete_New)
            
          } else {
            # It makes no sense that all rows are labeled as 'double order' because there has to be a original order to
            # begin with. Because of that we keep at least one line of all lines that have been labeled as double orders.
            rowsToDelete <- c(rowsToDelete, row.names(combSku[-1, ]))
          }
          
        } else {
          rowsToDelete <- c(rowsToDelete, row.names(combSku[doubleOrderIndex, ]))
        }
        
        # Needed for later steps. Done here in order to only call the grepl-function when there are any non-empty
        # comment entries.
        stockOut <- grepl(combSku$item_comment, pattern = "STOCK") | grepl(combSku$comments, pattern = "STOCK")
        belowInvoice <- grepl(combSku$item_comment, pattern = "MIN") | grepl(combSku$comments, pattern = "MIN")
        notCovered <- grepl(combSku$item_comment, pattern = "AREA") | grepl(combSku$comments, pattern = "AREA")
        
      } else {
        stockOut <- rep_len(FALSE, length.out = nrow(combSku))
        belowInvoice <- rep_len(FALSE, length.out = nrow(combSku))
        notCovered <- rep_len(FALSE, length.out = nrow(combSku))
      }
      
      # This if-condition has been deleted because it runs on the assumption that there should only be one order
      # of the same sku_code of the exact same structure per pharmacy per day. This doesn't necessarly have to be
      # the case as a pharmacy can simply place the same order twice per day if they think it is reasonable to do so.
      # if(all(stockOut | belowInvoice | notCovered)) {
      #   browser()
      #   rowsToDelete <- c(rowsToDelete, row.names(combSku[-1, ]))
      # }
      
      # In this last step we look at each row seperately. If it has been deleted, not delivered and there is 
      # no indication of stock out etc. we also deem this row an unnecessary duplicate.
      for(i in 1:nrow(combSku)) {
        deleted <- !is.na(combSku$PO_deleted[i])
        noInvoice <- is.na(combSku$invoice_id[i])
        notDelivered <- is.na(combSku$delivered_at[i])
        
        noStockOut <- !(stockOut[i])
        noBelowInvoice <- !(belowInvoice[i])
        noNotCovered <- !(notCovered[i])
        
        if(deleted && notDelivered && noStockOut && noBelowInvoice && noNotCovered) {
          rowsToDelete <- c(rowsToDelete, row.names(combSku[i, ]))
        }
        
      }
      return(unique(rowsToDelete))
    }))
    return(rowsToDelete)
  })
  transactions <- transactions[!rownames(transactions) %in% rowsToDelete, ]
  
  # rownames(transactions) <- 1:nrow(transactions)  
  return(transactions)
}

# ---

############ Double Orders Week ################

# It seems like there are also double orders that took place on different days. For that reason we search for
# duplicates that have been ordered at least during the same week. As a pharmacy can obviously order the same
# product several times per week on purpose we constrain ourselves to rows that possess a 'DOUBLE ORDER' comment.
# We also demand the deleted row to possess a 'PO_deleted' entry and to not have been delivered.

deleteByDoubleOrdersWeek <- function(transactions) {
  
  doubleOrder_itemComment <- grepl(transactions$item_comment, pattern = "DOUBLE")
  doubleOrder_comments <- grepl(transactions$comments, pattern = "DOUBLE")
  doubleOrder_indices <- doubleOrder_itemComment | doubleOrder_comments
  
  transactions_double <- transactions[doubleOrder_indices, ]
  transactions_noDouble <- transactions[!doubleOrder_indices, ]
  
  columnsToCheck <- c("Ordered_date_week", 
                      "pharmacy_id", 
                      "quantity",
                      "sku_code", 
                      "unit_selling_price", 
                      "unit_discount")
  
  # We try to find all rows amoung our 'DOUBLE'-rows for which another row exists that is identical for 
  # all columns of 'columnsToCheck'. 
  rowsToCheck <- rbind(transactions_noDouble[, columnsToCheck], transactions_double[, columnsToCheck])
  duplicatedCheck <- tail(x = duplicated(rowsToCheck), n = nrow(transactions_double))
  
  # Among the rows that we view as candidates for deletion should only be such rows
  # for which we could find a duplicate-equivalent to begin with. If a row has been labeled 'DOUBLE ORDER'
  # but we can't find such a duplicate-equivalent, we must assume that this label has been placed accidentally.
  transactions_double <- transactions_double[duplicatedCheck, ]
  
  transactions_mod <- transactions[, columnsToCheck]
  transactions_double_mod <- transactions_double[, columnsToCheck]
  po_ids <- unique(transactions_double$PO_id)
  
  # For each of the duplicate rows we reverse the procedure and thus try to find all rows now that are
  # a duplicate of the original duplicate. By doing this we get a pool of candidate rows that are potential double orders
  # and could therefor be deleted. To finally decide on which rows to delete we apply a certain decision rule later on that
  # can be reenacted by looking at the code below.
  
  registerDoParallel()
  rowsToDelete <- unlist(foreach(po_id = po_ids) %dopar% {
  # rowsToDelete <- unlist(lapply(po_ids, function(po_id) { 
    order <- transactions_double[transactions_double$PO_id == po_id, ]
    order_mod <- order[, columnsToCheck]
    
    orderedDateWeek <- unique(order_mod$Ordered_date_week)
    sku_codes <- order_mod$sku_code
    pharmacy_id <- unique(order_mod$pharmacy_id)
    quantities <- order_mod$quantity
    
    # Before applying the 'duplicated' function yet again we first restrict the amount of transactions that we look
    # into to find duplicates by requiring the po_id to be different (only possible because we used the function
    # 'deleteByDoubleSkus' before hand), the order-day to be the same etc.
    # IMPORTANT: Because we require the PO_id to be different the pool of candidates can't contain the 
    # same rows as our current 'order'-object.
    candidatesIndices <- transactions_mod$Ordered_date_week == orderedDateWeek & 
      transactions_mod$sku_code %in% sku_codes &
      transactions$PO_id != po_id &
      transactions_mod$pharmacy_id == pharmacy_id &
      transactions_mod$quantity %in% quantities
    
    candidates <- transactions[candidatesIndices, ]
    candidates_mod <- candidates[, columnsToCheck]
    
    tableToCheck <- rbind(order, candidates)
    tableToCheck_mod <- rbind(order_mod, candidates_mod)
    
    # The object duplicatesOrder contains all rows that are duplicates of our original duplicate row.
    # Note: We have to use the 'tail' function because there could theoretically also be duplicates
    # among the rows of 'order' itself.
    duplicates_indices <- tail(duplicated(tableToCheck_mod), n = nrow(candidates))
    duplicatesOrder <- candidates[duplicates_indices, ]
    
    if(nrow(duplicatesOrder) == 0) {
      return(NULL)
    } 
    
    rowsToDelete_indices <- sapply(1:nrow(order), function(i) {
      row <- order[i, ]
      
      sku_code_available <- row$sku_code %in% duplicatesOrder$sku_code
      noDelivery <- is.na(row$delivered_at)
      deleted <- !is.na(row$PO_deleted)
      
      if(!(sku_code_available && noDelivery && deleted)) {
        return(FALSE)
      }
      
      duplicate <- duplicatesOrder[duplicatesOrder$sku_code == row$sku_code, ]
      quantity_identical <- duplicate$quantity == row$quantity
      
      if(any(quantity_identical)) {
        duplicate <- duplicate[quantity_identical, ]
        chronology <- duplicate$Ordered_date <= row$Ordered_date

        if(any(chronology)) {
          return(TRUE)
        }
      }
      return(FALSE)
    })
    
    rowsToDelete <- rownames(order)[rowsToDelete_indices]
    return(rowsToDelete) 
  })
  
  transactions <- transactions[!rownames(transactions) %in% rowsToDelete, ]
  return(transactions)
}

# ---

############### Comments Deletion ##################

# Comments can be one of the following standardized entries:
# [1] ""                                                       "PHARMACY_REQUESTED_CANCELLATION"                       
# [3] "NEAR_EXPIRY_DATE"                                       "PRICE_CHANGE"                                          
# [5] "OVERDUE_SHIPMENT"                                       "OTHER"                                                 
# [7] "DOUBLE_ORDER"                                           "WRONG_ORDER"                                           
# [9] "ORDER_CANCELLED_DUE_TO_TAX"                             "OUT_OF_STOCK"                                          
# [11] "DISTRIBUTOR_SENT_WRONG_PRODUCT"                        "NO_SP"                                                 
# [13] "PRECURSOR_PRODUCT"                                     "BELOW_MINIMUM_INVOICE"                                 
# [15] "PHARMACY_IS_TESTING_APP"                               "EXPIRED_SIA"                                           
# [17] "AREA_NOT_COVERED"                                      "PHARMACY_HAS_DEBT"                                     
# [19] "SYSTEM CREATED DOUBLE ORDER"                           "PHARMACY REQUEST CANCEL"


# After applying the following deletion procedure there are still around 200 rows left with the comment
# "PHARMACY REQUESTED CANCELLATION". In around half of those cases there was a STOCK OUT or an order
# BELOW MINIMUM INVOICE etc, so we want to keep those rows, because they possess valueable information
# for us.
# In all the other cases the rows have an entry at 'delivered_at'. It would be reasonable to assume that
# those rows might have gotten their 'delivered_at' entry wrongly. This is backed up by the fact that many
# of those rows have an entry at 'POI_deleted'. 
# Looking at the attached invoice photos shows that this assumption is only true in a minority of cases. 
# In the majority of cases a row with a 'delivered_at' entry has been delivered for real despite the
# 'POI_deleted' entry. For this reason we don't delete any rows with a 'delivered_at' entry automatically.

# As I've looked at the invoice photos, the entries for which the assumption holds are deleted manually.
# This manual type of deletion won't be done for future data.

deleteByCancellations <- function(transactions) {
  
  cancelsComment_index <- which(transactions$comments == "PHARMACY_REQUESTED_CANCELLATION")
  cancelsItemComment_index <- which(transactions$item_comment == "REQ")
  
  rowsCancelled_index <- union(cancelsComment_index, cancelsItemComment_index)
  rowsCancelled <- transactions[rowsCancelled_index, ]
  
  noDelivery <- is.na(rowsCancelled$delivered_at)
  noStockOut <- !grepl(rowsCancelled$item_comment, pattern = "STOCK")
  noHNAChange <- !grepl(rowsCancelled$item_comment, pattern = "HNA")
  noQuantityChange <- !grepl(rowsCancelled$item_comment, pattern = "QTY")
  noPriceChanging <- !grepl(rowsCancelled$item_comment, pattern = "PRICE")
  noBelowInvoice <- !grepl(rowsCancelled$item_comment, pattern = "BELOW")
  noDiscChange <- !grepl(rowsCancelled$item_comment, pattern = "DISC")
  
  rowsToDelete_boolean <- noDelivery & noStockOut & noHNAChange & noQuantityChange & 
    noPriceChanging & noBelowInvoice & noDiscChange
  rowsToDelete <-  rownames(rowsCancelled[rowsToDelete_boolean, ])
  
  rowsCancelled_reduced <- rowsCancelled[!rowsToDelete_boolean, ]
  
  # Those PO_ids contain rows that have a 'delivered_at' entry but haven't been delivered in reality.
  # It was only able to detect this by looking at the invoice photos. This manual type of deletion
  # won't be done for future data.
  po_ids_manual <- c("17537", "17538", "20093", "21708", "22701", "24801", "55555")
  
  rowsToDelete_manual <- unlist(lapply(po_ids_manual, function(po_id) {
    rowsOfPoId <- rowsCancelled_reduced[rowsCancelled_reduced$PO_id == po_id, ]
    manualRows <- rownames(rowsOfPoId[!is.na(rowsOfPoId$POI_deleted), ])
    return(manualRows)
  }))

  rowsToDelete <- c(rowsToDelete, rowsToDelete_manual)
  transactions <- transactions[!rownames(transactions) %in% rowsToDelete, ]
  
  # rownames(transactions) <- 1:nrow(transactions)
  return(transactions)
}


#---

############## PO_deleted #############

deleteByPODeleted <- function(transactions) {

  transactions_del <- transactions[!is.na(transactions$PO_deleted), ]
  noItemComment <- is.na(transactions_del$item_comment)
  noComments <- is.na(transactions_del$comments)
  noDelivery <- is.na(transactions_del$delivered_at)
  rowsToDelete <- rownames(transactions_del[noItemComment & noComments & noDelivery, ])

  wrongOrder_itemComment <- grepl(transactions_del$item_comment, pattern = "WRONG")
  wrongOrder_comments <- grepl(transactions_del$comments, pattern = "WRONG")
  
  transactions_wrong <- transactions_del[wrongOrder_itemComment | wrongOrder_comments, ]
  rowsToDelete <- c(rowsToDelete, rownames(transactions_wrong))
  
  transactions <- transactions[!rownames(transactions) %in% rowsToDelete, ]
  return(transactions)
}

#---

############# Quantity Cleaning ################

# In around 1000 cases the quantity got updated by item_comments of the form "QTY x", where x refers to the updated 
# quantity of the order. In 950 of those cases the quantitiy-column itself has been updated as well to the given number.
# In almost all of the 50 cases left it seems to have simply been forgotten to update the quantity-column to the new 
# value. Looking at the attached invoices proofs this hypothesis.

# In a lot of cases the original quantitiy is given, because the item_comment has the structure "QTY x FROM y", where 
# y would be the original quantitiy of the order. As there is barely never any reason given for the update of the
# quantitiy, we don't track the original quantity. It makes no sense to assume that this was the true demand, because
# it is at least as likely that the pharmacy simply had to update the quantity because they realized they need more or
# less than they originally ordered. This would imply that the updated quantitiy is usually identical to the true
# demand of the pharmacy for that order.

# There are 11 cases in which there is both a stock-out and a quantity-change comment. One could think that it 
# would be reasonable to use the original quantity that was placed before the stock-out occured as the real demand. 
# This has one big downside, though: If the desired amount couldn't be delivered it is quite likely that the 
# pharmacy went ahead and ordered the missing amount at a later point of time. If we adjusted the quantity to the
# original amount we would therefor overestimate the real demand over the whole the whole time horizon.

quantityCleaning <- function(transactions) {

  patterns <- c("QTY\\s*[[:alpha:]]*\\s*[JADI]*\\s*[0-9]+",
               "[[:alpha:]]*\\s*KUANTITI\\s*[[:alpha:]]*\\s*[0-9]+")
  
  pattern <- paste0(patterns, collapse = "|")
  # When there is no "QTY" item_comment, str_extract simply returns NA. This is useful because we can use this
  # to easily create a new quantity_updated column.
  quantityChanged <- grepl(transactions$item_comment, pattern = pattern)
  quantity_comments <- str_extract(transactions$item_comment, pattern = pattern)
  quantity_new <- str_extract(quantity_comments, pattern = "[0-9]+")
  quantity_new <- as.numeric(quantity_new)
  
  quantityInconsistent <- transactions$quantity != quantity_new
  quantityInconsistent[is.na(quantityInconsistent)] <- FALSE
  
  quantity_updated <- transactions$quantity
  quantity_updated[quantityInconsistent] <- quantity_new[quantityInconsistent]

  transactions <- add_column(transactions, quantity_updated, .after = "quantity") 
  transactions <- add_column(transactions, quantityChanged, .after = "quantity_updated")
  transactions <- add_column(transactions, quantityInconsistent, .after = "quantityChanged")
  
  # While scanning the data I found one row, where the original quantity instead of the seemingly updated quantity 
  # had been delivered. We include this one extra treatment for that row just for the sake of it.

  row_index <- rownames(transactions) == "72080"
  original_quantity <- transactions$quantity[row_index]
  
  transactions[row_index, "quantityChanged"] <- FALSE
  transactions[row_index, "quantityInconsistent"] <- FALSE
  transactions[row_index, "quantity_updated"] <- original_quantity
  
  return(transactions)
}

#---

############## Price Cleaning ##################

# WICHTIG: Es gibt sehr oft den Kommentar der Form "distributor_id - PRICE CHANGING" oder "distributor_id PRICE CHANGING"
# Hierbei müssen wir darauf achten, nicht die distributor_id als neue Zahl einzulesen. 

# Ferner gibt es einzelne 
# Kommentare bspw. der Form "1". Es ist davon auszugehen, dass dies eher einen unit_discount oder eine Quantity
# angeben sollte. Da wir nicht sicher sagen können, was gemeint ist, lassen wir diese Kommentare einfach weg.
# Für das priceCleaning müssen wir dementsprechend eine Abfrage der Form "newPrice > 9" einbauen, da davon auszugehen
# ist, dass einzelne Zahlen keinen Preis angeben sollen. 

priceCleaning <- function(transactions) {

  item_comments <- transactions$item_comment
  
  patterns_newPricesGiven <- c("HNA\\s*-*\\s*[[:alpha:]]*\\s*[[:alpha:]]*\\s*[[:alpha:]]*\\s*[0-9,.]{3,}",
                               "HARGA\\s*[[:alpha:]]*\\s*[[:alpha:]]*\\s*[0-9,.]{3,}",
                               "PRICE\\s*[0-9,.]{3,}",
                               "[0-9,.]{3,}[,]\\sPRICE\\s*CHANGING")
  
  
  newPricesGiven_index <- grepl(item_comments, pattern = paste0(patterns_newPricesGiven, collapse = "|"))
  newPricesGiven_comment <- item_comments[newPricesGiven_index]
  
  newPrices_unclean <- str_extract(newPricesGiven_comment, pattern = paste0(patterns_newPricesGiven, collapse = "|"))
  newPrices_unclean <- str_extract(newPrices_unclean, pattern = "[0-9]+[,.]*[0-9]*")
  
  newPrices_clean <- sub(newPrices_unclean, pattern = "[,.]+", replacement = "")
  newPrices <- as.numeric(newPrices_clean)
  
  #---
  
  digitComments_index <- !grepl(item_comments, pattern = "[[:alpha:]]+|[/]") & !is.na(item_comments)
  digitComments <- item_comments[digitComments_index]
  
  # Handling of the special case that there is both a new discount and a new price specified by the comment at 
  # the same time
  digitComments_mixed_index <- grepl(digitComments, pattern = "[,]\\s")
  digitComments_mixed <- digitComments[digitComments_mixed_index]
  
  digitComments_mixed_split <- str_split(digitComments_mixed, pattern = "[,]\\s")
  
  digitComments_mixed_clean <- sapply(digitComments_mixed_split, function(comment_split) {
    priceComment <- comment_split[!grepl(comment_split, pattern = "%")]
    return(priceComment)
  })
  digitComments[digitComments_mixed_index] <- digitComments_mixed_clean
  
  # Every comment that is part of digitComments and contains a '%' sign is refering to a discount instead of a price
  # and therefor has to be dropped.
  
  digitComments_index[which(digitComments_index)][grepl(digitComments, pattern = "%")] <- FALSE
  digitComments <- digitComments[!grepl(digitComments, pattern = "%")]
  
  digitComments_clean <- sub(digitComments, pattern = "[,.]+", replacement = "")
  newPrices_digitComments <- as.numeric(digitComments_clean)
  
  # Putting stuff together
  priceChanged_index <- newPricesGiven_index | digitComments_index
  
  unit_selling_price_updated <- transactions$unit_selling_price
  unit_selling_price_updated[newPricesGiven_index] <- newPrices
  unit_selling_price_updated[digitComments_index] <- newPrices_digitComments
  
  # Create logical features tracking the price changes
  priceInconsistent <- transactions$unit_selling_price != unit_selling_price_updated
  
  priceChanged <- grepl(item_comments, pattern = "HNA|PRICE") | 
    digitComments_index | 
    grepl(transactions$comments, pattern = "PRICE")
  
  # Handling of special cases: 
  
  # 1) 'HNA REGULER': 
  # When the comment 'HNA REGULER' is given, it means that price we extracted refers to the
  # original price. In this case the corresponding entry for 'priceInconsistent' has to be FALSE, because we don't
  # have an updated price to compare with. Further the updated price has to be set identical to the unit_selling_price,
  # because either the price has been updated already or there is no updated price available.
  regulerPrice_index <- grepl(item_comments, pattern = "HNA\\sREGULER\\s*[0-9]+")
  unit_selling_price_updated[regulerPrice_index] <- transactions$unit_selling_price[regulerPrice_index]
  priceInconsistent[regulerPrice_index] <- FALSE
  
  # 2) 'Price too low':
  # When the grabbed updated price is below 100 it is pretty safe to assume that a mistake was made here. The
  # unit_selling_prices are usually a lot higher than 100. For comparison: 1€ equals 16.145 as of 12.05.20.
  priceTooLow_index <- unit_selling_price_updated < 100
  unit_selling_price_updated[priceTooLow_index] <- transactions$unit_selling_price[priceTooLow_index]
  priceChanged[priceTooLow_index] <- FALSE
  priceInconsistent[priceTooLow_index] <- FALSE
  
  transactions <- add_column(transactions, unit_selling_price_updated, .after = "unit_selling_price")
  transactions <- add_column(transactions, priceChanged, .after = "unit_selling_price_updated")
  transactions <- add_column(transactions, priceInconsistent, .after = "priceChanged")
  
  # Special treatment is necessary for the 
  
  return(transactions)
}

#---

########### Discount Cleaning ###############

# The function discountCleaning creates 3 new columns:
# 1) discountsChanged (class = logical): When 'discountsChanged' is TRUE this simply implies that the discount has
# been updated after the order has been placed. This gives no information, whether the 'unit_discount' column
# has been updated or not. It only implies that there is a comment indicating that there has been a change of
# the unit_discount.
# 2) discountInconsistent (class = logical): When 'discountsInconsistent' is TRUE this implies that there has been
# an update of the unit_discount after the order has been placed but the unit_discount column hasn't been adjusted,
# 3) unit_discount_updated (class = numeric): Contains the updated unit_discount for every order. If the unit_discount
# hasn't been updated, unit_discount and unit_discount_updated are obviously identical.

# NOTE: It has to be discussed, whether orders that haven't been delivered because of a discount change are kept in 
# the data. The reasoning behind this would be that those rows create artificial demand.

discountCleaning <- function(transactions) {
  
  disc_comments <- transactions$item_comment
  
  disc_comments_index <- grepl(disc_comments, pattern = "DISK|DISC|%")
  
  disc_comments[!disc_comments_index] <- NA
  disc_comments <- str_extract(disc_comments, pattern = "[0-9,.]+%|[0-9,.]+\\s%")
  disc_comments <- str_extract(disc_comments, pattern = "[0-9,.]+")
  disc_comments <- sub(disc_comments, pattern = ",", replacement = ".")
  
  # toCheck <- grepl(transactions$item_comment, pattern = "DIS[.][0-9]")
  # 
  # test <- sapply(transactions$item_comment[toCheck], function(comment) {
  #   comment2 <- str_extract(comment, pattern = "[0-9,.]+%|[0-9,.]+\\s%")
  #   comment2 <- str_extract(comment2, pattern = "[0-9,.]+")
  #   comment2 <- sub(comment2, pattern = ",", replacement = ".")
  #   comment_split <- str_split(comment2, pattern = "[.]")[[1]]
  #   
  #   numeric_index <- !is.na(as.numeric(comment_split))
  #   firstNumeric_index <- min(which(numeric_index))
  # 
  #   if(firstNumeric_index == 1) {
  #     return(comment2)
  #     
  #   } else if(sum(numeric_index) == 1) {
  #     return(comment_split[numeric_index])
  #     
  #   } else {
  #     
  #     # The sole purpose of the next line is to make using the paste-function easier in the next step.
  #     numeric_index[firstNumeric_index] <- FALSE
  #     cleanComment <- paste0(comment_split[firstNumeric_index], ".", paste0(comment_split[numeric_index]))
  #     return(cleanComment)
  #   }
  # })
  # browser()
  
  disc_comments[!is.na(disc_comments)] <- sapply(disc_comments[!is.na(disc_comments)], function(comment) {
    comment_split <- str_split(comment, pattern = "[.]")[[1]]
    
    numeric_index <- !is.na(as.numeric(comment_split))
    firstNumeric_index <- min(which(numeric_index))
    
    if(firstNumeric_index == 1) {
      return(comment)
      
    } else if(sum(numeric_index) == 1) {
      return(comment_split[numeric_index])
      
    } else {
      
      # The sole purpose of the next line is to make using the paste-function easier in the next step.
      numeric_index[firstNumeric_index] <- FALSE
      cleanComment <- paste0(comment_split[firstNumeric_index], ".", paste0(comment_split[numeric_index]))
      return(cleanComment)
    }
  })
  discounts <- as.numeric(disc_comments) / 100
  
  discountInconsistent <- transactions$unit_discount != discounts
  discountInconsistent[is.na(discountInconsistent)] <- FALSE
  
  discounts[!discountInconsistent] <- transactions$unit_discount[!discountInconsistent]
  unit_discount_updated <- discounts
  
  discountChanged <- disc_comments_index

  transactions <- add_column(transactions, unit_discount_updated, .after = "unit_discount")
  transactions <- add_column(transactions, discountChanged, .after = "unit_discount_updated")
  transactions <- add_column(transactions, discountInconsistent, .after = "discountChanged")
  
  return(transactions)
}

#---


