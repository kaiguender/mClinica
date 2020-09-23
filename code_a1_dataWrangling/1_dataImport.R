############## Packages needed ####################

library(tidyverse)
library(Gmisc)

#---

############## Execution of Functions ##############

path <- paste0(getwd(), "/TransactionTables_new")
transactions <- dataImport_main(path)

write.csv2(transactions, file = "transactions_original.csv")
saveRDS(transactions, file = "transactions_original.rds")

sum(transactions$unit_discount == "0")

# ---

############### Main Function #################

# This function takes as input the path to the folder where all transaction tables are stored. 
# IMPORTANT NOTE: The given folder is supposed to only contain the transaction tables and no other files. Other files will
# lead to an error.
# After importing all the different tables they are merged together and all duplicates created by overlap of the files 
# are deleted.

dataImport_main <- function(path) {
  
  transactions_list <- readTransactions(path)
  transactions <- mergeTransactions(transactions_list)
  
  return(transactions)
}

# ---

############### Reading in of Data ##################

# Read in transaction tables automatically (and create new files with proper naming)

# This function takes as input the path to the folder where all transaction tables are stored. 
# IMPORTANT NOTE: The given folder is supposed to only contain the transaction tables and no other files. Other files will
# lead to an error.
readTransactions <- function(path) {
  
  transactions_fileNames <- list.files(path)
  
  transactions_list <- list()
  for(i in seq_along(transactions_fileNames)) {
    readPath <- paste0(path, "/", transactions_fileNames[i])
    
    table <- read.csv2(readPath, 
                       numerals = "no.loss", 
                       stringsAsFactors = FALSE)
    
    # minDate <- as.Date(min(table$Ordered_date))
    # maxDate <- as.Date(max(table$Ordered_date))
    # writeFile <- paste0("Transactions_", minDate, "_", maxDate, ".csv")
    # write.csv2(table, file = writeFile, row.names = FALSE)
    
    transactions_list <- append(transactions_list, list(table))
    # nameTable <- paste0("Transactions_", transactions_fileNames[i])
    # names(transactions_list)[length(transactions_list)] <- nameTable
  }
  return(transactions_list)
}

# ---
  
############# Merge imported tables ##################

# There can be overlaps in the provided excel files of the transactions. It is important to get rid of those
# duplicates. 
# The rows are sorted by the PO_ids. Row names are assigned afterwards simply by choosing 1, 2, 3...., nrow().

mergeTransactions <- function(transactions_list) {

  mergedTransactions <- fastDoCall("rbind", transactions_list) %>%
    distinct()
  
  orderedTransactions <- mergedTransactions[order(mergedTransactions$PO_id), ]
  rownames(orderedTransactions) <- 1:nrow(orderedTransactions) 
  
  return(orderedTransactions)
}
