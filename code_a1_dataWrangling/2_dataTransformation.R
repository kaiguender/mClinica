############## Packages needed ####################

library(lubridate)
library(tidyverse)
library(Gmisc)
library(fasttime)
library(doParallel)
library(foreach)

# ---

############ Execution of Functions #############

transactions_original <- readRDS(file = "transactions_original.rds")
transactions_unclean <- dataTransformation_main(transactions_original)

# write.csv2(transactions_unclean, file = "transactions_unclean.csv")
saveRDS(transactions_unclean, file = "transactions_unclean.rds")

# ---

############### Main Function ####################

dataTransformation_main <- function(transactions) {
  
  transactions <- transformDates(transactions)
  transactions <- getOrderedDates(transactions)
  transactions <- getSkuCodePrefix(transactions)
  transactions <- getCleanComments(transactions)
  transactions <- getPackageFeatures(transactions)
  transactions <- transformDiscounts(transactions)
  
  return(transactions)
}

# ---

############### Transform Date Features ##################

# There are a bunch of date and time features that have to be transformed accordingly into the POSIXct format.
# As a timezone we chose "Asia/Jakarta" because all the data has been recorded in Jakarta.

transformDates <- function(transactions) {

  dateTimeFeatures <- c("Ordered_date", "PO_deleted", "PO_updated", 
                        "POI_deleted", "invoice_input_time",
                        "delivered_at", "received_date", "claimed_date", 
                        "paid_date", "invoice_updated", "invoice_deleted")

  transactions_dateFeatures <- transactions[, dateTimeFeatures]
  
  transformSingleColumn <- function(column) {
    column <- as.POSIXct(strptime(column, "%Y-%m-%d %H:%M:%OS", tz = "Asia/Jakarta"))

    attr(column, "tzone") <- "Asia/Jakarta"
    return(column)
  }
  
  transactions[, dateTimeFeatures] <- transactions[, dateTimeFeatures] %>%
    mutate_all(transformSingleColumn)
  
  # Invoice date needs a special treatment because it contains no time stamp and is given in a
  # different format
  transactions$invoice_date <- as.POSIXct(strptime(transactions$invoice_date, "%d/%m/%Y", tz = "Asia/Jakarta"))
  
  return(transactions)
}

# ---

################# Additional Date Features #################

getOrderedDates <- function(transactions) {

  Ordered_date_date <- date(transactions$Ordered_date)
  Ordered_date_dayOfWeek <- lubridate::wday(Ordered_date_date, week_start = 1, label = FALSE)
  Ordered_date_dayOfMonth <- day(transactions$Ordered_date)
  
  Ordered_date_week <- isoweek(transactions$Ordered_date)
  Ordered_date_month <- month(transactions$Ordered_date)
  Ordered_date_year <- year(transactions$Ordered_date)
  
  Ordered_date_yearweek <- (Ordered_date_year == 2018)*0 + 
    (Ordered_date_year == 2019)*52 + (Ordered_date_year == 2020)*104 + Ordered_date_week
  
  # The date '2019-12-30' for example has the isoweek 1 and would therefor receive the Ordered_date_yearweek 
  # value of 53 despite the fact that '2019-12-30' actually belongs to week 105.
  endOfYearCorrection_index <- which(Ordered_date_month == 12 & Ordered_date_week == 1)

  Ordered_date_yearweek[endOfYearCorrection_index] <- Ordered_date_yearweek[endOfYearCorrection_index] + 52

  transactions <- add_column(transactions, Ordered_date_date, .after = "Ordered_date")
  transactions <- add_column(transactions, Ordered_date_dayOfWeek, .after = "Ordered_date_date")
  transactions <- add_column(transactions, Ordered_date_dayOfMonth, .after = "Ordered_date_dayOfWeek")
  transactions <- add_column(transactions, Ordered_date_week, .after = "Ordered_date_dayOfMonth")
  transactions <- add_column(transactions, Ordered_date_yearweek, .after = "Ordered_date_week")
  transactions <- add_column(transactions, Ordered_date_month, .after = "Ordered_date_yearweek")
  transactions <- add_column(transactions, Ordered_date_year, .after = "Ordered_date_month")
  return(transactions)
}

# ---

############### sku_code Prefix ##################

# Every sku_code has the following structure: "ID103273-1". The prefix "ID103273" uniquely 
# identifies a product name. The suffix "-1" on the other hand identifies the belonging package size / package type.
# Because of that we create a new feature called "sku_code_prefix".

getSkuCodePrefix <- function(transactions) {
  
  emptyIndex <- which(transactions$sku_code == "")   
  transactions$sku_code[emptyIndex] <- NA
  
  sku_code_prefix <- transactions$sku_code
  sku_code_prefix[-emptyIndex] <- str_split(sku_code_prefix[-emptyIndex], pattern = "ID|-", simplify = TRUE)[, 2]
  
  transactions <- add_column(transactions, sku_code_prefix, .after = "pharmacy_name")
  
  return(transactions)
}

# ---

############### Cleaning Comments ##################

# We transform the features "item_comment" and "comments" to only contain capital letters. Every empty entry is transformed
# into a NA-entry.

getCleanComments <- function(transactions) {
  
  transactions$item_comment <- toupper(transactions$item_comment)
  transactions$comments <- toupper(transactions$comments)
  
  emptyIndex <- !grepl(transactions$item_comment, pattern = "[[:alpha:]]+|[[:digit:]]+")   
  transactions$item_comment[emptyIndex] <- NA

  emptyIndex <- !grepl(transactions$comments, pattern = "[[:alpha:]]+|[[:digit:]]+") 
  transactions$comments[emptyIndex] <- NA
  
  return(transactions)
}

# ---

############### Additional Package Features ##################

getPackageFeatures <- function(transactions) {
  
  # Sometimes there is a free-extra product delivered with the package. In all cases those free extra products are
  # labeled through "(FREE product x...)" in the text of the package. 
  freeExtraProduct <- grepl(transactions$package, pattern = "FREE")
  transactions <- add_column(transactions, freeExtraProduct, .after = "package")
  
  if(any(freeExtraProduct))
  {
    transactions[freeExtraProduct, "package"] <- str_split(transactions$package[freeExtraProduct], 
                                                           pattern = "\\(FREE",
                                                           simplify = TRUE)[, 1]
  }
  
  # Creating promotion features
  promoDiscount_comment <- str_extract(transactions$package, pattern = "PROMO\\s*[0-9,.]+\\s*%")
  promoDiscount <- str_extract(promoDiscount_comment, pattern = "[0-9,.]+")
  promoDiscount <- gsub(promoDiscount, pattern = ",", replacement = ".")
  promoDiscount[is.na(promoDiscount)] <- 0
  promoDiscount <- as.numeric(promoDiscount) / 100
  
  hasPromotion <- promoDiscount != 0
  
  transactions <- add_column(transactions, hasPromotion, promoDiscount, .after = "package")
  
  package_mod <- str_split(transactions[hasPromotion, ]$package, 
                           pattern = "PROMO\\s*[0-9 ,.%]+\\s[[:alpha:]]*\\s*\\)\\s*",
                           simplify = TRUE)[, 2]
  
  transactions$package[hasPromotion] <- package_mod
  
  packages <- transactions$package
  
  # There are 2 cases where the package size '0, 6 ML' is given. We need a special treatment here
  specialCases_index <- grepl(packages, pattern = "[[:digit:]]+[,]\\s[[:digit:]]+")
  specialCases <- grep(packages, value = TRUE, pattern = "[[:digit:]]+[,]\\s[[:digit:]]+")
  specialCases_clean <- gsub(specialCases, pattern = ",\\s", replacement = ".")
  packages[specialCases_index] <- specialCases_clean
  
  # "," as seperator between numbers occurs frequently and causes trouble if we don't replace it with "."
  quantityStrings <- str_extract_all(transactions$package, pattern = "[0-9]+\\.*\\,*[0-9]*", simplify = TRUE)

  quantity_perPackage <- apply(quantityStrings, MARGIN = 1, function(row) {
    row_cleaned <- grep(x = row, pattern = "[[:digit:]]", value = TRUE)
    row_cleaned <- gsub(x = row_cleaned, pattern = ",", replacement = ".")
    row_cleaned <- as.numeric(row_cleaned)
    
    return(prod(as.numeric(row_cleaned)))
  })
  transactions <- add_column(transactions, quantity_perPackage, .after = "package")
  
  return(transactions)
}

#---

############### Discount Transformation ##################

transformDiscounts <- function(transactions) {
  
  noDiscount_index <- !grepl(transactions$unit_discount, pattern = "[0-9]", )
  transactions$unit_discount[noDiscount_index] <- "0"
  
  # Now that the empty entries are set to "0" we can transform the unit_discounts to numeric
  transactions$unit_discount <- as.numeric(transactions$unit_discount)
  
  # When there is a discount that is higher than 1 it is reasonable to assume that accidently the
  # percentage instead of the actual discount was given
  valuesToCorrect_index <- transactions$unit_discount > 1
  valuesCorrected <- transactions$unit_discount[valuesToCorrect_index] / 100
  transactions$unit_discount[valuesToCorrect_index] <- valuesCorrected

  return(transactions)
}

#---
