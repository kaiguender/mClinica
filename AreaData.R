############# Area Data ###############

areaInfo <- readxl::read_xlsx("Additional_Information/current_pbf_pharma_coverage.xlsx")

# Basic analysis of the distributors
# 1) There are 9 wholesalers available in the data. 
# 2) Wholesaler 5 doesn't seem to work together with mClinica anymore: He only appears in 2018 and isn't even part of the area 
# info anymore.
# 3) The wholesalers 640, 1116 and 1123 only joined pretty late into the date. In fact, 1116 and 1123 didn't even join before
# mid April 2020. Wholesaler 640 joined at the begin of the year 2020. 
# 4) The wholesalers 640, 1116 and 1123 also deliver to far less pharmacies according to the coverage-file. Distributor 2
# delivers to 993 pharmacies whereas Distributor 1116 covers only 104 pharmacies.
# 5) With a coverage of 993 pharmacies, distributor 2 covers almost all pharmacies (1066 according to coverage-file).

unique(areaInfo$pbf_id) # 8
unique(transactions$distributor_id) # 9
unique(transactions$distributor_id[-(1:60000)]) # 8
unique(transactions$distributor_id[(1:150000)]) 
unique(transactions$distributor_id[(1:15000)])

pharmacyCountPerDistr <- areaInfo %>%
  group_by(pbf_id) %>%
  summarise(n = n())

pharmacyCountPerDistr
hist(distr_count$n, breaks = 1:6)

#---

# Basic analysis of the pharmacies

# 1) 1066 pharmacies are part of the coverage-document, so this should be equal to the amount of pharmacies that are currently
# using the swipeRX marketplace.
# 2) 809 pharmacies appear at least once in the transactions data. This shows 2 things:
# a) Not every registered pharmacy really uses swipeRX actively
# b) It could be the case that pharmacies undergo a registration process that requieres some time until a pharmacy can finally
# start ordering products. If this is the case, the number of pharmacies currently registered gives a massive hint towards future
# growth!
# 3) Despite there being 8 different wholesalers, no pharmacy is covered by more than 6 wholesalers. 802 pharmacies
# are either covered by 4 or 5 wholesalers. 88 pharmacies are covered by 6 , 94 by 3, 45 by 2 and 37 by 1 wholesalers.

uniqueN(areaInfo$pharmacy_id) # 1066
uniqueN(transactions$pharmacy_id) # 809

distrCount_dist <- areaInfo %>%
  group_by(pharmacy_id) %>%
  summarise(count = n()) %>%
  group_by(count) %>%
  summarise(distrPerPharmacy = n())
distrCount_dist

countsWholesalers <- areaInfo %>%
  group_by(pharmacy_id) %>%
  summarise(count = n())

hist(countsWholesalers$count, breaks = 0:6)

# ---

# Distributions of Pharmacies among Wholesalers

# Almost no pharmacies are exclusively only delivered by one wholesaler. As expected there are quite large
# intersections between all the wholesalers.

# The output of the function 'getWholesalerDistributions' is a list with 3 entries. The entries have to be 
# interpreted the following way:
# 1) 'wholesaler_distributions': Every row and every column refers to one distributor. The entry
# 'wholesaler_distributions[distributor_1, distributor_2]' for example tells you how many pharmacies,
# that are covered by distributor_1, are also covered by distributor_2. 
# 2) 'wholesaler_distributions%': The same as 'wholesaler_distributions', but this time percentages
# instead of counts are given.
# 3) 'exclusivePharmacies': This vector tells you how many pharmacies are exclusively covered by
# one specific distributor

getWholesalerDistributions <- function(areaInfo) {
  
  distributor_ids <- sort(unique(areaInfo$pbf_id))
  
  pharmaciesPerDistr <- lapply(distributor_ids, function(distributor_id) {
    pharmaciesOfDistr <- (areaInfo %>%
                            filter(pbf_id == distributor_id))$pharmacy_id
  })
  distributor_names <- paste0("distributor_", distributor_ids)
  
  results <- lapply(seq_along(pharmaciesPerDistr), function(i) {
    result <- sapply(seq_along(pharmaciesPerDistr), function(j) {
      sum(pharmaciesPerDistr[[j]] %in% pharmaciesPerDistr[[i]])
    })
    names(result) <- distributor_names
    return(result)
  })
  wholesalerDistr <- fastDoCall("rbind", results)
  rownames(wholesalerDistr) <- distributor_names
  
  wholesalerDistr_perc <- t(apply(wholesalerDistr, MARGIN = 1, function(row) {
    row / max(row)
  }))
  
  exclusiveCount <- sapply(seq_along(pharmaciesPerDistr), function(i) {
    sum(!pharmaciesPerDistr[[i]] %in% fastDoCall("c", pharmaciesPerDistr[-i]))
  })
  names(exclusiveCount) <- distributor_names
  
  return(list("wholesaler_distributions" = wholesalerDistr,
              "wholesaler_distributions%" = wholesalerDistr_perc,
              "exclusivePharmacies" = exclusiveCount))
}

wholeSalerDistributions <- getWholesalerDistributions(areaInfo)
wholeSalerDistributions[[2]]
