transactions %>%
  filter(sku_code_prefix == "123333") %>%
  select(distributor_id)

distrData <- transactions %>%
  group_by(sku_code_prefix) %>%
  summarise(distrCount = uniqueN(distributor_id))

distrData %>%
  summary()

topSkuPrefixes <- getTopSkus(transactions, topPercentSkus = 0.3, returnPrefixes = TRUE)
distrData %>%
  filter(sku_code_prefix %in% topSkuPrefixes) %>%
  summary()

#---

distrData <- transactions %>%
  group_by(sku_code) %>%
  summarise(distrCount = uniqueN(distributor_id))

distrData %>%
  summary()

topSkuCodes <- getTopSkus(transactions, topPercentSkus = 0.3, returnPrefixes = FALSE)
distrData %>%
  filter(sku_code %in% topSkuCodes) %>%
  summary()
