####### Ideas for Improvement ######

# 1) Problems of simultan training of Skus:
# When we train all skus simultanously we have the problem that our model has no way of knowing which observations belong
# to the same sku. The only real link between the skus belonging to each other are features like cumDemand_skuPrefix and 
# meanPrice_skuPrefix. The question remains if it makes sense to train the model in such a way: Different products behave
# completely differently in terms of price sensibility, base-mean-demand, seasonality etc. 
# In a lot of cases the models seem to have especially a hard time when it comes to guessing the base-demand correctly.
# In order to improve the performance in those cases, it seems reasonable to try to include the following features:
# i) mean_demandSkuPrefix, median_demandSkuPrefix: It remains to clarify on which time horizon we determine those features.
# We could either chose the whole time horizon or simply the last 4 weeks etc.
# ii) unique identifier of each sku_code_prefix: This would have the disadvantage of making the computational time for
# training each model a lot longer depending on how many sku_code_prefixes we train simultanously. 
# 
# In order to evaluate the problems correctly it makes also sense to train all Skus seperately yet again to see how this
# impacts performance. Hypothesis: The predictions aren't necessarily much better on average, but probably a lot more stable.

# 2) Change level of aggregation 
# The most important decision regarding aggregation that we have to make regards the handling of different prices. We have 
# two ways of handling different prices that occured during the same week:
# a) Work with min/mean/max price that are computed on each aggregation level.
# b) Track every price that appeared during the week for the current aggregation level
# 
# Using (b) has the disadvantage that the number of different prices basically stays the same between using skuCode-level
# vs. using skuPrefix-level. It is probably also extremely difficult for our models to predict the demand for each price
# correctly per week. On the other hand this obviously has the advantage of introducing many more observations compared to
# the case when would only work with one weekly observation for each skuCode or skuPrefix respectively.
# On the level of the products themselves we have 3 levels of aggregation that we can work on. On each level we can apply
# the handling of price data either according to (a) or (b). Levels of aggregation are ordered by most aggregated to
# least aggregated:
# i) skuPrefix: Here we really only look at the product itself and don't care about the way it is packaged.
# ii) skuCode: For every product we track also all the ways it can be packaged. Here we don't pay attention to what
# distributor sold each package/product.
# iii) distributor: Not only do we track the way how each product can be packaged, we also track what price has been offered
# by each distributor. As a consequence for every package and every week we always have one observation for each distributor
# that sells the package.


# 3) Extrapolation of prices vs. no extrapolation of prices
# It remains an open question if it makes sense to extrapolate the prices the way we are doing it right now. It introduces
# the big disadvantage that if one massive discount appeared at some time point we might extrapolate this massive
# discount also to the following weeks without there being any demand for it. This can greatly confuse our model: "Why
# is there a massiv discount and no demand?"
# On the other hand extrapolating prices opens the possibility to train every SKU on the whole time horizon and also
# give the model the information that 0 demand can occur. The appearance of 0 demand differs greatly between SKUs. For some
# this happens basically never, for others it happens for several weeks in a row.
# For now the only way to figure out what to do here is to compare the performance of the models between extrapolation
# and no extrapolation.

# 4) Feature Imporantance
# Right now we are training a massive amount of features. Does it really make sense to train using all of them? It would
# make a lot of sense to try to reduce the amount greatly. We have to 3 ways to go about this:
# i) Do univariate analysis on features where it is straightforward to create a hypothesis and prove/disprove it. Good
# examples are features like 'hasPromotion' or 'lagDemand'.
# ii) Use the 'Feature Importance'-functionality of caret to get an overview over feature importance over all features.
# iii) Trail and Error: Simply try modelling with a low number of predictors that we intuitively deem most important.
#
# 'regsubsets' gives some interesting hints and insights. 
# a) The most important feature for a linear model is quantity_perPackage, which makes perfect sense as this gives 
# the models kind of a baseline for the demandOfUnits features to predict. 
# b) Interestingly all skuPrefix-features (both price and lags) never got included in any best model. Maybe there 
# isn't such a big connection between the different packages of each product as we expected. This certainly
# needs further investigation.
# c) unit_discount and pricePerSingleUnit never got included, whereas unit_selling_price and maxPriceSkuCode did.

# 5) Model to Use
# i) It remains an open question what kind of model / models we should be fitting. There are many possibilities from xgBoost
# to neural networks etc. We could also go ahead an use an ensemble in the end, but this has the big disadvantage of not
# being easily interpretable.
# ii) We could also try detrending the data in some way

# 6) Error measure to use
# i) The MASE is surely easiest to interprete and best for comparison among different models, but it has the great disadvantage
# of being only useful when we are predicting a real time series. We can therefor only use the MASE when we are evaluating
# the predictions on a skuPrefix-level and work only with observation per week. This also explains why our models are pretty
# good according to the MASE on a low-aggregate-level: The lower the agg-level, the worse the benchmark gets because using
# a naive-one-step forecasts makes less and less sense the lower the agg-level is.
# ii) Does it really make sense to use the smape?

# 7) Transformation
# It is clear that we have to ensure in some way that our models predict only positive values. One way to achieve this
# is simply transforming the demand-data before training and transforming it back afterwards. This can have a big downside,
# though: The transformation seems to introduce a bias to our model. If we use x^(1/2) as our transformation for example
# we tend to underforecast. If we don't use any transformation we obviously get the problem of possible negative forecasts.
# Apart from that it also seems like we have a slight tendency towards overforecasting in general if we don't use
# any transformation despite the problem of negative forecasts. One idea could be to try a transformation that 
# is convex (log and sqrt are concav).

# 8) Price Data
# It is strange to see that the price has barely any influence on our model's performance. It is quite interesting to look
# at the prices given for sku_code_prefix 102068 when it comes to promotions. From time to time absurdly low prices appear
# but don't get any orders. This could hint at either of 2 things:
# a) Our price Data is flawed: A quick look at the original data suggested that this could be the case. It seems like
# the item_comment giving a new price wasn't refering to whole price of the Promo-bundle but instead to the single
# units of each bundle. This doesn't seem to be the case too often. The question arises how we could handle this special
# case: When the unit_selling_price was really updated (priceInconsistent == TRUE) then the presence of the above special
# case would indicate that the new price is lower than "unit_selling_price / specialPackageSizeGivenByPromoComment".
# This is the case for at most 17 transactions, though.
# b) The pharmacies can make special deals for a limited amount of shipment.
#
# Similar weird things are happening with sku_code ID110555-2 and sku_code ID110555-1. While the latter
# is a Box with 25 tubes per product, the former seems to contain only one tube and yet costs exactly the 
# same amount. It could very well be that working with demandOfUnits introduces yet another possible source
# of error. 

#---

###### To-Do ######

# 1) Splitting vs. noSplitting:
# Compare performance between splitting and no splitting: In order to be able to do this we have to update the output
# the function mClinica_split creates. We want to store the data exactly the same way as we do for mClinica_noSplit.
#
# Ergebnis: Ein kurzer Stichproben-Test hat ergeben, dass getrenntes Training zu keinerlei Verbesserung führt.

# 2) New Features or Identifiers of SKUs
# I guess it makes sense to try both. The identifiers of the skus should only be used if we train using a very low amount
# of skus.

# 3) Agg-Level:
# Decide on which agg-level to work on for now. It is especially important to make a decision on whether to create one
# observation per price for each week for now or not. It would make sense to quickly fit 2 models one time with and 
# the other time without prices and compare the results, but we have to be careful: It could be the case that the effect of
# working with disaggregated-price-data changes depending on the agg-level in terms of distributors etc.

# 4) Feature Importance:
# For now it would be too much effort to do univariate analysises for several features. Instead we try two different
# approaches for now:
# i) Feature importance of caret using different evaluation procedures
# ii) Intuitive feature selection
# The sole purpose for now is to see whether this immediately improves performance or not.



####### Detrending ########

transactions <- readRDS("transactions_clean.rds")
data_train <- readRDS("training_withoutDistr_extrapolated.rds")
topSkus <- getTopSkus(transactions, topPercentSkus = 0.01, returnPrefixes = TRUE)

train_agg <- data_train %>%
  group_by(Ordered_date_yearweek) %>%
  group_by(pharmActive_sinceBeginning, pharmAverageOrdersWeek, add = TRUE) %>%
  summarise(demand = sum(demand))

trainSku <- data_train %>%
  filter(sku_code_prefix == topSkus[25])

trainSku_agg <- trainSku %>%
  group_by(Ordered_date_yearweek) %>%
  group_by(pharmActive_sinceBeginning, pharmAverageOrdersWeek, add = TRUE) %>%
  summarise(demand = sum(demand))

lm <- lm(data = trainSku_agg, formula = demand ~ pharmActive_sinceBeginning + pharmAverageOrdersWeek)
coefs <- coefficients(lm)

trainSku_detrend <- trainSku_agg %>%
  mutate(demand_detrend = demand - coefs[1] - pharmActive_sinceBeginning * coefs[2] - pharmAverageOrdersWeek * coefs[3])

plot(trainSku_agg$demand, type = "l")
plot(detrend(trainSku_agg$demand), type = "l")
plot(trainSku_detrend$demand_detrend, type = "l")

ggplot(trainSku_detrend, aes(x = Ordered_date_yearweek, y = demand)) + 
  geom_line(col = "red") +
  geom_line(aes(x = Ordered_date_yearweek, y = demand_detrend), col = "steelblue") +
  # geom_abline(intercept = coefs[1], slope = coefs[2]) +
  theme_bw()


pharmacyCount_data <- trainSku %>%
  group_by(Ordered_date_yearweek) %>%
  slice(1) %>%
  select(pharmActive_sinceBeginning)

ggplot(pharmacyCount_data, aes(x = Ordered_date_yearweek,
                               y = pharmaciesActive_sinceBeginning)) + 
  geom_point() + 
  geom_line() + 
  ylim(c(0, 1200)) + 
  xlim(c(50, 130)) + 
  geom_abline(intercept = -607.73996, slope = 12.20097)


####### Detrending SkuCode ########

transactions <- readRDS("transactions_clean.rds")
data_train <- readRDS("training_withoutDistr_extrapolated.rds")
topSkus <- getTopSkus(transactions, topPercentSkus = 0.1, returnPrefixes = FALSE)

train_agg <- data_train %>%
  group_by(Ordered_date_yearweek) %>%
  group_by(pharmActive_sinceBeginning, pharmAverageOrdersWeek, add = TRUE) %>%
  summarise(demand = sum(demand))

trainSku <- data_train %>%
  filter(sku_code == topSkus[300])

trainSku_agg <- trainSku %>%
  group_by(Ordered_date_yearweek) %>%
  group_by(pharmActive_sinceBeginning, pharmAverageOrdersWeek, add = TRUE) %>%
  summarise(demand = sum(demand))

lm <- lm(data = trainSku_agg, formula = demand ~ pharmActive_sinceBeginning + pharmAverageOrdersWeek)
coefs <- coefficients(lm)

trainSku_detrend <- trainSku_agg %>%
  mutate(demand_detrend = demand - coefs[1] - pharmActive_sinceBeginning * coefs[2] - pharmAverageOrdersWeek * coefs[3])

plot(trainSku_agg$demand, type = "l")
plot(detrend(trainSku_agg$demand), type = "l")
plot(trainSku_detrend$demand_detrend, type = "l")

ggplot(trainSku_detrend, aes(x = Ordered_date_yearweek, y = demand)) + 
  geom_line(col = "red") +
  geom_line(aes(x = Ordered_date_yearweek, y = demand_detrend), col = "steelblue") +
  # geom_abline(intercept = coefs[1], slope = coefs[2]) +
  theme_bw()


pharmacyCount_data <- trainSku %>%
  group_by(Ordered_date_yearweek) %>%
  slice(1) %>%
  select(pharmActive_sinceBeginning)

#---

###### Package Problems #######

transactions[which(grepl(transactions$package, pattern = "CATCH COVER"))[2], ]$package
index <- which(grepl(transactions$package, pattern = "CATCH COVER"))[11]

skuCodePrefix <- transactions[index, ]$sku_code_prefix

transactions %>%
  filter(sku_code_prefix == skuCodePrefix) %>%
  select(package) %>%
  unique()

(transactions %>%
  filter(sku_code_prefix == skuCodePrefix) %>%
  mutate(pricePerSingleUnit = unit_selling_price_updated / quantity_perPackage) %>%
  select(package, unit_selling_price_updated, pricePerSingleUnit, quantity_perPackage, name) %>%
  unique())

(transactions %>%
    filter(sku_code_prefix == skuCodePrefix) %>%
    mutate(pricePerSingleUnit = unit_selling_price_updated / quantity_perPackage))[467, "invoice_photo"]


#---

# FREE in packages

packageFree <- grep(transactions$package, pattern = "FREE", value = TRUE)

str_split(packageFree, pattern = "FREE", simplify = TRUE)[, 1]
