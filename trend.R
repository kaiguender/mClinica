
data <- readRDS("data_processed/data_noDistr_extrapolated.rds")
transactions <- readRDS("data_processed/transactions_clean.rds")

demand <- data %>%
  group_by(Ordered_date_yearweek) %>%
  summarise(demand = sum(demand),
            revenue = sum(demand * pricePerSingleUnit),
            avgPrice = revenue / demand)

pharmData <- data %>%
  group_by(Ordered_date_yearweek) %>%
  slice(1) %>%
  select(Ordered_date_yearweek, pharmActive_month, pharmActive_sinceBeginning, pharmAverageOrdersMonth)

trendData <- left_join(demand, pharmData, by = "Ordered_date_yearweek")

plot(trendData$demand, type = "l")
plot(y = trendData$avgPrice, x = trendData$Ordered_date_yearweek, type = "l")

simpleLM <- lm(data = trendData, formula = demand ~ Ordered_date_yearweek)
summary(simpleLM)

ggplot(data = trendData, aes(x = Ordered_date_yearweek, y = demand)) +
  geom_line() + 
  geom_abline(slope = simpleLM$coefficients[2], intercept = simpleLM$coefficients[1])

lm <- lm(data = trendData, formula = demand ~ pharmActive_month)
summary(lm)

ggplot(data = trendData, aes(x = pharmActive_month, y = demand)) + 
  geom_point() + 
  geom_abline(slope = lm$coefficients[2], intercept = lm$coefficients[1])
