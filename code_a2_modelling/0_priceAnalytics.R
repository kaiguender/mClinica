######### Price Analysis #########

# Hier untersuchen wir die uns von Assandra für Wholesaler 1 bereitgestellten Preisdaten.

require("readxl")
priceData <- read.csv("Friday Pricing BKA.csv", stringsAsFactors = FALSE)[, -1]

priceData <- priceData %>%
  rename(Ordered_date = date,
         product_id = Product_ID,
         sku_code = SKU_Code,
         name = Product_Name,
         unit_selling_price = sell_price,
         unit_discount = discount) %>%
  select(-net_price)

transformDates <- function(dates) {
  
  dates_mod <- sapply(dates, USE.NAMES = FALSE, function(date) {
    dateSplit <- str_split(date, pattern = "-", simplify = TRUE)
    dateSplit[2] <- switch(dateSplit[2],
                           "Jan" = "01",
                           "Feb" = "02",
                           "Mar" = "03", 
                           "Apr" = "04",
                           "May" = "05",
                           "Jun" = "06",
                           "Jul" = "07",
                           "Aug" = "08",
                           "Sep" = "09",
                           "Oct" = "10", 
                           "Nov" = "11",
                           "Dec" = "12")
    
    dateString <- paste0(dateSplit, collapse = "-")
    # dateToAdd <- as.POSIXct(strptime(dateString, "%d-%m-%Y", tz = "Asia/Jakarta"))
    
    # attr(dateToAdd, "tzone") <- "Asia/Jakarta"
    
    return(dateString)
  })
  dates_mod <- as.POSIXct(strptime(dates_mod, "%d-%m-%Y", tz = "Asia/Jakarta"))
  return(dates_mod)
}
priceData$Ordered_date <- transformDates(priceData$Ordered_date)

priceData <- getOrderedDates(priceData) %>%
  select(-c(Ordered_date, Ordered_date_month, Ordered_date_dayOfMonth, Ordered_date_week, Ordered_date_year,
            product_id)) %>%
  select(sku_code, everything())


priceData %>%
  group_by(sku_code) %>%
  summarise(count = n()) %>%
  arrange(desc(count))

priceData[1:10, ]
dataDistr <- readRDS("training_withDistr_extrapolated.rds")

dataBKA <- dataDistr %>% 
  filter(distributor_id == 1)

# Wir wollen die angegebenen Preise mit den tatsächlich georderten Preisen vergleichen. 
# Hierzu benötigen wir eine sinnvolle Gegenüberstellung. 
# 1) Wir extrapolieren die gegebenen Preis-Daten dahingehend, dass wir stets davon ausgehen, dass ein vorliegender Preis
# stets eine ganze Woche aktiv war. Wenn in der darauffolgenden Woche kein neuer Preis vorliegt, so gehen wir davon aus,
# dass das Produkt nun nicht mehr zur Verfügung stand. 

weeksPriceData <- priceData %>%
  filter(sku_code == "ID100053-2") %>%
  select(Ordered_date_yearweek) %>%
  c()

weeksBKA <- dataBKA %>%
  filter(sku_code == "ID100053-2") %>%
  select(Ordered_date_yearweek) %>%
  c()

skuBKA <- unique(transactions[transactions$distributor_id == 1, ]$sku_code)
skuData <- unique(priceData$sku_code)

skuCodesShared <- intersect(skuBKA, skuData)

sum(skuBKA %in% skuData) / length(skuBKA) # 80,6%
sum(skuData %in% skuBKA) / length(skuData) # 78,2%

# Für eine nicht unerhebliche Menge an SKUs, welche in den letzten 1,5 Jahren bestellt wurden, liegen keine Preisdaten
# vor. Gleichzeitig wurde eine ähnliche Anzahl an SKUs, für welche Preisdaten vorliegen, überhaupt nicht bestellt.

priceData <- priceData %>%
  filter(sku_code %in% skuCodesShared)

dataBKA <- dataBKA %>%
  filter(sku_code %in% skuCodesShared)

# Wenn man die vorliegenden Preis-Daten vergleicht, so stimmen diese größtenteils überein. Allerdings gibt es immer
# wieder Fälle, in denen leicht unterschiedliche Preise vorliegen. In diesen Fällen ist es wohl so, dass 
# bei den Abweichungen oft noch der alte Preis bestellt wurde, was sich wohl daher erklärt, dass wir stets die Preise
# von Freitagen (Ende der Woche) gegeben haben. Dementsprechend macht es durchaus Sinn anzunehmen, dass die Preise
# eher am Ende der Woche upgedated werden. 

impCols <- c("sku_code", "unit_selling_price", "unit_discount", "Ordered_date_yearweek")

priceDataComb <- left_join(priceData, dataBKA[, impCols], by = c("Ordered_date_yearweek", "sku_code"))
priceDataFil <- priceDataComb %>%
  drop_na()

priceDataFil[1:30, c("sku_code", "Ordered_date_yearweek", 
                     "unit_selling_price.x", "unit_selling_price.y", 
                     "unit_discount.x", "unit_discount.y")]


# Die gegebenen Preisdaten sollen vor allem unsere Daten verbessern während der Zeitpunkte, in denen keine Bestellungen
# vorliegend waren. Dementsprechend sollten wir uns anschauen, wie sich die Preise in Wochen verhalten, in denen keine
# Bestellungen vorlagen.

impCols <- c("sku_code", "unit_selling_price", "unit_discount", "Ordered_date_yearweek")

dataBKA_fil <- dataBKA %>%
  filter(demand == 0)

priceDataComb <- left_join(dataBKA_fil[, impCols], priceData, by = c("Ordered_date_yearweek", "sku_code"))
priceDataFil <- priceDataComb %>%
  drop_na()

pricesCompare <- priceDataFil[, c("sku_code", "Ordered_date_yearweek", 
                                  "unit_selling_price.x", "unit_selling_price.y", 
                                  "unit_discount.x", "unit_discount.y")] %>%
  filter(unit_selling_price.x != unit_selling_price.y | unit_discount.x != unit_discount.y) %>%
  arrange(sku_code)

pricesCompare_diff <- pricesCompare %>%
  mutate(price.x = unit_selling_price.x * (1 - unit_discount.x),
         price.y = unit_selling_price.y * (1 - unit_discount.y)) %>%
  filter((price.x - price.y) / price.y >= 0.05)

# There are very minor cases in which the prices that we interpolated deviate from the prices provided to us.
pricesCompare_diff


# Das Problem der interpolierten Preise scheint nicht zu sein, dass diese stark abweichen, sondern dass 
# wir Preisdaten in die Trainingsdaten aufnehmen, die so überhaupt nicht existieren: Soll heißen, in einigen Wochen
# wir das vorliegende Produkt von einem Großhändler gar nicht angeboten. Dieses Problem sollte aber deutlich stärker
# ins Gewicht fallen, wenn wir die Trainingsdaten auf der Ebene der Wholesaler betrachten. Aktuell ignorieren wir
# diese Ebene komplett und dementsprechend sollte diese Problematik eher weniger ins Gewicht fallen. Es ist davon
# auszugehen, dass irgendein Wholesaler das vorliegende Produkt stets im Portfolio hat.

# Wir sollten nun vergleichen, wie oft Preisdaten vorliegen in Wochen, welche wir extrapoliert haben!

impCols <- c("sku_code", "unit_selling_price", "unit_discount", "Ordered_date_yearweek")

dataBKA_fil <- dataBKA %>%
  filter(demand == 0)

priceDataComb <- left_join(dataBKA_fil[, impCols], priceData, by = c("Ordered_date_yearweek", "sku_code"))

priceDataFil <- priceDataComb %>%
  drop_na()

nrow(dataBKA) # 45313
nrow(dataBKA_fil) # 30811
nrow(priceDataFil) # 2141

# Wir interpolieren unfassbar viele Wochen: 2/3 aller Wochen-Preis-Daten sind für Wholesaler BKA (1) interpoliert.
# Gleichzeitig liegen in gerade mal 7% der Fälle Preisdaten vor für Wochen-Preis-Daten, welche wir extrapoliert haben.
# Folglich interpolieren wir viele Zeitpunkte, obwohl hier der Wholesaler das Produkt überhaupt nicht angeboten hat.
# DIES IST EIN GROßES PROBLEM!!

# Trifft dies auch zu, wenn wir uns auf die top SKUs beschränken?
topSkus <- getTopSkus(transactions, rankingByPrefix = TRUE, returnPrefixes = FALSE, topPercentSkus = 0.1)

dataBKA_filSku <- dataBKA %>%
  filter(sku_code %in% topSkus) 

dataBKA_fil <- dataBKA_filSku %>%
  filter(demand == 0)

priceData_fil <- priceData %>%
  filter(sku_code %in% topSkus)

priceDataComb <- left_join(dataBKA_fil[, impCols], priceData_fil, by = c("Ordered_date_yearweek", "sku_code"))

priceDataFil <- priceDataComb %>%
  drop_na()

nrow(dataBKA_filSku) # 20575
nrow(dataBKA_fil) # 13315
nrow(priceDataFil) # 909

# Auch hier ergibt sich exakt das Gleiche Bild wie oben, es liegt also keinerlei Verbesserung vor, wenn wir uns nur auf
# die häufigsten SKUs beschränken. 

#---

# UNTERSUCHUNG OHNE WHOLESALER

trainNoDistr <- readRDS("training_withoutDistr_extrapolated.rds")

trainNoDistr %>%
  filter(demand == 0) %>%
  summarise(count = n()) # 22095

nrow(trainNoDistr) # 78285

# Wenn wir die Wholesaler-Ebene verlassen, sind nur noch etwa 22% aller Wochen-Preis-Daten extrapoliert.

interpolCount <- trainNoDistr %>%
  filter(demand == 0) %>%
  group_by(Ordered_date_yearweek) %>%
  summarise(count = n()) %>%
  arrange(Ordered_date_yearweek) %>%
  as.data.frame()

plot(x = interpolCount$Ordered_date_yearweek, y = interpolCount$count, type = "l")

uniqueN(trainNoDistr$sku_code) # 1213

# Die Anzahl der Interpolationen nimmt über den zeitlichen Verlauf stark ab, erreicht aber ein Minimum bei week 100,
# nur um dann wieder stark schwankend etwas anzusteigen. 
# Im Durchschnitt sind wohl stets etwa 1/4 der Preise pro Woche interpoliert (300 von 1213). Aber wie schaut es aus,
# wenn wir uns auf die topSkus beschränken?

topSkus <- getTopSkus(transactions, rankingByPrefix = TRUE, topPercentSkus = 0.2, returnPrefixes = FALSE)
length(topSkus) # 1336

trainNoDistr %>%
  filter(sku_code %in% topSkus) %>%
  group_by(sku_code) %>%
  slice(1) %>%
  nrow() # 907

interpolCount <- trainNoDistr %>%
  filter(sku_code %in% topSkus) %>%
  filter(demand == 0) %>%
  group_by(Ordered_date_yearweek) %>%
  summarise(count = n()) %>%
  arrange(Ordered_date_yearweek) %>%
  as.data.frame()

plot(x = interpolCount$Ordered_date_yearweek, y = interpolCount$count, type = "l")