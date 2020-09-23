
##### Molecule Analysis ######

transactions <- readRDS("transactions_clean.rds")
data_Train <- readRDS("training_withoutDistr_extrapolated.rds")

unique(data_Train$molecule)

transactions %>%
  group_by(sku_code) %>%
  slice(1) %>%
  group_by(molecule) %>%
  summarise(count = n()) %>%
  arrange(desc(count)) %>%
  as.data.frame()

uniqueN(transactions$sku_code)

# Allgemeine Idee der Umsetzung

# 1) Die Hauptidee ist, einen Pool aus einzelnen Wirkstoffen zu erstellen, die wir alle als einzelne Strings 
# abgespeichert haben. Hierzu gehen wir alle Molecule Einträge durch und versuchen identische Wirkstoffe
# auf syntaktischer Ebene miteinander zu identifizieren
# 2) Nachdem wir diesen Pool gebildet haben, checken wir für jeden einzelnen Wirkstoff, ob dieser im ausgewählten
# Molecule-Eintrag enthalten ist und bilden auf dieser Basis eine Dummy-Variable

# PROBLEME:
# 1) Den Pool zu erstellen, sollte relativ machbar sein. Mehr Probleme wird bereiten, hierauf die einzelnen Molecule
# Einträge durchzugehen und zu checken, ob der jeweilige Wirkstoff dort enthalten ist. Das Hauptproblem stellen hierbei
# die vielen unterschiedlichen Schreibweisen für identische Begriffe dar (siehe Beispiele unten). Bei der Erstellung
# des Pools können wir ohne Probleme einfach identische Wörter auf Basis von Distanz usw. miteinander identifizieren,
# bei den Molecule Einträgen haben wir hierbei größere Schwierigkeiten.
# IDEEN ZUR LÖSUNG
# a) Wir zerlegen jeden einzelnen Molecule Eintrag eines jeden sku_code identisch zu unserem Vorgang bei der Erstellung
# des Pools. Um dies sinnvoll durchführen zu können, speichern wir jeden Molecule Eintrag als einzelnen Listen-Eintrag,
# welche wir nacheinander durchlaufen.

# 1) Wir sollten versuchen Einträge wie "mg" loszuwerden. Vor diesen erscheint stets eine Zahlenangange, welche als
# Mengenangabe dient wie "50 mg". Dies könnten wir ausnutzen, um diese Einträge zu entfernen.
# Grundsätzlich sollte es Sinn machen, einzelne Zahlen zu entfernen. Zahlen, die einen Wirkstoff spezifieren,
# sind normalerweise an einen Character direkt angeschlossen ohne Leerzeichen. Hier sollte man mal testen, was
# wir alles catchen, wenn wir nach isolierten Zahlen suchen. 
# BEACHTE WICHTIG: "MG" kann nicht nur für Milligramm, sondern wohl auch für Magnesium stehen (manchmal sogar
# MGANESIUM" geschrieben). Es muss also nach dem Entfernen nicht jeder Eintrag mit "MG" entfernt worden sein.
# SPEZIALFALL: ""SILYMARIN PHYTOSOME35 MG". Hier werden wir die Zahlen nicht los, weil das Leerzeichen fehlt.

# 2) Es gibt zig verschiedene uneinheitliche Schreibweisen für bspw. "Vitamin B12": 
# vit B12, B12, vitamin B12, vut B12
# Bei dem Spezialfall der Vitamine könnte man sich überlegen, Kombinationen aus "B" und direkt nachfolgenden Zahlen
# zu isolieren und ausschließlich mit diesen zu arbeiten. 
# BEACHTE: Dies könnte man auch bei Vitamin A, B, C, D und E versuchen, indem wir nach einzel stehenden Buchstaben
# suchen. Muss man mal testen, was hierbei schlussendlich gecatched wird :/
# BEACHTE2: Es gibt aber auch wohl Vitamin D3! Die Kombi "Buchstabe+Zahl" liegt also nicht nur bei Vitamin B vor!
# 
# "POLYMYXIN B SULFATE" ist ein Antibiotikum und könnte Schwierigkeiten bei der hier vorgeschlagenen Suche nach
# Einzelbuchstaben zum Auffinden von Vitamin A/B/C/D/E bereiten.
# Das gleiche gilt für "CALCIUM D PANTOTENATE".

# 3) Auch gibt es Fälle wie "Povidon iodine" und "povidone-iodine". Hier könnten wir über String-Distance versuchen
# ranzukommen. BEACHTE: Hierbei String-Distance nur auf Strings einer gewissen Mindestlänge anwenden (sofern die Distance
# nicht skaliert ist), denn sonst werden auch Fälle wie "B6" und "B1" als identisch erkannt. 

# 4) Schwierig wird es in Fällen wie "Betamethason valerate" und "betamethasone". Nicht nur gibt es hier wiederum
# eine unterschiedliche Schreibweise für Betamethasone/Betamethason, sondern es gibt hier ein komplettes extra Wort.
# Wie sollten wir diese Fälle als identisch isolieren können?


#---

# 0) Transformation zu Großbuchstaben



#---

molSplit <- unlist(str_split(transactions$molecule, pattern = ",\\s|;\\s", simplify = FALSE))
molSplit <- toupper(molSplit)
molSplit <- unique(molSplit)

# 1) Isolierte Zahlen Entfernen 
# Gefundene Probleme: Einzig der String "Tiap 0,6 mL sirup mengandung vitamin A" macht hierbei Probleme, denn die
# Information "Vitamin A" geht verloren. Immerhin haben alle Fälle, in denen dies auftritt, die gemeinsame Info "Tiap",
# welche erhalten bleibt.
# Dieser Vorgang hat sogar den Effekt, dass alle Einträge wie "MG" oder "ML" verschwinden.
# Wenn wir auf den gesamtem Transaktions-Daten arbeiten gibt es noch Beispiele von Bestandteilen, die wir als Kollateral-
# schaden abschneiden. Da es sich nur um Einzelfälle handelt, verzichten wir erst einmal darauf, diese Fälle extra zu
# behandeln.

isolatedDigits_index <- grepl(molSplit, pattern = "\\s[[:digit:]]+")

molSplit[isolatedDigits_index] <- str_split(molSplit[isolatedDigits_index], 
                                            pattern = "\\s[[:digit:]]+", 
                                            simplify = TRUE)[, 1]
molSplit <- unique(molSplit)
molSplit
# grep(molSplit, pattern = "%", value = TRUE)

# 2) Isolierung von Vitaminen als einzelne Buchstaben
# "CALCIUM D PANTOTENATE" und "POLYMYXIN B SULFATE" werden ebenfalls gecatched. 

# Nötig, um die Einzelbuchstaben der Vitamine korrekt auslesen zu können. Wenn wir bspw. das Leerzeichen am Anfang 
# weglassen würden, so hätte dies zur Konsequenz, dass wir durch das pattern "[A-E]{1}[0-9]*\\s" auch Wörter
# auslesen, die mit A-E enden.
molSplit_mod <- paste0(" ", molSplit, " ")
vitaminIndex <- grepl(molSplit_mod, pattern = "\\s[A-E]{1}[0-9]*\\s")
test <- str_extract(molSplit_mod[vitaminIndex], pattern = "\\s[A-E]{1}[0-9]*\\s")
molSplit_mod[vitaminIndex]


###### Stock Outs ######

# HINWEIS: Es wurden maximal etwa 100 Zeilen aus den originalen Transaktions-Daten entfernt, welche einen
# Stock Out beinhaltet haben. Dies habe ich hier auch überprüft. Wir können transactions_clean folglich
# problemlos zur weiteren Analyse heranziehen. 

transactions <- readRDS("SavedData/transactions_clean.rds")

# OUT_OF_STOCK, x STOCK OUT, STOCK OUT, x STCOK OUT
comments_stockOut <- grepl(transactions$comment, pattern = "STOCK|STCOK")
itemComment_stockOut <- grepl(transactions$item_comment, pattern = "STOCK")
stockOut_index <- comments_stockOut | itemComment_stockOut

stockOut <- rep(FALSE, nrow(transactions))
stockOut[stockOut_index] <- TRUE
transactions <- add_column(transactions, stockOut)

stockOutData <- transactions %>%
  group_by(sku_code_prefix, sku_code) %>%
  summarise(stockOuts_sku = sum(stockOut)) %>%
  group_by() %>%
  mutate(stockOutsPercent_sku = stockOuts_sku / sum(stockOuts_sku)) %>%
  group_by(sku_code_prefix) %>%
  mutate(stockOutsPercent_prefix = sum(stockOutsPercent_sku),
         stockOuts_prefix = sum(stockOuts_sku)) %>%
  arrange(desc(stockOutsPercent_prefix)) %>%
  as.data.frame()

stockOutData_prefix <- stockOutData %>%
  group_by(sku_code_prefix) %>%
  slice(1) %>%
  select(sku_code_prefix, stockOuts_prefix, stockOutsPercent_prefix) %>%
  arrange(desc(stockOutsPercent_prefix)) %>%
  group_by() %>%
  mutate(stockOutsPercent_cum = cumsum(stockOutsPercent_prefix)) %>%
  as.data.frame()

stockOutData <- left_join(stockOutData, 
                          stockOutData_prefix[, c("sku_code_prefix", "stockOutsPercent_cum")],
                          by = "sku_code_prefix")

nameData <- transactions %>%
  group_by(sku_code) %>%
  slice(1) %>%
  select(sku_code, name)

stockOutData <- left_join(stockOutData, nameData, by = "sku_code")

saveRDS(stockOutData_prefix, "SavedData/stockOutData.rds")

#---

# Untersuchung der Verteilung der StockOuts

stockOutData %>%
  group_by(sku_code_prefix) %>%
  slice(1) %>%
  filter(stockOutsPercent_cum <= 0.8) %>%
  nrow() / uniqueN(stockOutData$sku_code_prefix) # 18,2% aller SKUs sind für 80% aller Stock-Outs zuständig

stockOuts_top <- (stockOutData %>%
  group_by(sku_code_prefix) %>%
  slice(1) %>%
  filter(stockOutsPercent_cum <= 0.8) %>%
  select(sku_code_prefix) %>%
  c())$sku_code_prefix

stockOuts_plotData <- stockOutData %>%
  group_by(sku_code_prefix) %>%
  slice(1) %>%
  group_by() %>%
  arrange(stockOutsPercent_cum) %>%
  mutate(index = 1:n()) 

plot(x = stockOuts_plotData$index, stockOuts_plotData$stockOutsPercent_cum, type = "l")

##### Payment Volume #####

transactions <- readRDS("SavedData/transactions_clean.rds")

payData <- transactions %>%
  group_by(sku_code_prefix) %>%
  summarise(payVolume = sum(quantity * unit_selling_price * (1 - unit_discount))) %>%
  arrange(desc(payVolume)) %>%
  group_by() %>%
  mutate(payVolumePercent = payVolume / sum(payVolume)) %>%
  mutate(payVolumePercent_cum = cumsum(payVolumePercent)) %>%
  as.data.frame()
 
payData %>%
  filter(payVolumePercent_cum <= 0.8) %>%
  nrow() / uniqueN(payData$sku_code_prefix) # 15,4% aller SKUs sind für 80% des Payment-Volumes verantwortlich.

payData_top <- (payData %>%
  filter(payVolumePercent_cum <= 0.8) %>%
  select(sku_code_prefix) %>%
  c())$sku_code_prefix

payData_plotData <- payData %>%
  mutate(index = 1:n())

plot(x = payData_plotData$index, payData_plotData$payVolumePercent_cum, type = "l")

#---

###### Order Frequency ######

transactions <- readRDS("SavedData/transactions_clean.rds")

countData <- transactions %>%
  group_by(sku_code_prefix) %>%
  summarise(count = n()) %>%
  arrange(desc(count)) %>%
  group_by() %>%
  mutate(countPercent = count / sum(count)) %>%
  mutate(countPercent_cum = cumsum(countPercent)) %>%
  mutate(index = 1:n() / n())

count_top <- (countData %>%
  filter(countPercent_cum <= 0.8) %>%
  select(sku_code_prefix) %>%
  c())$sku_code_prefix

length(count_top) / uniqueN(transactions$sku_code_prefix) # 24,2% aller SKUs sind für 80% aller Transaktionen
# verantwortlich

plot(x = countData$index, countData$countPercent_cum, type = "l")



###### Compare Revenue / StockOut / Häufigkeit #####

# Der Vergleich zeigt, dass ein Großteil der wichtigsten Payment-SKUs und StockOut-SKUs Teil der häufigsten SKUs
# ist. 
# Da das Gewicht der häufigsten SKUs auf etwas mehr SKUs verteilt ist, sind nur die Hälfte der SKUs, die 80% aller
# Transaktionen ausmachen, Teil der SKUs, die für 80% des Payments oder der StockOuts verantwortlich sind.
# Leider überschneiden sich auch StockOuts und Payment weniger als erhofft.

sum(payData_top %in% stockOuts_top) / length(payData_top) # 61,5%
sum(payData_top %in% count_top) / length(payData_top) # 85,3%

sum(stockOuts_top %in% payData_top) / length(stockOuts_top) # 51,9%
sum(stockOuts_top %in% count_top) / length(stockOuts_top) # 79,1%

sum(count_top %in% payData_top) / length(count_top) # 54,3%
sum(count_top %in% stockOuts_top) / length(count_top) # 59,7%

importantSkus_small <- intersect(payData_top, stockOuts_top)
importantSkus_small <- intersect(count_top, importantSkus_small)

saveRDS(importantSkus_small, "importantSkus_small.rds")

payData %>%
  filter(sku_code_prefix %in% importantSkus_small) %>%
  select(payVolumePercent) %>%
  sum() # 59%

stockOutData %>%
  filter(sku_code_prefix %in% importantSkus_small) %>%
  group_by(sku_code_prefix) %>%
  slice(1) %>%
  group_by() %>%
  select(stockOutsPercent_prefix) %>%
  sum() # 51,5%

length(importantSkus_small) # 227

#---

# HAUPTGEWICHTUNG REVENUE
# Wir wählen lediglich diejenigen SKUs, die 80% des Umsatzes erzeugen und reichern diese mit den top10 SKUs an, die
# am häufigsten bestellt werden

freqTop10 <- countData[1:(ceiling(nrow(countData) * 0.1)), ]$sku_code_prefix

importantSkus_small <- unique(c(payData_top, freqTop10))

saveRDS(importantSkus_small, "importantSkus_small.rds")

payData %>%
  filter(sku_code_prefix %in% importantSkus_small) %>%
  select(payVolumePercent) %>%
  sum() # 81,6

stockOutData %>%
  filter(sku_code_prefix %in% importantSkus_small) %>%
  group_by(sku_code_prefix) %>%
  slice(1) %>%
  group_by() %>%
  select(stockOutsPercent_prefix) %>%
  sum() # 61,1

#---

# Nun gehen wir einen anderen Weg: Anstatt die verschiedenen Skus miteinander zu schneiden, legen wir den Fokus
# auf die top-Payment Skus und erweitern diese um die Top10 StockOut-Skus und die Top10 OrderCount-Skus.

countSkus <- uniqueN(countData$sku_code_prefix)
top10_index <- seq(from = 1, to = round(ceiling(0.1 * countSkus)), by = 1)
top10_freq <- countData$sku_code_prefix[top10_index]
importantSkus <- union(top10_freq, payData_top)

countSkus <- uniqueN(stockOutData_prefix$sku_code_prefix)
top10_index <- seq(from = 1, to = round(ceiling(0.1 * countSkus)), by = 1)
top10_stockOut <- stockOutData_prefix$sku_code_prefix[top10_index]
importantSkus <- union(importantSkus, top10_stockOut)

saveRDS(importantSkus, "importantSkus.rds")

payData %>%
  filter(sku_code_prefix %in% importantSkus) %>%
  select(payVolumePercent) %>%
  sum() # 84%

stockOutData %>%
  filter(sku_code_prefix %in% importantSkus) %>%
  group_by(sku_code_prefix) %>%
  slice(1) %>%
  group_by() %>%
  select(stockOutsPercent_prefix) %>%
  sum() # 77%

length(importantSkus) # 523
length(payData_top) # 387

#---

# Nun bilden wir einfach die riesige Gruppe aller wichtigen SKUs

importantSkus_big <- union(stockOuts_top, payData_top)
importantSkus_big <- union(importantSkus_big, count_top)

saveRDS(importantSkus_big, "SavedData/importantSkus_big.rds")

payData %>%
  filter(sku_code_prefix %in% importantSkus_big) %>%
  select(payVolumePercent) %>%
  sum() # 89%

stockOutData %>%
  filter(sku_code_prefix %in% importantSkus_big) %>%
  group_by(sku_code_prefix) %>%
  slice(1) %>%
  group_by() %>%
  select(stockOutsPercent_prefix) %>%
  sum() # 87%

length(importantSkus_big) # 750
length(payData_top) # 387

#---

payData_top <- (payData %>%
                  filter(payVolumePercent_cum <= 0.80) %>%
                  select(sku_code_prefix) %>%
                  c())$sku_code_prefix

stockOuts_top <- (stockOutData %>%
                    group_by(sku_code_prefix) %>%
                    slice(1) %>%
                    filter(stockOutsPercent_cum <= 0.80) %>%
                    select(sku_code_prefix) %>%
                    c())$sku_code_prefix

payData %>%
  filter(sku_code_prefix %in% importantSkus) %>%
  select(payVolumePercent) %>%
  sum() # 59%

stockOutData %>%
  filter(sku_code_prefix %in% importantSkus) %>%
  group_by(sku_code_prefix) %>%
  slice(1) %>%
  group_by() %>%
  select(stockOutsPercent_prefix) %>%
  sum() # 51,5%

#---

###### Statistics of chosen SKUs ######

data <- readRDS("SavedData/training_withoutDistr_extrapolated.rds")
importantSkus <- readRDS("SavedData/importantSkus.rds")

data_Train <- data %>%
  filter(sku_code_prefix %in% importantSkus)

transactions <- readRDS("SavedData/transactions_clean.rds")
revenueData <- readRDS("SavedData/revenueData.rds")
stockOutData <- readRDS("SavedData/stockOutData.rds")

revenueData %>%
  filter(sku_code_prefix %in% importantSkus) %>%
  summarise(sum(payVolumePercent)) # 84%

stockOutData %>%
  filter(sku_code_prefix %in% importantSkus) %>%
  summarise(sum(stockOutsPercent_prefix)) # 77%

orderData <- transactions %>%
  filter(Ordered_date_yearweek >= 66) %>%
  mutate(orderCountAll = n()) %>%
  group_by(sku_code_prefix) %>%
  mutate(orderShare = n() / orderCountAll) %>%
  slice(1) %>%
  as.data.frame()

orderData %>%
  filter(sku_code_prefix %in% importantSkus) %>%
  summarise(sum(orderShare)) # 71%

length(importantSkus) / uniqueN(transactions[transactions$Ordered_date_yearweek >= 66, ]$sku_code_prefix)

#---

###### Correlation / Feature Selection ######

data <- readRDS("training_withoutDistr_extrapolated.rds")
topSkus <- getTopSkus(transactions, returnPrefixes = TRUE, topPercentSkus = 0.2)

data <- data %>%
  filter(sku_code_prefix %in% topSkus)

data <- data %>%
  select(-molecule)

# Preprocessing Functions
data <- detrendDemand(data)
data <- centerDemand(data)
data <- scalePrices(data)
data <- recalcDemandFeatures(data)

# # Additional Features
corrData <- getCorrelatedSkus(data)
data <- addCorrLags(data, corrData)

data <- setorderv(data, cols = c("sku_code_prefix", "Ordered_date_yearweek"))

dataSku <- data %>%
  filter(sku_code_prefix == unique(data$sku_code_prefix)[24]) %>%
  group_by(Ordered_date_yearweek) %>%
  summarise(demand = sum(demand))

plot(dataSku$Ordered_date_yearweek, dataSku$demand, type = "l")

data <- data %>%
  select(-c(sku_code, 
            sku_code_prefix,
            Ordered_date_yearweek,
            unit_selling_price,
            mean,
            sd,
            detrendFactor)) 
data <- as.data.frame(data)

#---

# LINEARE KOMBINATIONEN
# Sehr interessant: Die Funktion linearCombs gibt uns die Empfehlung die Feature "cumDemand_lag2_SkuCode",
# "cumDemand_lag3_SkuCode", "cumDemand_lag4_SkuCode" und "cumDemand_lag2_SkuPrefix", "cumDemand_lag3_SkuPrefix",
# "cumDemand_lag4_SkuPrefix" zu entfernen. 
# Man müsste dies mal mit Zettel und Stift nachrechnen: Es könnte wirklich gut sein, dass diese Feature problemlos
# aus den normalen demand-lags und cumDemand_lag1 gewonnen werden können. 
linearCombs <- findLinearCombos(data)
colnames(data[, linearCombs$remove]) # In

# KORRELIERTE FEATURE
corrs <-  cor(data)
foundCorrs <- findCorrelation(corrs, cutoff = 0.9)
colnames(data[, foundCorrs])

dataTest <- data[1:5000, ]

set.seed(44)
control <- rfeControl(functions = rfFuncs, method = "cv", number = 4)

# run the RFE algorithm
resRFE <- rfe(x = dataTest[, -1], y = dataTest[, 1], sizes = 11 * 1:8, rfeControl = control)

# plot the results
plot(resRFE, type = c("g", "o"))

predictors(resRFE)

setdiff(colnames(data), predictors(resRFE))

#---

##### PharmacyCount ######

lmData <- data %>%
  group_by(Ordered_date_yearweek) %>%
  slice(1) %>%
  select(Ordered_date_yearweek, pharmActive_sinceBeginning)

lmFit <- lm(data = lmData, formula = pharmActive_sinceBeginning ~ Ordered_date_yearweek)
summary(lmFit)

plot(x = lmData$Ordered_date_yearweek, y = lmData$pharmActive_sinceBeginning)
abline(lmFit$coefficients)

#---

lmData <- data %>%
  group_by(Ordered_date_yearweek) %>%
  slice(1) %>%
  select(Ordered_date_yearweek, pharmAverageOrdersMonth)

lmFit <- lm(data = lmData, formula = pharmAverageOrdersMonth ~ Ordered_date_yearweek)
summary(lmFit)

plot(x = lmData$Ordered_date_yearweek, y = lmData$pharmAverageOrdersMonth)
abline(lmFit$coefficients)
