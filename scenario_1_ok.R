library(sparklyr)
library(ggplot2)
library(dplyr)
library(anytime)
library(data.table)
library(ggfortify)
library(zoom)
library(sqldf)
library(zoo)

########################################---SPARKLYR---##########################################
config <- spark_config()
config$sparklyr.defaultPackages <- c("org.mongodb.spark:mongo-spark-connector_2.11:2.2.1")
sc <- spark_connect(master = "local", config = config) 
########################################-----------##########################################


########################################---MONGO/SPARK---##########################################
uri_txin <- "mongodb://localhost/bitcoin.tx"
txin<-spark_read_source(sc,"tx","com.mongodb.spark.sql.DefaultSource",
                             list(spark.mongodb.input.uri = uri_txin,
                                  keyspace = "bitcoin",
                                  table = "tx"),
                             memory = FALSE)

uri_txtime <- "mongodb://localhost/bitcoin.txtime"
txtime<-spark_read_source(sc,"txtime","com.mongodb.spark.sql.DefaultSource",
                               list(spark.mongodb.input.uri = uri_txtime,
                                    keyspace = "bitcoin",
                                    table = "txtime"),
                               memory = FALSE)
########################################------##########################################

################Prova query su R dataframe##########################
# txin_r=collect(txin)                #non carica nemmeno...
# txtime_r=collect(txtime)
# 
# sqldf("SELECT * FROM txtime WHERE (unixtime<1252995200)")
# sqldf("SELECT * FROM txin WHERE (txID<300000)")
####################################################################

########################################---QUERY---##########################################
library(DBI)
txtime_preview <- dbGetQuery(sc, "SELECT * FROM txtime WHERE unixtime>1233446400 AND 
                             unixtime<1306800000") #dal 01/02/2009 al 31/05/2011
txin_preview <- dbGetQuery(sc, "SELECT * FROM tx WHERE (txID<587812)") #trovato in mongodb con query sul mx in quel unixtime
########################################-----------##########################################


########################################---MERGE E MODIFICA---###############################
merged_df<-merge(txtime_preview, txin_preview, by="txID")
merged_df$addrID=NULL
merged_df$blockID=NULL
merged_df$n_inputs=NULL
merged_df$n_outputs=NULL
merged_df = na.omit(merged_df)
merged_Rtbl<-merged_df %>% collect
merged_plot<- mutate(merged_Rtbl, unixtime=anydate(unixtime), value=(value/1000000))
per_giorno<- merged_plot %>% group_by(unixtime)%>%
    summarise(value=sum(value))
per_mese = merged_plot %>% mutate(unixtime = as.yearmon(unixtime)) %>% group_by(unixtime) %>%
  summarise(value=sum(value))
########################################-----------##########################################
# rm(txtime_preview, txin_preview, merged_df, merged_Rtbl)
# 
# txtime_preview_2 <- dbGetQuery(sc, "SELECT * FROM txtime WHERE unixtime BETWEEN 1306800000 AND 1309900000") #fino al 31/05/2011
# txin_preview_2 <- dbGetQuery(sc, "SELECT * FROM tx WHERE txID BETWEEN 587812 AND 687812")

########################################---PLOT---##########################################

####################Plot per giorno

options(scipen=999) 

ggplot(merged_plot, aes(unixtime)) + geom_histogram()


ggplot(merged_plot, aes(unixtime)) +
  geom_histogram(binwidth = 1)+
  coord_cartesian(ylim=c(0, 6000))

ggplot(per_giorno, aes(unixtime, value)) +
  geom_smooth() +
  scale_size_area(max_size = 2)

#problema con il count delle transazini, il summarise va fatto prima
ggplot(per_giorno, aes(unixtime)) + geom_histogram()
ggplot(per_giorno, aes(unixtime)) +
geom_histogram(binwidth = 1) +
coord_cartesian(ylim=c(0, 6000))  
ggplot(per_giorno, aes(unixtime)) + geom_density()

model <- glm(value~unixtime, family=poisson, data=per_giorno)
ggplot(per_giorno,aes(unixtime,value))+geom_point(colour="red")+geom_line(aes(y=fitted(model)), colour="blue")
########################################-----------##########################################


###########################Plot per mese
ggplot(per_giorno, aes(unixtime, value)) +
  geom_smooth() +
  scale_size_area(max_size = 2)

#problema con il count delle transazini, il summarise va fatto prima

ggplot(per_mese, aes(unixtime)) +
  geom_histogram(binwidth = 1) +
  coord_cartesian(ylim=c(0, 6000))  
ggplot(per_mese, aes(unixtime)) + geom_density()

model <- glm(value~unixtime, family=poisson, data=per_mese)
ggplot(per_mese,aes(unixtime,value))+geom_point(colour="red")+geom_line(aes(y=fitted(model)), colour="blue")

