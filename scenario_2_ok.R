library(sparklyr)
library(ggplot2)
library(dplyr)
library(anytime)
library(data.table)
library(ggfortify)
library(zoom)
library(anytime)
library(data.table)
library(zoo)


########################################---SPARKLYR
config <- spark_config()
config$sparklyr.defaultPackages <- c("org.mongodb.spark:mongo-spark-connector_2.11:2.2.1")
sc <- spark_connect(master = "local", config = config) 
########################################

########################################---MONGO/SPARK
uri_txin <- "mongodb://localhost/bitcoin.txin_fino_2011"
txin<-spark_read_source(sc,"txin_fino_2011","com.mongodb.spark.sql.DefaultSource",
                        list(spark.mongodb.input.uri = uri_txin,
                             keyspace = "bitcoin",
                             table = "txin_fino_2011"),
                        memory = FALSE)
########################################

#######################################---MERGE/MODIFICA
per_giorno <- txin %>% 
  collect  %>%
  mutate(unixtime = anydate(unixtime), value=(value/1000000)) %>%
  group_by(unixtime) %>% 
  summarise( count = n(), value=sum(value))

per_mese = mutated %>% mutate(unixtime = as.yearmon(unixtime)) %>% 
  group_by(unixtime) %>% summarise(value=sum(value), count=sum(count))
#######################################



########################################---PLOT per giorno
options(scipen=999)
model <- glm(value~unixtime, family=poisson, data=per_giorno)
ggplot(per_giorno,aes(unixtime,value))+geom_point(color="red")+geom_line(aes(y=fitted(model)),
                                                                         color="blue")+
  coord_cartesian(ylim = c(0, 10000000))

ggplot(per_giorno, aes(unixtime, value))+geom_point(color="firebrick")

ggplot(per_giorno, aes(x=unixtime, y=value))+geom_line(color="grey")+geom_point(color="red")+
  coord_cartesian(ylim = c(0, 70000000))

ggplot(data = per_giorno, aes(x = unixtime, y = value)) + 
  geom_point(color='blue') +
  geom_smooth(method = "lm", se = FALSE)


########################################---PLOT per mese
model <- glm(value~unixtime, family=poisson, data=per_mese)
ggplot(per_mese,aes(unixtime,value))+geom_point(color="red")+geom_line(aes(y=fitted(model)), color="blue")

ggplot(per_mese, aes(unixtime, value))+geom_point(color="firebrick")

ggplot(per_mese, aes(x=unixtime, y=value))+geom_line(color="grey")+geom_point(color="red")

ggplot(data = per_mese, aes(x = unixtime, y = value)) + 
  geom_point(color='blue') +
  geom_smooth(method = "glm", se = FALSE)


