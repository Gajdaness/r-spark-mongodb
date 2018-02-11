library(sparklyr)
library(ggplot2)
library(dplyr)
library(anytime)
library(scales)
library(zoo)
library(lubridate)

########################################---SPARKLYR
config <- spark_config()
# config$`sparklyr.shell.driver-memory` <- "4G"
# config$`sparklyr.shell.executor-memory` <- "4G"
# config$`spark.yarn.executor.memoryOverhead` <- "1g"
config$sparklyr.defaultPackages <- c("org.mongodb.spark:mongo-spark-connector_2.11:2.2.1")
sc <- spark_connect(master = "local", config = config) 
########################################

########################################---MONGO/SPARK
uri_coll_out <- "mongodb://localhost/bitcoin.coll_out"
uri_data_prova<- "mongodb://localhost/bitcoin.data_prova"
coll_out<-spark_read_source(sc,"coll_out","com.mongodb.spark.sql.DefaultSource",
                        list(spark.mongodb.input.uri = uri_coll_out,
                             keyspace = "bitcoin",
                             table = "coll_out"), #dati aggregati per giorno.
                        memory = FALSE)

data<-spark_read_source(sc,"data_prova","com.mongodb.spark.sql.DefaultSource",
                            list(spark.mongodb.input.uri = uri_data_prova,
                                 keyspace = "bitcoin",
                                 table = "data_prova"), #dati aggregati per mese.
                            memory = FALSE)


########################################

########################################---COLLECT DATA
# to_plot = coll_out %>% collect

# to_plot = mutate (to_plot, unixtime=anydate(unixtime), totalAmount=(totalAmount/1000000))
# to_plot = na.omit(to_plot)
# to_plot = to_plot %>% mutate(unixtime = as.yearmon(unixtime)) %>% group_by(unixtime) %>%
#    summarise(count=sum(count), value=sum(totalAmount))
# to_plot = na.omit(to_plot)
#########################################
per_mese = data %>% collect
per_giorno = coll_out %>% collect

# colnames(per_mese)=c("mese", "count", "value")
colnames(per_giorno)=c("data", "count", "value")

per_mese= per_mese %>% mutate(data = anydate(data), value=(value/1000000))
per_giorno= per_giorno %>% mutate(value=(value/1000000))

# per_giorno[data=null]<-NA
# per_giorno <- na.omit(per_giorno)

#########################################---PLOT
options(scipen=999) #toglie visualizzazione esponenziale

#####plot dati per mese
model <- glm(value~data, family = poisson, data=per_mese)
ggplot(per_mese,aes(data,value))+geom_point(color="red")+geom_line(aes(y=fitted(model)), color="blue")

ggplot(per_mese, aes(x=data, y=value))+geom_line(color="grey")+geom_point(color="red")

ggplot(per_mese, aes(x=data, y=value)) +
  geom_point(shape=1, colour="red") +
  scale_colour_hue(l=50)


#plot per giorno
ggplot(per_giorno, aes(data, value))+geom_point(color="red")+geom_line(color="lightblue")

ggplot(per_giorno, aes(x=data, y=value))+geom_line(color="grey")+geom_point(color="red")+ 
  coord_cartesian(ylim=c(0, 750000000))


ggplot(per_giorno, aes(x=data, y=value)) +
  geom_point(shape=1, colour="red") +
  scale_colour_hue(l=50) + # Use a slightly darker palette than normal
  coord_cartesian(ylim = c(0, 750000000))
  