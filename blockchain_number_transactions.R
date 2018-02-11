library(sparklyr)
library(ggplot2)
library(dplyr)
library(anytime)
library(qdapTools)
library(zoo)


config <- spark_config()

config$sparklyr.defaultPackages <- c("org.mongodb.spark:mongo-spark-connector_2.11:2.2.1")
sc <- spark_connect(master = "local", config = config) 
uri_blockhash <- "mongodb://localhost/bitcoin_full.blockhash"
blockhash<-spark_read_source(sc,"blockhash","com.mongodb.spark.sql.DefaultSource",
                             list(spark.mongodb.input.uri = uri_blockhash,
                                  keyspace = "bitcoin_full",
                                  table = "blockhash"),
                             memory = FALSE)

ok = blockhash %>% collect
ok$bhash=NULL
ok = mutate (ok, btime=anydate(btime))

# ok = ok %>% 
#   mutate(btime = as.Date(btime, '%Y/%m/%d')) %>%
#   group_by(MM = format(btime, '%m'), YY = format(btime, '%Y')) %>% 
#   summarise(txs = sum(txs))

ok = ok %>% mutate(btime = as.yearmon(btime)) %>%
  group_by(btime) %>%
  summarise(txs=sum(txs))



options(scipen=999) 


ggplot(ok, aes(x=btime, y=txs)) +
  geom_point(shape=1) +
  scale_colour_hue(l=50) + # Use a slightly darker palette than normal
  geom_smooth(method=lm,   # Add linear regression lines
              se=TRUE)

ggplot(ok, aes(btime, txs)) +
  geom_point(aes(size = txs)) +
  geom_smooth()

model <- glm(txs~btime, family=poisson, data=ok)
ggplot(ok,aes(btime,txs))+geom_point()+geom_line(aes(y=fitted(model)))
