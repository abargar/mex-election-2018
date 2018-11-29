library(RSQLite)
library(DBI)
library(dplyr)
library(ggplot2)
library(lmPerm)

perm_fun <- function(x, n1, n2){
  n <- n1 + n2
  idx_b <- sample(1:n, n1)
  idx_a <- setdiff(1:n, idx_b)
  mean_diff <- mean(x[idx_b]) - mean(x[idx_a])
  return(mean_diff)
}

trimlimit <- 10 + 1

con = dbConnect(drv=RSQLite::SQLite(), dbname="mexico_urls.db")

res <- dbSendQuery(con, "SELECT * FROM popularity_by_entity WHERE type IS NOT 'tweet_status'
                   and strftime('%Y-%m-%d', created_at) >= '2018-06-12';")

df <- dbFetch(res) #read.csv("recent_popularity_by_entity.csv")
features <- select(df, id_str, created_at, user_id, timestamp, type)

temp <- df %>%
  group_by(type) %>%
  arrange(desc(max_retweets)) %>%
  slice(trimlimit:n()) %>%
  arrange(max_retweets) %>%
  slice(trimlimit:n())

#summary(aovp(max_retweets ~ type, data=temp))
ggplot(temp, aes(x=type, y=max_retweets)) + geom_boxplot()
#leveneTest(max_retweets ~ type, data = temp)

# Levene's Test for Homogeneity of Variance (center = median)
#           Df F value    Pr(>F)    
# group      2  405.27 < 2.2e-16 ***
#       830568  

temp <- df %>%
  group_by(type) %>%
  arrange(desc(max_quotes)) %>%
  slice(trimlimit:n()) %>%
  arrange(max_quotes) %>%
  slice(trimlimit:n())

summary(aovp(max_quotes ~ type, data=temp, perm = "Prob"))
ggplot(temp, aes(x=type, y=max_quotes)) + geom_boxplot()
leveneTest(max_quotes ~ type, data = temp)

# Levene's Test for Homogeneity of Variance (center = median)
#           Df F value    Pr(>F)    
# group      2  250.21 < 2.2e-16 ***
#       830568                      
# ---
# Signif. codes:  0 '***' 0.001 '**' 0.01 '*' 0.05 '.' 0.1 ' ' 1

# mean(temp$max_quotes[temp$type == "image"])
# [1] 0.7472479
# > mean(temp$max_quotes[temp$type == "video"])
# [1] 1.808455
# > mean(temp$max_quotes[temp$type == "url"])
# [1] 0.4287673

temp <- df %>%
  group_by(type) %>%
  arrange(desc(max_likes)) %>%
  slice(trimlimit:n()) %>%
  arrange(max_likes) %>%
  slice(trimlimit:n())

summary(aovp(max_likes ~ type, data=temp))
ggplot(temp, aes(x=type, y=max_likes)) + geom_boxplot()
leveneTest(max_likes ~ type, data = temp)



# Clear the result
dbClearResult(res)

# Disconnect from the database
dbDisconnect(con)