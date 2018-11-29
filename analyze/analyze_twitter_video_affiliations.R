library(RSQLite)
library(DBI)
library(dplyr)
library(igraph)
library(visNetwork)
library(ggplot2)

con = dbConnect(drv=RSQLite::SQLite(), dbname="mexico_urls.db")

res <- dbSendQuery(con, "select * from twitter_videos;")
twitter_videos <- dbFetch(res) #90,612 records
dbClearResult(res)

source_videos <- twitter_videos[is.na(twitter_videos$source_status_id_str),]
nrow(source_videos) #21,648 rows, aka unique uploads

View(source_videos %>%
       group_by(found_user_id_str) %>%
       summarize(count = n_distinct(id_str) #id_str here is for video
                 )
)
source_videos$duration_millis_num <- as.numeric(source_videos$duration_millis) #saved to separate variable, 'just checking'

num_videos_by_length <- source_videos %>%
  group_by(duration_millis_num) %>%
  summarize(count = n_distinct(id_str))

#Hypothesis - unusual for videos to share same length. Probably less likely when the length is not standardized, ie cut off at modulo 10 seconds.
plot(num_videos_by_length$duration_millis_num, num_videos_by_length$count)

nrow(num_videos_by_length) #9648 unique time lengths for videos.
nrow(num_videos_by_length[num_videos_by_length$count > 1,]) #2,628 time lengths that more than one video shares.
nrow(source_videos[! is.na(source_videos$title) & ! source_videos$title %in% c("Video", ""),]) #551 have titles..
source_videos$found_user_id_str <- paste("user_", source_videos$found_user_id_str, sep="") # number ids could be confused with lengths.
uservideonet <- graph_from_data_frame(source_videos[, c("found_user_id_str", "duration_millis_num")])
V(uservideonet)$type <- ifelse(V(uservideonet)$name %in% source_videos$found_user_id_str, 1, 0)
uservideonet <- simplify(uservideonet, remove.multiple = TRUE, edge.attr.comb = list(count="sum", "ignore"))
uservideoproj <- bipartite.projection(uservideonet, multiplicity = TRUE)
length(V(uservideoproj$proj2)) #8915
length(E(uservideoproj$proj2)) #262214
shared_times <- subgraph.edges(uservideoproj$proj2, E(uservideoproj$proj2)[E(uservideoproj$proj2)$weight > 2])
visNetwork::visIgraph(shared_times)

twitter_video_components <- components(shared_times)
V(shared_times)$membership <- twitter_video_components$membership
twitter_video_components$csize
#[1]  4 56  3  2  7  2  2 15  2 33  2  5 21  2  2  2  6  2  9  2  2  5  2  2  3
density <- c()
csize <- c()
min_multi <- c()
max_multi <- c()
med_multi <- c()
users <- c()
for(i in 1:25){
  tcomp <- induced_subgraph(shared_times, V(shared_times)[V(shared_times)$membership == i])
  density <- c(density, edge_density(tcomp))
  csize <- c(csize, length(V(tcomp)))
  users <- c(users, list(V(tcomp)$name))
  min_multi <- c(min_multi, min(E(tcomp)$weight))
  max_multi <- c(max_multi, max(E(tcomp)$weight))
  med_multi <- c(med_multi, median(E(tcomp)$weight))
}
twitter_video_components_info <- data.frame(EdgeDensity = density, ComponentSize = csize, MinMulti = min_multi, MaxMulti = max_multi, MedianMulti = med_multi)
View(twitter_video_components_info)

compusers <- source_videos[source_videos$found_user_id_str %in% users[[2]],]
