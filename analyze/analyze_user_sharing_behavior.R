library("igraph")
library("dplyr")

# STEP 1: Load data and create graph
df <- read.csv("enriched_youtube_links_june_through_july1.csv")
user_id_names <- unique(df[,c("user_id", "screen_name")])


freq_shares <- df %>% 
  group_by(user_id, vid) %>%
  summarize(num_shares = n())

freq_shares <- merge(freq_shares, user_id_names, by="user_id")
View(freq_shares)
boxplot(freq_shares$num_shares)
boxplot(freq_shares[freq_shares$num_shares < 800,"num_shares"])

freq_shares_by_user <- freq_shares %>%
  group_by(user_id) %>%
  summarize(avg_shares_per_video = mean(num_shares),
            med_shares_per_video = median(num_shares),
            std_shares_per_video = sd(num_shares),
            max_shares_for_video = max(num_shares),
            num_videos_shared = n_distinct(vid))

freq_shares_by_user <- merge(freq_shares_by_user, user_id_names, by="user_id")

plot(freq_shares_by_user$num_videos_shared, freq_shares_by_user$avg_shares_per_video)
plot(freq_shares_by_user$num_videos_shared, freq_shares_by_user$max_shares_for_video)

freq_shares_by_video <- freq_shares %>%
  group_by(vid) %>%
  summarize(avg_shares_per_user = mean(num_shares),
            med_shares_per_user = mean(num_shares),
            std_shares_per_user = sd(num_shares),
            max_shares_per_user = max(num_shares),
            num_users_shared = n_distinct(user_id))

plot(freq_shares_by_video$max_shares_per_user, freq_shares_by_video$num_users_shared)
plot(freq_shares_by_video$avg_shares_per_user, freq_shares_by_video$num_users_shared)


