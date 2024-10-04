# sampling in R
days <- as.Date(as.Date('2017-02-01', origin = 'UTC'):as.Date('2017-03-29', origin = 'UTC'), origin = '1970-01-01')
hours <- 0:23
min <- 0:59
all <- data.frame(
  partition_date = as.Date(rep(days, each = length(hours) * length(min)), origin = '1970-01-01'),
  hour = sprintf('%0.2d', rep(hours, times = length(days) * length(min))),
  hour_num = rep(hours, times = length(days) * length(min)),
  min = rep(min, each = length(hours), times = length(days)))
all$min_time <- as.numeric(all$partition_date) * 86400000 + all$hour_num * 3600000 + all$min * 60000
all$max_time <- all$min_time + 60000
options(scipen=999)
write.table(all[, c('partition_date', 'hour', 'min_time', 'max_time')], 'all.csv', sep = ',', row.names = F, quote = F, col.names = F)
