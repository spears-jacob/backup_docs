library(ggplot2)
library(openxlsx)

sheets <- getSheetNames('bitrate-histogram-apr.xlsx')
source('~/Documents/theme-yizhe.R')

for (sheet in c(1, 2, 4, 5)) {#1:length(sheets)) {

  data <- read.xlsx('bitrate-histogram-apr.xlsx', sheet = sheets[sheet])

  if (sheets[sheet] == 'Application') {
    p <- ggplot(data) +
      geom_bar(aes(x = bin_center, y = bin_height, fill = application_type), stat = 'identity', position = "dodge", width = 0.08) +
      facet_wrap(~application_type, nrow = 1) +
      theme_yizhe() +
      scale_y_continuous(labels = scales::comma) +
      labs(title = paste("Histogram by", sheets[sheet]),
      x = "Average Bitrate (mbps)",
      y = "Number of unique streams",
      caption = paste(strwrap("Source: prod.venona_events, Denver date 2017-04-14 to 2017-04-17", width = 100), collapse = "\n"))
    ggsave(paste0(sheets[sheet],'.png'), plot = p, scale = 1, width = 16, height = 8, dpi = 300)
  } else {
    apps <- unique(data$application_type)

    for (app in 1:length(apps)) {
      grouping <- colnames(data)[2]
      p <- ggplot(dplyr::filter(data, application_type == apps[app])) +
        geom_bar(aes(x = bin_center, y = bin_height, fill = application_type), stat = 'identity', position = "dodge", width = 0.08) +
        facet_wrap(~get(grouping), nrow = 1) +
        theme_yizhe() +
        scale_y_continuous(labels = scales::comma) +
        labs(title = paste("Histogram for", apps[app], "by", sheets[sheet]),
        x = "Average Bitrate (mbps)",
        y = "Number of unique streams",
        caption = paste(strwrap("Source: prod.venona_events, Denver date 2017-04-14 to 2017-04-17", width = 100), collapse = "\n"))
      ggsave(paste0(sheets[sheet], "-", apps[app], '.png'), plot = p, scale = 1, width = 16, height = 8, dpi = 300)
    }
  }
}
