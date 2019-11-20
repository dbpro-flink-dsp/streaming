# Run with:
#  source("plot_noaa.R") 
#  then:
#    plot_noaa('coyote_creek')
#    plot_noaa('santa_monica')
#
plot_noaa <- function(label) {

library(ggplot2)
library(scales)
library(gridExtra)

s <- read.csv("noaa_water_level.csv", header=FALSE)
colnames(s) <- c('name','location','water_level','ts')

sa <- read.csv("/tmp/noaa_water_level_averaged.csv", header=FALSE)
colnames(sa) <- c('location','water_level','ts')


if(label!=""){
    s <- s[s$location==label,]
    sa <- sa[sa$location==label,]
}

p1 <- ggplot(s, aes(as.POSIXct(ts, origin = "1970-01-01"), water_level, colour=location)) +
    geom_line() + geom_point() +
    theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
    scale_x_datetime(labels = date_format("%Y-%m-%d"), date_breaks="1 days") +
    ggtitle("NOAA Water level") + xlab("Time stamp") + ylab("Water level")


# ts divided by 1000 because the ts afte avg is saved in millisec.
p2 <- ggplot(sa, aes(as.POSIXct((ts/1000), origin = "1970-01-01"), water_level, colour=location)) +
    geom_line() + geom_point() +
    theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
    scale_x_datetime(labels = date_format("%Y-%m-%d"), date_breaks="1 days") +
    ggtitle("NOAA Water level, averaged window") + xlab("Time stamp") + ylab("Water level")

grid.arrange(p1, p2, nrow=2)

}
