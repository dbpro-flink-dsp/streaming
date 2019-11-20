# Run with:
#  source("plot_wave.R") 
#  then:
#    plot_wave()
#    
#
plot_wave <- function() {


library(ggplot2)
library(scales)
library(gridExtra)

s <- read.csv("../src/main/resources/tmp_wav.csv", header=FALSE)
colnames(s) <- c('ts','wav')


e <- read.csv("../src/main/resources/tmp_energy.csv", header=FALSE)
colnames(e) <- c('ts','energy')

p1 <- ggplot(s, aes(as.POSIXct(ts), wav)) + geom_line() + geom_point() + ggtitle("Audio processing") + xlab("Time stamp") + ylab("Wave")
p2 <- ggplot(e, aes(as.POSIXct(ts), energy)) + geom_line() + geom_point() + ggtitle("Audio processing") + xlab("Time stamp") + ylab("Energy")

grid.arrange(p1, p2, nrow=2)

}
