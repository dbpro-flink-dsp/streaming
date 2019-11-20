# Run with:
#  source("lr_gradient_descent.R") 
#  then:
#    lr_gradient_descent(0.01, 1000)
#
# examples from:
#  [1] https://www.r-bloggers.com/linear-regression-by-gradient-descent/
#  [2] https://towardsdatascience.com/linear-regression-simplified-ordinary-least-square-vs-gradient-descent-48145de2cf76
lr_gradient_descent <- function(alpha, num_iters){

   library(DAAG) # for pause()
    
   # load the data from [2]
   x <- c(2,3,5,13,8,16,11,1,9)
   y <- c(15,28,42, 64,50,90,58,8,54)

    
   # fit a linear model
   res <- lm( y ~ x )
   print(res)
 
   # plot the data and the model
   plot(x,y, col=rgb(0.2,0.4,0.6,0.4), main='Linear regression by gradient descent')
   abline(res, col='blue')

   # learning rate and iteration limit
   #alpha <- 0.01
   #num_iters <- 1000

   m <- length(y)
   previous_cost <- 0 
   # gradient descent
   for (i in 1:num_iters) {
        
     y_hat <- (X %*% theta)
     theta <- theta - alpha * ( 1/m * (t(X) %*% (y_hat-y) )  )

     plot(x,y)
     abline(res, col='blue')
     abline(theta[1],theta[2], col='red')
     c <- cost(X,y,theta)
     cost_change <- abs(c - previous_cost)
     previous_cost <- c
       
     cat('theta: [', theta, ']  cost:', cost(X,y,theta), '  cost_change: ', cost_change, '\n')
     #pause()
       
   }
   print('Final theta:')
   print(theta)



}


# squared error cost function
cost <- function(X, y, theta) {
  sum( (X %*% theta - y)^2 ) / (2*length(y))
}
