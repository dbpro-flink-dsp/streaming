package impro.examples;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.commons.math3.geometry.Vector;

import java.util.*;

/**
 * Calculate linear regression by a gradient descent iteration
 * Find the coefficients, theta, for the hypothesis:
 *     h(x) = theta[0] + theta[1] x  where y = h(x) and the assumption is that the vectors y and x are linearly related.
 *
 * See examples in:
 *    R implementation in ./R/lr_gradient_descent.R
 *    [1] https://www.r-bloggers.com/linear-regression-by-gradient-descent/
 *    [2] https://towardsdatascience.com/linear-regression-simplified-ordinary-least-square-vs-gradient-descent-48145de2cf76
 *  Iterations Apache flink:
 *    https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/batch/iterations.html
 */
public class BatchIterationJob {

    public static void main(String[] args) throws Exception {

        // using the data in [2]
        // x <- c(2,  3, 5,13, 8,16,11,1,9)
        // y <- c(15,28,42,64,50,90,58,8,54)
        List<Tuple2<Integer, Integer>> dataList = new ArrayList<>();
        dataList.add(new Tuple2<>(2,15));
        dataList.add(new Tuple2<>(3,28));
        dataList.add(new Tuple2<>(5,42));
        dataList.add(new Tuple2<>(13,64));
        dataList.add(new Tuple2<>(8,50));
        dataList.add(new Tuple2<>(16,90));
        dataList.add(new Tuple2<>(11,58));
        dataList.add(new Tuple2<>(1,8));
        dataList.add(new Tuple2<>(9,54));

        // Initialization
        List<Tuple2<Double, Double>> thetaList = new ArrayList<>();
        thetaList.add(new Tuple2<>(0.0,0.0));

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Create initial IterativeDataSet and set number of iterations
        DataSet<Tuple2<Integer, Integer>> data = env.fromCollection(dataList);
        DataSet<Tuple2<Double, Double>> thetaInitialValues = env.fromCollection(thetaList);
        data.print();

        IterativeDataSet<Tuple2<Double, Double>> thetaIterative = thetaInitialValues.iterate(1000);

        // calculate in every iteration new theta values
        DataSet<Tuple2<Double, Double>> newTheta = data
                .reduceGroup(new StepFunction()).withBroadcastSet(thetaIterative, "theta");

        // Iteratively transform the IterativeDataSet
        DataSet<Tuple2<Double, Double>> finalTheta = thetaIterative.closeWith(newTheta);

        finalTheta.print();


    }


    public static class StepFunction extends RichGroupReduceFunction<Tuple2<Integer, Integer>, Tuple2<Double, Double>> {
        int iterationNumber=0;
        double alpha = 0.01;
        double m = 9;  // data set size, hard coded here for this example
        private Collection<Tuple2<Double, Double>> theta;
        private double th0 = 0.0;
        private double th1 = 0.0;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.theta = getRuntimeContext().getBroadcastVariable("theta");
        }

        @Override
        public void reduce(Iterable<Tuple2<Integer, Integer>> value, Collector<Tuple2<Double, Double>> out) throws Exception {
            iterationNumber++;
            // get previous theta values
            for(Tuple2<Double, Double> val : theta ) {
                th0 = val.f0;
                th1 = val.f1;
            }
            System.out.println("     iterationNumber: " + iterationNumber + "  partial result:  th0=" + th0 + " th1=" + th1);

            // y_hat <- (X %*% theta)
            // yaux <- y_hat - y
            double yaux[] = new double[9];
            int x[] = new int[9];
            int i=0;
            //  Tuple2<   x   ,  y     >
            for(Tuple2<Integer, Integer> val : value ) {
                yaux[i] = ( th0 + (val.f0*th1) ) - val.f1;
                x[i] = val.f0;
                i++;
            }
            double th0aux = 0.0;
            double th1aux = 0.0;
            // tmp2 <- (t(X) %*% delta ) <-- dot product
            for(i=0; i<x.length; i++){
                th0aux += 1 * yaux[i];
                th1aux += x[i] * yaux[i];
            }

            //theta <- theta - alpha * ( 1/m * (t(X) %*% (y_hat-y) )  )
            //theta <- theta - alpha * ( 1/m * (t(X) %*% delta )  )
            th0 = th0 - alpha * ( (1.0/m) * th0aux);
            th1 = th1 - alpha * ( (1.0/m) * th1aux);


            System.out.println("     iterationNumber: " + iterationNumber + "  partial result:  th0=" + th0 + " th1=" + th1);
            out.collect(new Tuple2<Double, Double>(th0, th1));
        }
    }


}
