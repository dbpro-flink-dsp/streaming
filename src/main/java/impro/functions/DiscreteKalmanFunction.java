package impro.functions;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import impro.data.KeyedDataPoint;

/**
 * References:
 *   http://www.cs.unc.edu/~welch/media/pdf/kalman_intro.pdf
 *   http://www.cs.unc.edu/~tracker/media/pdf/SIGGRAPH2001_CoursePack_08.pdf
 A - state transition matrix
 B - control input matrix
 H - measurement matrix
 Q - process noise covariance matrix
 R - measurement noise covariance matrix
 P - error covariance matrix
 */

public class DiscreteKalmanFunction extends RichFlatMapFunction<KeyedDataPoint<Double>, KeyedDataPoint<Double>> {

    /**
     * The ValueState handle. The first field is the previous point value, the second field is P: error covariance Matrix
     */
    private transient ValueState<Tuple2<Double, Double>> filterParams;

    private Double R = 0.1; // Default value

    public DiscreteKalmanFunction(double r){
        this.R = r;
    }

    @Override
    public void flatMap(KeyedDataPoint<Double> inputPoint, Collector<KeyedDataPoint<Double>> outFilteredPointCollector) throws Exception {

        // access the state value
        // I will put in the state: xx[k-1] and P[k-1]
        // previousPoint.f0 : xx[k-1]
        // previousPoint.f1 : P[k-1]
        Tuple2<Double, Double> previousPointParams = filterParams.value();



        //# time update equations
        // xx_[k] <- xx[k-1]
        // P_[k]  <- P[k-1]        
        Double xx_ = previousPointParams.f0;
        Double P_  = previousPointParams.f1;

        //# measurement update equations
        // K[k] <- P_[k] / ( P_[k] + R)
        // xx[k] <- xx_[k] + K[k] * ( z[k] - xx_[k])
        // P[k] <- (1 - K[k]) * P_[k]        
        Double K = P_ / (P_ + R);
        Double xx = xx_ + K * ( inputPoint.getValue() - xx_ );
        Double P  = (1 - K ) * P_;

        // update state to keep current values for next calculation
        previousPointParams.f0 = xx;
        previousPointParams.f1 = P;
        filterParams.update(previousPointParams);


        // return filtered point     	 
        outFilteredPointCollector.collect(new KeyedDataPoint<Double>("filtered", inputPoint.getTimeStampMs(), xx));

    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Tuple2<Double, Double>> descriptor = new ValueStateDescriptor<>(
                "average", // the state name
                TypeInformation.of(new TypeHint<Tuple2<Double, Double>>() {}), // type information
                Tuple2.of(0.0, 1.0)); // default value of the state, if nothing was set  //TODO: check this initialisation!
        filterParams = getRuntimeContext().getState(descriptor);
    }

}

