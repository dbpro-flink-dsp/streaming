package impro.examples;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

import impro.data.DataPoint;
import impro.data.KeyedDataPoint;
import impro.util.AssignKeyFunction;
import impro.functions.DiscreteKalmanFunction;
import impro.functions.NormalDistributionFunction;
import impro.connectors.sinks.InfluxDBSink;
import impro.util.TimestampSource;

public class StreamingKalmanJob {
	
	public static void main(String[] args) throws Exception {
		
	    double meanVal =  -0.37727;
	    double standardDeviationVal = 0.1;
	    final int SLOWDOWN_FACTOR = 1;  //
	    final int PERIOD_MS = 100;    // 1000=1sec 100=100msec


	    // set up the execution environment
	    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	    
	    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
	    // Simulate sensor data	   
	    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1000, 1000)); //
	    //env.setParallelism(1);
	    //env.disableOperatorChaining();


	    // Initial data - just timestamped messages
	    DataStreamSource<DataPoint<Long>> timestampSource = env
	    		.addSource(new TimestampSource(PERIOD_MS, SLOWDOWN_FACTOR), "test data");
	    
	    // Initialise normal distribution and generate a sample, assign a key and generate a KeyedDataPoint stream
	    SingleOutputStreamOperator<KeyedDataPoint<Double>> normalDistStream = timestampSource
	    		.map(new NormalDistributionFunction(meanVal,standardDeviationVal)) 
	    		.name("random")
	    		.map(new AssignKeyFunction("random"))
	    		.name("assignKey(random)");


	    // Write this random stream out to InfluxDB	    
	    normalDistStream.addSink(new InfluxDBSink<>("testDB", "kalmanFilter"));
	    //-- randomStream.print();	   
	    
	    // apply the Kalman filter to 
	    DataStream<KeyedDataPoint<Double>> filteredStream = normalDistStream
				.keyBy("key")
				.flatMap(new DiscreteKalmanFunction(0.1));
	  
	    
	    filteredStream.addSink(new InfluxDBSink<>("testDB", "kalmanFilter"));	    
	   //-- filteredStream.print();
	    
	    // execute program
	    env.execute("Kalman job");
	  }


	
	private static class ExtractTimestamp extends AscendingTimestampExtractor<KeyedDataPoint<Double>> {
		private static final long serialVersionUID = 1L;

		@Override
		public long extractAscendingTimestamp(KeyedDataPoint<Double> element) {
			return element.getTimeStampMs();
		}
	}
	  

}
