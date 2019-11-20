/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package impro;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author Marcela Charfuelan
 *
 * In this example a time series data is studied in streaming mode.
 * the tasks are:
 *   - load the data as a stream series, using event time
 *   - apply a tumbling and a sliding window with various time settings
 *
 * Run with:
 *   --input ./src/main/resources/noaa_water_level.csv
 * The data used in this example is provided by influx here:
 *    https://s3.amazonaws.com/noaa.water-database/NOAA_data.txt
 *    more details on Influx here:
 *    https://docs.influxdata.com/influxdb/v1.2/query_language/data_download/
 */
public class StreamingJob {
	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		@SuppressWarnings({"rawtypes", "serial"})

		//         Tuple3<key, time stamp, measurement>
		DataStream<Tuple3<String, Double, Long>> waterData = env.readTextFile(params.get("input"))
				                                                .map(new ParseData());
		waterData.print();


		waterData.assignTimestampsAndWatermarks(new ExtractTimestamp())
				.keyBy(0)
				//.timeWindow(Time.hours(24))
				//       Tumbling window
				.window(TumblingEventTimeWindows.of(Time.hours(1)))
				//       Sliding window:
				//.window(SlidingEventTimeWindows.of(Time.days(7), Time.days(1)))
				//       Average the window
				.apply(new AverageFunction())

				// save the average data for every key in a different series
				//.name("waterLevelAvg");
				.writeAsCsv("/tmp/noaa_water_level_averaged.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
				//.print();

		env.execute("WaterLevelExample");
	}

	private static class AverageFunction implements WindowFunction<Tuple3<String, Double, Long>, Tuple3<String, Double, Long>, Tuple, TimeWindow> {

		@Override
		public void apply(Tuple arg0, TimeWindow window, Iterable<Tuple3<String, Double, Long>> input, Collector<Tuple3<String, Double, Long>> out) {
			int count = 0;
			double winsum = 0;
			String winKey = input.iterator().next().f0; // get the key of this window  "movingAvg";

			// get the sum of the elements in the window
			for (Tuple3<String, Double, Long> in: input) {
				winsum = winsum + in.f1;
				count++;
			}

			Double avg = winsum/(1.0 * count);
			System.out.println("MovingAverageFunction: winsum=" +  winsum + "  count=" + count + "  avg=" + avg + "  time=" + window.getStart());

			Tuple3<String, Double, Long> windowAvg = new Tuple3<>(winKey,avg,window.getEnd());

			out.collect(windowAvg);

		}
	}




	private static class ParseData extends RichMapFunction<String, Tuple3<String, Double, Long>> {
		private static final long serialVersionUID = 1L;


		@Override
		public Tuple3<String, Double, Long> map(String record) {
			//String rawData = record.substring(1, record.length() - 1);
			String rawData = record;
			String[] data = rawData.split(",");

			// the data look like this...
			// measure,location,water_level,timestamp
			// h2o_feet,coyote_creek,8.120,1439856000
			// h2o_feet,coyote_creek,8.005,1439856360
			// h2o_feet,coyote_creek,7.887,1439856720
			// h2o_feet,coyote_creek,7.762,1439857080
			//                                                                        time stamp in ms
			return new Tuple3<String, Double, Long>(data[1], Double.valueOf(data[2]), Long.valueOf(data[3])*1000);

		}
	}

	private static class ExtractTimestamp extends AscendingTimestampExtractor<Tuple3<String, Double, Long>> {
		private static final long serialVersionUID = 1L;

		@Override
		public long extractAscendingTimestamp(Tuple3<String, Double, Long> element) {
			return element.f2;  // returns time stamp in milliseconds
		}
	}

}
