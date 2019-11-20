package impro.examples;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import impro.data.KeyedDataPoint;
import impro.connectors.sinks.InfluxDBSink;
import impro.functions.AverageWindowFunction;
import impro.functions.MovingAverageFunction;

/**
 * run with parameter:
 *   --input ./src/main/resources/a40834n.csv
 */
public class StreamingMedicalJob {
    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        @SuppressWarnings({"rawtypes", "serial"})

        // test with file in: src/main/resources/a40834n.csv
        DataStream<KeyedDataPoint<Double>> mimicData;
        mimicData = env.readTextFile(params.get("input")).map(new ParseData());

        //mimicData.addSink(new InfluxDBSink<>("mimicData", "ABPMean"));

        mimicData.print();

        mimicData
                .assignTimestampsAndWatermarks(new ExtractTimestamp())
                .keyBy("key")
                // mimic data is sampled every minute
                //
                .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1))).trigger(CountTrigger.of(10))
                //
                .apply(new MovingAverageFunction())
                // save the average data for every key in a different series
                .name("mimicDataAvg")
                .print();
                //.addSink(new InfluxDBSink<>("mimicData", "ABPMeanAvg"));

        //mimicData.print();



        env.execute("slidingWindowExample");
    }



    private static class ParseData extends RichMapFunction<String, KeyedDataPoint<Double>> {
        private static final long serialVersionUID = 1L;


        @Override
        public KeyedDataPoint<Double> map(String record) {
            //String rawData = record.substring(1, record.length() - 1);
            String rawData = record;
            String[] data = rawData.split(",");


            // the data look like this... and we want to process ABPMean <- field 4
            // for this example I remove the first line...
            //            Timeanddate,HR,ABPSys,ABPDias,ABPMean,PAPSys,PAPDias,PAPMean,CVP,PULSE,RESP,SpO2,NBPSys,NBPDias,NBPMean,CO
            // '[10:36:00 31/05/2011]',0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,-,-,-,0.000
            // '[10:37:00 31/05/2011]',0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,-,-,-,0.000
            // '[10:38:00 31/05/2011]',0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,-,-,-,0.000
            // '[10:39:00 31/05/2011]',0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,-,-,-,0.000
            // '[10:40:00 31/05/2011]',0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,-,-,-,0.000

            String var1 = data[0].replace("'[", "");
            String var2 = var1.replace("]'", "");

            long millisSinceEpoch = LocalDateTime.parse(var2, DateTimeFormatter.ofPattern("HH:mm:ss dd/MM/uuuu"))
                    .atZone(ZoneId.systemDefault())
                    .toInstant()
                    .toEpochMilli();

            return new KeyedDataPoint<Double>("ABPMean", millisSinceEpoch, Double.valueOf(data[4]));


        }
    }

    private static class ExtractTimestamp extends AscendingTimestampExtractor<KeyedDataPoint<Double>> {
        private static final long serialVersionUID = 1L;

        @Override
        public long extractAscendingTimestamp(KeyedDataPoint<Double> element) {
            return element.getTimeStampMs();
        }
    }



}
