package impro.examples;

import impro.connectors.sources.AudioFolderSourceFunction;
import impro.data.KeyedDataPoint;
import impro.functions.LPCMultiple;
import impro.functions.ParseEmotions;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;

import java.util.concurrent.TimeUnit;

/**
 * This example reads the audio data from a wave file and apply some processing to the samples of data
 * when reading the audio data, the sampling frequency is obtained from the .wav file
 * the timestamps are assigned according to the period=1/sampling_frequency
 * period = (1.0/samplingRate) * 10e6;  // period in miliseconds
 * so every point of the data has a timestamp every period miliseconds
 * since we need a date to create the ts, it is selected to start: 04.11.2019 <-- adjust if necessary
 * <p>
 * Run with:
 * --input ./src/java/resources/LPCin/short_curious.wav
 * the ouput is saved in the same directory in two .csv files:
 * tmp_wav.csv
 * tmp_energy.csv
 * these two files can be plotted with the R script:
 */
public class StreamingLPCMultipleJob {
    public static void main(String[] args) throws Exception {
        //Parameters
        int counter = 1;
        int p = 20; // number of lpc coefficients
        long w_frame = 250000;  // 0.025 seconds length of window frame
        //=> 400 samples per window at 16000 sample rate
        long w_period = 50000;  // 0.005 seconds window period
        //=> 80 samples per window at 16000 sample rate

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        // Read the audio data from a wave file and assign fictional time
        DataStream<KeyedDataPoint<Double>> audioDataStream = env
                .addSource(new AudioFolderSourceFunction("./src/main/resources/LPCin/emotions"))
                //.map(new AssignKeyFunction("pressure"))
                .setParallelism(1);

        // Apply the Energy function per window
        System.out.println("before LPC processing");
        //DataStream<KeyedDataPoint<Double>> LPCStream =
        audioDataStream
                // the timestamps are from the data
                .assignTimestampsAndWatermarks(new ExtractTimestamp())
                .map(new ParseEmotions<Double>())
                .keyBy("key")
                //.window of((25000, milliseconds), (10000, milliseconds)
                //.window(SlidingEventTimeWindows.of(Time.seconds(2), Time.seconds(1)))
                .window(SlidingEventTimeWindows.of(Time.of(w_frame, TimeUnit.MILLISECONDS), //size
                        Time.of(w_period, TimeUnit.MILLISECONDS)))
                .trigger(CountTrigger.of(400))//sliding
                //or do it with countwindow
                .apply(new LPCMultiple(400, 20)) //apply destroys windows
                .rebalance()
                .writeAsText("./src/main/resources/LPCout/LPCMultipleout.csv", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        //WRITE OUTPUT TO FILES

/*
        LPCStream.filter(new FilterByKey("hamming"))
                .rebalance()
                .writeAsText(path +emotion+"/"+audioname+"/hamming.csv", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);
        LPCStream.filter(new FilterByKey("a"))
                .rebalance()
                .writeAsText(path +emotion+"/"+audioname+"/a.csv", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);
        LPCStream.filter(new FilterByKey("residual"))
                .rebalance()
                .writeAsText(path +emotion+"/"+audioname+"/residual.csv", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);
        LPCStream.filter(new FilterByKey("G2"))
                .rebalance()
                .writeAsText(path +emotion+"/"+audioname+"/G2.csv", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);
*/

        //PREPARE DATA FOR INFLUX?
        //OUTPUT TO INFLUX/GRAFANA
//        LPCStream
//                .addSink(new InfluxDBSink<>("sineWave", "sensors"))
//                .name("sensors-sink");

        env.execute("StreamingAudioProcesingJob");
    }


    private static class FilterByKey implements FilterFunction<KeyedDataPoint<Double>> {
        String key;

        FilterByKey(String key) {
            this.key = key;
        }

        @Override
        public boolean filter(KeyedDataPoint<Double> doubleKeyedDataPoint) throws Exception {
            return doubleKeyedDataPoint.getKey().equals(key);
        }
    }


    private static class ExtractTimestamp extends AscendingTimestampExtractor<KeyedDataPoint<Double>> {
        private static final long serialVersionUID = 1L;

        @Override
        public long extractAscendingTimestamp(KeyedDataPoint<Double> element) {
            //return (long)(element.getTimeStampMs() * 0.001);  //???
            return element.getTimeStampMs();
        }
    }


}