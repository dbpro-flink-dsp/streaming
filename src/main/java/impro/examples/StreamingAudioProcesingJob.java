package impro.examples;

import impro.connectors.sources.AudioDataSourceFunction;
import impro.data.KeyedDataPoint;
import impro.util.AssignKeyFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * This example reads the audio data from a wave file and apply some processing to the samples of data
 * when reading the audio data, the sampling frequency is obtained from the .wav file
 * the timestamps are assigned according to the period=1/sampling_frequency
 *      period = (1.0/samplingRate) * 10e6;  // period in miliseconds
 * so every point of the data has a timestamp every period miliseconds
 * since we need a date to create the ts, it is selected to start: 04.11.2019 <-- adjust if necessary
 *
 * Run with:
 *    --input ./src/java/resources/short_curious.wav
 * the ouput is saved in the same directory in two .csv files:
 *      tmp_wav.csv
 *      tmp_energy.csv
 * these two files can be plotted with the R script:
 *
 */
public class StreamingAudioProcesingJob {
    public static void main(String[] args) throws Exception {

        //final ParameterTool params = ParameterTool.fromArgs(args);
        //String wavFile = params.get("input");
        String wavFile = "./src/main/resources/short_curious.wav";
        System.out.println("     wav file: " + wavFile);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        @SuppressWarnings({"rawtypes", "serial"})
        // Read the audio data from a wave file
                DataStream<KeyedDataPoint<Double>> audioDataStream = env.addSource(new AudioDataSourceFunction(wavFile))
                .map(new AssignKeyFunction("wav"));

        // print and write in a csv file the input data, just for visualization, other option could be
        // to sink into InfluxDB and plot with grafana
        audioDataStream.print();
        audioDataStream.writeAsText("./src/main/resources/tmp_wav.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // Apply the Energy function per window
        DataStream<KeyedDataPoint<Double>> audioDataStreamEnergy = audioDataStream
                // the timestamps are from the data
                .assignTimestampsAndWatermarks(new ExtractTimestamp())
                .keyBy("key")
                // slide a window
                .window(SlidingEventTimeWindows.of(Time.seconds(2), Time.seconds(1)))
                // calculate energy per window
                .apply(new EnergyCalculationFunction());


        //audioDataStreamEnergy.print();
        // print and write in a csv file the input data, just for vizualisation
        audioDataStreamEnergy.writeAsText("./src/main/resources/tmp_energy.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("StreamingAudioProcesingJob");
    }


    /**
     * The energy of a window can be calculated as:
     *    window_energy = sum(x[i]^2)
     */
    private static class EnergyCalculationFunction implements WindowFunction<KeyedDataPoint<Double>, KeyedDataPoint<Double>, Tuple, TimeWindow> {

        @Override
        public void apply(Tuple arg0, TimeWindow window, Iterable<KeyedDataPoint<Double>> input, Collector<KeyedDataPoint<Double>> out) {
            int count = 0;
            double winEnergy = 0;
            String winKey = input.iterator().next().getKey(); // get the key of this window

            // get the sum^2 of the elements in the window
            for (KeyedDataPoint<Double> in: input) {
                winEnergy = winEnergy + (in.getValue()*in.getValue());
                count++;
            }

            System.out.println("EnergyCalculationFunction: win energy=" +  winEnergy + "  count=" + count + "  time=" + window.getStart());

            KeyedDataPoint<Double> windowEnergy = new KeyedDataPoint<Double>(winKey,window.getEnd(),winEnergy);

            out.collect(windowEnergy);

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