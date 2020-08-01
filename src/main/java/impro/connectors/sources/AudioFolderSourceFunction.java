package impro.connectors.sources;

import impro.data.KeyedDataPoint;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

public class AudioFolderSourceFunction implements SourceFunction<KeyedDataPoint<Double>> {

    private volatile boolean isRunning = true;

    private String folderPath;
    private int samplingRate=0;

    private volatile long currentTimeMs = -1;

    public AudioFolderSourceFunction(String folderPath) {
        this.folderPath = folderPath;
    }

    @Override
    public void run(SourceContext<KeyedDataPoint<Double>> sourceContext) throws Exception {
        List<File> files = Files.walk(Paths.get("./src/main/resources/LPCin/emotions")).filter(Files::isRegularFile).map(Path::toFile).collect(Collectors.toList());


        for (File f : files) {
            //String readFilePath = "./src/main/resources/LPCin/emotions/" + f.getName();
            String writeFilePath = "./src/main/resources/LPCout/emotions/" +f.getName();
            Calendar calendar = new GregorianCalendar();
            // Set an arbitrary starting time, or set one fixed one to always generate the same time stamps (for testing)
            // Current time:
            //long startTime = System.currentTimeMillis();
            // Fixed time:
            calendar.set(2019, 11, 04, 0, 0, 0);  // month is between 0-11: 2017,0,1,0,0,0 corresponds to: 2017 Jan 01 00:00:00.000
            long startTime = calendar.getTimeInMillis();
            //long startTime = 0;
            System.out.println("first epoch: " + startTime);
            // Get the Stream of AudioData Elements in the wav File:
            try (DoubleStream stream = getLocalAudioData(f.getAbsolutePath())) {

                // We need to get an iterator, since the SourceFunction has to break out of its main loop on cancellation:
                Iterator<Double> iterator = stream.iterator();
                //double periodMs = (1.0/samplingRate) * 10e3;  // period in miliseconds
                long periodMs = 625;

                System.out.println("     ****samplingRate" + samplingRate + "  periodMs: " + periodMs);

                // Make sure to cancel, when the Source function is canceled by an external event:
                while (isRunning && iterator.hasNext()) {

                    if (currentTimeMs == -1) {
                        currentTimeMs = startTime;
                        sourceContext.collect(new KeyedDataPoint<Double>(f.getName(), currentTimeMs, iterator.next()));
                    } else {
                        currentTimeMs = (long) (currentTimeMs + periodMs);
                        sourceContext.collect(new KeyedDataPoint<Double>(f.getName(), currentTimeMs, iterator.next()));
                    }

                }
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    /**
     * Get the data in Doubles from the wav file and return it in a DoubleStream
     * @param wavFile
     * @return
     */
    private DoubleStream getLocalAudioData(String wavFile) {

        double[] audio = null;
        DoubleStream audioDoubleStream = null;

        try {
            AudioInputStream ais = AudioSystem.getAudioInputStream(new File(wavFile));
            this.samplingRate = (int) ais.getFormat().getSampleRate();
            this.samplingRate = (int) ais.getFormat().getSampleRate();

            AudioDoubleDataSource signal = new AudioDoubleDataSource(ais);

            audio = signal.getAllData();
            audioDoubleStream = Arrays.stream(audio);
        }
        catch (final Exception e) {
            e.printStackTrace();
        }
        return audioDoubleStream;
    }


}
