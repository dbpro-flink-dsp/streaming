package impro.connectors.sources;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import impro.data.DataPoint;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.stream.DoubleStream;
import java.io.File;

import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;

public class AudioDataSourceFunction implements SourceFunction<DataPoint<Double>> {

    private volatile boolean isRunning = true;

    private String wavFileName;
    private int samplingRate=0;
    
    private volatile long currentTimeMs = -1;

    public AudioDataSourceFunction(String wavFileName) {
        this.wavFileName = wavFileName;        
    }

    @Override
    public void run(SourceFunction.SourceContext<DataPoint<Double>> sourceContext) throws Exception {

    	
    	Calendar calendar = new GregorianCalendar();
    	// Set an arbitrary starting time, or set one fixed one to always generate the same time stamps (for testing)
   	    // Current time:
        //long startTime = System.currentTimeMillis();
   	    // Fixed time:
        calendar.set(2019, 11, 04, 0, 0, 0);  // month is between 0-11: 2017,0,1,0,0,0 corresponds to: 2017 Jan 01 00:00:00.000
     	long startTime = calendar.getTimeInMillis();
    	
        // Get the Stream of AudioData Elements in the wav File:
        try(DoubleStream stream = getLocalAudioData(wavFileName)) {

            // We need to get an iterator, since the SourceFunction has to break out of its main loop on cancellation:
            Iterator<Double> iterator = stream.iterator();
            double periodMs = (1.0/samplingRate) * 10e6;  // period in miliseconds
            
            System.out.println("     ****samplingRate" + samplingRate + "  periodMs: " + periodMs);
            
            // Make sure to cancel, when the Source function is canceled by an external event:
            while (isRunning && iterator.hasNext()) {      	  

            	if(currentTimeMs == -1) {
                    currentTimeMs = startTime;
                    sourceContext.collect(new DataPoint<Double>(currentTimeMs,iterator.next()));
                } else {
                	currentTimeMs = (long)(currentTimeMs + periodMs);
                	sourceContext.collect(new DataPoint<Double>(currentTimeMs,iterator.next()));
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
