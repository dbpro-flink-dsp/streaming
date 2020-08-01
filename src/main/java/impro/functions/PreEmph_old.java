package impro.functions;

import impro.data.KeyedDataPoint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class PreEmph_old implements WindowFunction<KeyedDataPoint<Double>, KeyedDataPoint<Double>, Tuple, TimeWindow> {
    protected static final double TWO_PI = 2 * Math.PI;

    @Override
    public void apply(Tuple arg0, TimeWindow window, Iterable<KeyedDataPoint<Double>> input, Collector<KeyedDataPoint<Double>> out) {
    	/*
    	We need a way to recreate the windows later. We will do this by assigning a window-specific
    	key to each element in the current window. window.getEnd() provides one such unique number.
    	How else can we do this?
    	 */
        String winKey = String.valueOf(window.getEnd());//input.iterator().next().getKey();
        double preEmphasisCoeff=0.97;
        //count window length
        int length = 0;
        Iterator countIter = input.iterator();
        for ( ; countIter.hasNext() ; ++length ) countIter.next();
        //System.out.println("counted "+length);

        // get the sum of the elements in the window
        KeyedDataPoint<Double> newElem;
        Iterator inputIterator = input.iterator();

        //PREEMPHASIS AND HAMMING
        for (int index = 0;inputIterator.hasNext();index++) {
            KeyedDataPoint<Double> in = (KeyedDataPoint<Double>) inputIterator.next();
            //apply pre-emphasis
            double newValue = in.getValue()*preEmphasisCoeff;
            //apply hamming window
            double factor = 0.54f - 0.46f * (float) Math.cos(TWO_PI * index / (windowSize - 1));
            newValue = newValue*factor;
            newElem = new KeyedDataPoint<>(winKey, window.getEnd(), newValue);
            //System.out.println(newElem);
            out.collect(newElem);
        }

    }
    private int windowSize;
    public PreEmph_old(int windowSize){
        this.windowSize = windowSize;

    }

}
