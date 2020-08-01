package impro.functions;

import impro.data.KeyedDataPoint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Iterator;

public class LPCMultiple implements WindowFunction<KeyedDataPoint<Double>, KeyedDataPoint<Double>, Tuple, TimeWindow> {
    protected static final double TWO_PI = 2 * Math.PI;

    @Override
    public void apply(Tuple arg0, TimeWindow window, Iterable<KeyedDataPoint<Double>> input, Collector<KeyedDataPoint<Double>> out) {
        this.window = window;
        this.out=out;
        String winKey = input.iterator().next().getKey();
        double preEmphasisCoeff=0.97;

        //TODO maybe sort?
        //save window in array seq
        double[] seq = windowToArray(input);

        //PREEMPHASIS
        double[] preEmphed = new double[seq.length];
        preEmphed[0]=seq[0];
        for (int index = 0;index<seq.length-1;index++) {
            seq[index] = seq[index]*preEmphasisCoeff;
            preEmphed[index+1] = seq[index+1] - ( preEmphasisCoeff * seq[index] );
        }
        //HAMMING

        double[] hammingFactors = Hamming.of(windowSize);
        for (int index=0; index<preEmphed.length;index++)
            preEmphed[index] = preEmphed[index]* hammingFactors[index];
        //System.out.println(Arrays.toString(preEmphed));

        //DURBIN

        double[] r = autocorr(preEmphed, numCoeff);
        double E = r[0];
        r = Arrays.copyOfRange(r,1,r.length); //remove first element
        //System.out.println(Arrays.toString(r));
        double[] a = durbin (r, E, numCoeff);
        //System.out.println(Arrays.toString(a));

        //CALCULATE G^2 = residual energy resulting from the coefficients...
        double G2 = r[0];
        for (int k=0; k<numCoeff;k++){
            G2 = a[k]*r[k] + G2;
        }

        //CALCULATE residual
        double[] residual = new double[preEmphed.length]; // e = residual in Octave
        for(int n=0;n<preEmphed.length;n++){
            residual[n] = 0;
            for (int k=0; k<numCoeff;k++){
                if ( (n-k) > 0 )
                    residual[n] -= a[k]*preEmphed[n-k]; //was +=
            }

            residual[n] = preEmphed[n] + residual[n];
        }
        //System.out.println(Arrays.toString(preEmphed));
        //System.out.println(a[0]);
        //System.out.println(Arrays.toString(a));
        //System.out.println(Arrays.toString(residual));
        //System.out.println(Arrays.toString(new double[]{G2}));


        //OUTPUT TO STREAM
        long timeStamp = input.iterator().next().getTimeStampMs();
        //output("hamming", timeStamp, preEmphed);
        output(winKey,timeStamp,a);
        //output("residual",timeStamp,residual);
        //output("G2",timeStamp,new double[]{G2}); //anonymous array


    }
    private TimeWindow window;
    private Collector<KeyedDataPoint<Double>> out;
    private int windowSize;
    private int numCoeff; //number of coefficients (p in Octave)
    public LPCMultiple(int windowSize, int numCoeff){
        this.windowSize = windowSize;
        this.numCoeff = numCoeff;

    }
    private double[] windowToArray(Iterable<KeyedDataPoint<Double>> input){
        int length = 0;
        Iterator countIter = input.iterator();
        for ( ; countIter.hasNext() ; ++length ) countIter.next();

        Iterator inputIterator = input.iterator();
        double[] seq = new double[length]; //sometimes windows are smaller than that but it's ok
        for (int index = 0; inputIterator.hasNext(); index++) {
            KeyedDataPoint<Double> in = (KeyedDataPoint<Double>) inputIterator.next();
            seq[index] = in.getValue();
        }
        return seq;
    }

    private void output(String key,long timeStamp,double[] a){
        for (int i=0;i<a.length;i++){
            KeyedDataPoint<Double> newElem = new KeyedDataPoint<Double>(key,timeStamp, a[i]);
            out.collect(newElem);
        }
    }
    /**
     * @param seq    x in Octave
     * @param cutOff p in Octave
     * @return
     */
    private static double[] autocorr(double[] seq, int cutOff) {

        double[] r = new double[cutOff+1]; //new arrays guaranteed filled with 0
        for (int i = 0; i < cutOff+1; i++) {
            for (int j = 0; j < seq.length - i; j++)
                r[i] = (seq[j] * seq[j + i]) + r[i];
        }
        return r;
    }

    /**
     *
     * @param r correlation values WITHOUT the first value. The first one is in E
     * @param E The first correlation value.
     * @param p how many a's we need. Size of returned array
     * @return the a coefficients
     */
    public static double[] durbin(double[] r, double E, int p) {
        double[] a = new double[p+1];
        double[] new_a = new double[p+1];

        for (int i = 1; i <=p; i++) {
            //(1) a new set of reflexion coefficients k (i) are calculated
            double ki = 0;

            for (int j = 1; j <=(i - 1); j++)
                ki = ki + (a[j] * r[i - j -1]);
            ki = (r[i - 1] + ki) / E;


            //(2) the prediction energy is updated
            //Enew = (1 - ki ^ 2) * Eold
            E = (1 - (ki * ki)) * E;

            //(3) new filter coefficients are computed
            new_a[i] = -ki;

            for (int j = 1; j <=(i - 1); j++)
                new_a[j] = a[j] - ki * a[i - j];

            for (int j = 1; j <=i; j++)
                a[j] = new_a[j];
        }
        return Arrays.copyOfRange(a,1,a.length);
    }

}
