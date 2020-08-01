package impro.functions;

import impro.data.KeyedDataPoint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class Durbin_old implements WindowFunction<KeyedDataPoint<Double>, KeyedDataPoint<Double>, Tuple, GlobalWindow> {
    protected static final double TWO_PI = 2 * Math.PI;
    private GlobalWindow window;
    private Collector<KeyedDataPoint<Double>> out;
    @Override
    public void apply(Tuple arg0, GlobalWindow window, Iterable<KeyedDataPoint<Double>> input, Collector<KeyedDataPoint<Double>> out) {
        this.window = window;
        this.out=out;
        double[] a = new double[numCoeff];//a = lpc coefficients
        double[] new_a = new double [numCoeff]; //new arrays guaranteed filled with 0

        String winKey = input.iterator().next().getKey();
        Iterator inputIterator = input.iterator();

        int durbinWindowLength=1;
        for (durbinWindowLength = 1; inputIterator.hasNext(); durbinWindowLength++) {
            KeyedDataPoint<Double> in = (KeyedDataPoint<Double>) inputIterator.next();
        }
        System.out.println("durbinWindowLength="+durbinWindowLength);

        //save window in array seq
        double[] seq = new double[windowSize]; //sometimes windows are smaller than that but it's ok
        for (int index = 0; inputIterator.hasNext(); index++) {
            KeyedDataPoint<Double> in = (KeyedDataPoint<Double>) inputIterator.next();
            seq[index] = in.getValue();
        }
        double[] r = autocorr(seq, numCoeff);
        double E = r[0];

        // CALCULATE a
        for (int i=0;i<numCoeff;i++){
            //(1) a new set of reflexion coefficients k(i) are calculated
            double ki = 0;
            for (int j=0;j<i-1;j++){
                ki = ki + ( a[j] * r[i-j] );
            }
            ki = (r[i] - ki) / E; //was r(i) + ki
            //(2) the prediction energy is updated
            // Enew = (1-ki^2) * Eold
            E = ( 1 - (ki*ki) ) * E;
            //(3) new filter coefficients are computed
            new_a[i] = ki; //was minus

            for (int j=0;j<i-1;j++){
                new_a[j]= a[j]- ki * a[i-j];
            }
            for (int j=0;j<i;j++){
                a[j] = new_a[j];
            }
        }

        //CALCULATE G^2 = residual energy resulting from the coefficients...
        double G2 = r[0];
        for (int k=0; k<numCoeff;k++){
            G2 = a[k]*r[k] + G2;
        }

        //CALCULATE residual
        double[] residual = new double[seq.length]; // e = residual in Octave
        for(int n=0;n<seq.length;n++){
            residual[n] = 0;
            for (int k=0; k<numCoeff;k++){
                if ( (n-k) > 0 )
                    residual[n] -= a[k]*seq[n-k]; //was +=
            }

            residual[n] = seq[n] + residual[n];
        }

        //OUTPUT TO STREAM
        long timeStamp = input.iterator().next().getTimeStampMs();
        output("a",timeStamp,a);
        output("residual",timeStamp,residual);
        output("G2",timeStamp,new double[]{G2}); //anonymous array

    }

    private int windowSize;
    private int numCoeff;


    public Durbin_old(int windowSize, int numCoeff) {
        this.windowSize = windowSize;
        this.numCoeff = numCoeff;

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
    public double[] autocorr(double[] seq, int cutOff) {

        double[] r = new double[seq.length];
        for (int i = 0; i < cutOff; i++) {
            r[i] = 0.0;
            for (int j = 1; j < seq.length - i; j++)
                r[i] = (seq[j] * seq[j + i]) + r[i];
        }
        return r;
    }

}
