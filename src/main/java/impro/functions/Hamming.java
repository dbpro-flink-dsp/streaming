package impro.functions;

import java.util.Arrays;
/*
hamming window function adapted from:
https://github.com/amaurycrickx/recognito/blob/master/recognito/src/main/java/com/bitsinharmony/recognito/algorithms/windowing/HammingWindowFunction.java
 */

public class Hamming {


    private static final double TWO_PI = 6.28318530718;

    public static double[] of(int size) {
        double[] factor = new double[size];

        for (int i=0;i<size/2;i++){
            double value = 0.54d - 0.46d * Math.cos(TWO_PI * i / (size - 1));
            factor[i] = factor[size-i-1] = value;
        }
        if (size%2==1){
            int mid = Math.floorDiv(size,2)+1;
            factor[mid] = 0.54d - 0.46d * Math.cos(TWO_PI * mid / (size - 1));
        }
        return factor;
//        for(int i=0;i<size;i++){
//            System.out.println(factor[i]);
//        }
    }
}