/*
This is a java scratchpad to test functions
 */

package impro.examples;

import java.util.Arrays;

public class test {

    public static void main(String[] args) {
        String key= "03a04Ad";
        String keySplit = key.substring(0,2) + "," + key.substring(2,5)+","+key.substring(5,6)+","+key.substring(6,7);
        System.out.println(keySplit);
    }
}
