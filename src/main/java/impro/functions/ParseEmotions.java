package impro.functions;

import impro.data.KeyedDataPoint;
import org.apache.flink.api.common.functions.MapFunction;

public class ParseEmotions<T> implements MapFunction<KeyedDataPoint<T>, KeyedDataPoint<T>> {
    @Override
    public KeyedDataPoint<T> map(KeyedDataPoint<T> point) throws Exception {
        String key = point.getKey();
        String keySplit = key.substring(0,2) + "," + key.substring(2,5)+","+key.substring(5,6)+","+key.substring(6,7);
        point.setKey(keySplit);
        //System.out.println("point: "+point);
        return point;
    }
}
