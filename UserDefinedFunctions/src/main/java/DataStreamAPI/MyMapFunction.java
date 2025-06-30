package DataStreamAPI;

import org.apache.flink.api.common.functions.MapFunction;

public class MyMapFunction implements MapFunction<String, String> {

    @Override
    public String map(String s) throws Exception {
        return s + " MyMapFunction " + System.currentTimeMillis();
    }
}
