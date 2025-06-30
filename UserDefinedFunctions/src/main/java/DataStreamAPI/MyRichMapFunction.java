package DataStreamAPI;

import org.apache.flink.api.common.functions.MapFunction;

import java.util.Date;

public class MyRichMapFunction implements MapFunction<String, String> {

    @Override
    public String map(String s) throws Exception {
        return s + " MyRichMapFunction " + new Date();
    }
}
