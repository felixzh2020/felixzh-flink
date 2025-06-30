import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class NCFlinkDataStreamAPIUDF {
    public static void main(String... args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> socketData = env.socketTextStream("felixzh", 4444);

        // UDF 来自独立jar
        //方法1：通过反射newInstance实例化对象。优点：解耦、可配置类名称
        String mapFunctionClassName = "DataStreamAPI.MyMapFunction";
        String mapRichFunctionClassName = "DataStreamAPI.MyRichMapFunction";

        Class<?> myMapFunction = Class.forName(mapFunctionClassName);
        Class<?> myRichMapFunction = Class.forName(mapRichFunctionClassName);

        Object myMapFunctionInstance = myMapFunction.newInstance();
        Object myRichMapFunctionInstance = myRichMapFunction.newInstance();

        socketData
                .map((MapFunction<String, Object>) myMapFunctionInstance)
                .map((MapFunction<Object, Object>) myRichMapFunctionInstance)
                .print();

        //方法2：通过new实例化对象。缺点：耦合固定类名称
        //socketData.map(new DataStreamAPI.MyMapFunction()).print();
        //socketData.map(new DataStreamAPI.MyRichMapFunction()).print();

        env.execute();
    }
}
