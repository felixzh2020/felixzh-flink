import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.UserDefinedFunction;

public class NCFlinkTableAPIScalarFunction {
    public static void main(String... args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // UDF 来自独立jar
        String scalarFunctionClassName = "TableAPI.MyScalarFunction";
        Class<?> myScalarFunction = Class.forName(scalarFunctionClassName);

        //注册的Function名称自定义任意值
        tEnv.createTemporarySystemFunction("JsonFunction", (Class<? extends UserDefinedFunction>) myScalarFunction);

        // 或者  jar内部使用 的方法
        //tEnv.createTemporarySystemFunction("JsonFunction", TableAPI.MyScalarFunction.class);

        // execute a Flink SQL job and print the result locally
        tEnv.executeSql(
                        // define the aggregation
                        "SELECT JsonFunction(word), frequency\n"
                                // read from an artificial fixed-size table with rows and columns
                                + "FROM (\n"
                                + "  VALUES ('Hello', 1), ('Ciao', 1), ('felixzh', 2)\n"
                                + ")\n"
                                // name the table and its columns
                                + "AS WordTable(word, frequency)\n")
                .print();
    }
}
