import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;

public class NCFlinkTableAPIAggregateFunction {
    public static void main(String... args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //方式2 ：独立jar
        String aggregateFunctionClassName = "TableAPI.MyAggregateFunctionsForWeightedAvg";
        Class<?> myAggregateFunction = Class.forName(aggregateFunctionClassName);
        tEnv.createTemporarySystemFunction("WeightedAvg", (Class<? extends UserDefinedFunction>) myAggregateFunction);

        List<Row> data = Arrays.asList(
                Row.of("110", 1),
                Row.of("120", 2),
                Row.of("120", 3),
                Row.of("120", 100)
        );

        Table table1 = tEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("myField", DataTypes.BIGINT()),
                        DataTypes.FIELD("weight", DataTypes.INT())
                )
                , data);

        tEnv.createTemporaryView("table1", table1);

        tEnv.executeSql("SELECT * FROM table1").print();

        tEnv.executeSql(
                        "SELECT myField, WeightedAvg(myField, weight)\n"
                                + " FROM table1 GROUP BY myField\n")
                .print();
    }
}
