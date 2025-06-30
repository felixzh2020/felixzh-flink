import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;

public class NCFlinkTableAPITableFunction {
    public static void main(String... args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 方式1 ：内部jar
        //tEnv.createTemporarySystemFunction("SplitFunction", TableAPI.MyTableFunction.class);

        //方式2 ：独立jar
        String tableFunctionClassName = "TableAPI.MyTableFunction";
        Class<?> myTableFunction = Class.forName(tableFunctionClassName);
        tEnv.createTemporarySystemFunction("SplitFunction", (Class<? extends UserDefinedFunction>) myTableFunction);

        List<Row> data = Arrays.asList(
                Row.of("hello felixzh"),
                Row.of("FelixZh ok")
        );

        Table table1 = tEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("myField", DataTypes.STRING())
                )
                , data);

        tEnv.createTemporaryView("table1", table1);

        tEnv.executeSql("SELECT * FROM table1").print();

        tEnv.executeSql(
                        "SELECT myField, word, length\n"
                                + " FROM table1\n"
                                + " ,LATERAL TABLE(SplitFunction(myField))\n")
                .print();
    }
}
