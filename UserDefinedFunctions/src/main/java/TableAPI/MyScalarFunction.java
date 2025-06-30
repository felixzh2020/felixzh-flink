package TableAPI;

import org.apache.flink.table.functions.ScalarFunction;

public class MyScalarFunction extends ScalarFunction {
    public String eval(String line) {
        return line + ": MyScalarFunction ok";
    }
}
