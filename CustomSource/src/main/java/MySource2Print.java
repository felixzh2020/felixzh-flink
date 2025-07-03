import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MySource2Print {
    public static void main(String... args) throws Exception {
        if (args.length != 1) {
            System.out.println("Usage: flink run -t yarn-per-job -d /path/to/jar /path/to/MySource2Print.properties");
            System.exit(0);
        }

        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        long checkpointMS = parameterTool.getLong("env.checkpointing.timeMS", 0);
        if (checkpointMS != 0) {
            env.enableCheckpointing(checkpointMS, CheckpointingMode.EXACTLY_ONCE);
        }

        CustomSource<String> customSource = new CustomSource<>(
                parameterTool.getLong("custom.source.data.size"),
                parameterTool.getLong("custom.source.sleep.ms"));

        env.addSource(customSource).returns(Types.STRING).setParallelism(parameterTool.getInt("custom.source.parallelism")).print();

        env.execute();
    }
}
