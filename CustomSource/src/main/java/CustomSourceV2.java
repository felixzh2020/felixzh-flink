import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;

public class CustomSourceV2 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //GeneratorFunction<Long, String> generatorFunction = index -> "Number: " + index;
        GeneratorFunction<Long, String> generatorFunction = index -> {
            // 这里不用index。根据实际需求，改成自己场景需要的测试数据。
            return buildData(1024);
        };

        DataGeneratorSource<String> source = new DataGeneratorSource<>(
                generatorFunction,
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(100),
                Types.STRING);


        DataStreamSource<String> streamSource =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Custom SourceV2");
        streamSource.print();

        env.execute("CustomSourceV2");
    }

    private static String buildData(long targetBytes) {
        StringBuilder sb = new StringBuilder();
        while (true) {
            SecureRandom secureRandom = new SecureRandom();
            // 生成32-126之间的ASCII码（可打印字符）
            int asciiCode = 32 + secureRandom.nextInt(95);
            char randomChar = (char) asciiCode;
            sb.append(randomChar);
            byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
            if (bytes.length >= targetBytes) {
                return sb.toString();
            }
        }
    }
}
