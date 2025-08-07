import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchSink;
import org.apache.flink.connector.elasticsearch.sink.FlushBackoffType;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author felixzh
 * <p>
 * 在Java中，类加载器通常遵循双亲委派模型，即子类加载器在加载类前先委托父类加载器。
 * 但是，在Flink中，当用户依赖Jar与Flink依赖Jar有冲突时候，可以通过classloader.resolve-order调整类加载器顺序。
 * child-first表示优先从用户依赖Jar加载，也就是打破了Java双亲委派模型的累加器方式。
 * 如果在 Flink 中将 classloader.resolve-order 设置为 parent-firt，
 * 则 Flink AppClassLoader 将优先于 FlinkUserCodeClassLoader 进行类加载。
 * 也就是说，当用户自定义代码中存在与系统级别库相同的类时，Flink AppClassLoader 将优先加载系统级别库中的类，而不是用户自定义代码中的类。
 * 这种加载机制与传统的 Java 应用程序的类加载机制是相同的，即优先从父 ClassLoader 中加载类，如果父 ClassLoader 中不存在该类，则委托给子 ClassLoader 进行加载。
 * <p>
 * 而yarn.classpath.include-user-jar则决定YARN模式下用户Jar如何添加到task manager进程的classpath！
 * 为了避免全局classpath污染，推荐优先使用child-first + DISABLED 组合。
 * <p>
 * When deploying Flink with PerJob/Application Mode on Yarn, the JAR file specified in startup command and all JAR files
 * in Flink’s usrlib folder will be recognized as user-jars. By default Flink will include the user-jars into the system classpath.
 * This behavior can be controlled with the yarn.classpath.include-user-jar parameter.
 * When setting this to DISABLED Flink will include the jar in the user classpath instead.
 * The user-jars position in the classpath can be controlled by setting the parameter to one of the following:
 * ORDER: (default) Adds the jar to the system classpath based on the lexicographic order.
 * FIRST: Adds the jar to the beginning of the system classpath.
 * LAST: Adds the jar to the end of the system classpath.
 * <p>
 * when setting the yarn.classpath.include-user-jar to DISABLED, Flink will include the user jars in the user classpath
 * and load them dynamically by FlinkUserCodeClassLoader.
 */
public class DataGen2ES {
    public static void main(String... args) throws Exception {
        if (args.length != 1) {
            System.out.println("Usage: flink run -t yarn-per-job /path/to/DataGen2ES-1.0.jar /path/to/DataGen2ES.properties");
            System.out.println("Usage: flink run-application -t yarn-application  -Dyarn.ship-files=FlinkJob_DataGen2ES.properties -Dclassloader.resolve-order=parent-first /path/to/DataGen2ES-1.0.jar /path/to/DataGen2ES.properties");
            System.out.println("Usage: flink run-application -t yarn-application  -Dyarn.ship-files=FlinkJob_DataGen2ES.properties -Dyarn.classpath.include-user-jar=DISABLED /path/to/DataGen2ES-1.0.jar /path/to/DataGen2ES.properties");
            System.exit(0);
        }

        ParameterTool param = ParameterTool.fromPropertiesFile(args[0]);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        GeneratorFunction<Long, String> generatorFunction = index ->
                RandomPrintableDataGenerator.generate(param.getInt("source.record.size.byte", 512), true);

        DataGeneratorSource<String> source = new DataGeneratorSource<>(
                generatorFunction,
                Long.MAX_VALUE,
                //RateLimiterStrategy.noOp(),
                RateLimiterStrategy.perSecond(param.getInt("source.record.per.sec", 1)),
                Types.STRING);

        HttpHost[] esHosts = Arrays.stream(param.get("sink.es.list").split(","))
                .map(ipPort -> {
                    String[] ipPortList = ipPort.split(":");
                    return new HttpHost(ipPortList[0], Integer.parseInt(ipPortList[1]), "http");
                }).toArray(HttpHost[]::new);

        int bulkFlushMaxActions = param.getInt("sink.bulk.flush.max.actions", 1);
        long bulkFlushIntervalMs = param.getLong("sink.bulk.flush.interval.ms", 1000);
        int bulkFlushMaxMb = param.getInt("sink.bulk.flush.max.mb", 1);

        ElasticsearchSink<String> sink = new Elasticsearch7SinkBuilder<String>()
                .setBulkFlushMaxActions(bulkFlushMaxActions)
                .setBulkFlushInterval(bulkFlushIntervalMs)
                .setBulkFlushMaxSizeMb(bulkFlushMaxMb)
                .setHosts(esHosts)
                .setEmitter((element, context, indexer) -> {
                            indexer.add(genIndexRequest(param.getRequired("sink.es.index"), element));
                        }
                )
                .setBulkFlushBackoffStrategy(FlushBackoffType.CONSTANT, 3, 500)
                .setConnectionTimeout(60000)
                .setConnectionRequestTimeout(60000)
                .setSocketTimeout(60000)
                .build();

        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "FelixZhDataGen")
                .setParallelism(param.getInt("source.parallelism", 2));

        stream.sinkTo(sink).setParallelism(param.getInt("sink.parallelism", 1));

        env.execute(DataGen2ES.class.getName() + " FelixZh");
    }

    private static IndexRequest genIndexRequest(String index, String element) {
        Map<String, Object> json = new HashMap<>();
        json.put("data", element);
        //非必要不建议指定id，容易造成数据倾斜
        return Requests.indexRequest().index(index)/*.id(id)*/.source(json, XContentType.JSON);
    }
}
