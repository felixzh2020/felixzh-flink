import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MyEsSink<T> extends RichSinkFunction<List<T>> implements CheckpointedFunction {

    private final Logger LOG = LoggerFactory.getLogger(MyEsSink.class);

    private final String esIps;

    private final int esPort;

    private final String indexName;

    public static MyEsSink<String> create(String esIps, int esPort, String indexName) {
        return new MyEsSink<>(esIps, esPort, indexName);
    }

    public MyEsSink(String esIps, int esPort, String indexName) {
        this.esIps = esIps;
        this.esPort = esPort;
        this.indexName = indexName;
    }

    private RestHighLevelClient client;

    @Override
    public void open(Configuration parameters) throws Exception {
        Header[] headers = {new BasicHeader("Accept", "application/json"), new BasicHeader("Content-type", "application/json")};
        client = new RestHighLevelClient(RestClient.builder(Util.getHostArray(esIps, esPort)).setDefaultHeaders(headers));
    }

    @Override
    public void invoke(List<T> values, Context context) throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        values.forEach(value -> {
            bulkRequest.add(new IndexRequest(indexName, "_doc").source(value, XContentType.JSON));
        });
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        if (bulkResponse.hasFailures()) {
            LOG.error(bulkResponse.buildFailureMessage());
        }
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }


}
