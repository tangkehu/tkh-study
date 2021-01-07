package pers.tkh.flink;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.table.data.RowData;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class ElasticsearchSplitEnumerator implements SplitEnumerator {

    private SplitEnumeratorContext context;
    private String hosts;
    private String indices;
    private String types;
    private int size;
    private int slices;
    private String[] fields;
    private DeserializationSchema<RowData> deserializer;
    private RestClient restClient;
    private Queue<ElasticsearchSourceSplit> queue = new LinkedList<>();

    public ElasticsearchSplitEnumerator(SplitEnumeratorContext context, String hosts, String indices, String types, int size, int slices,
                                        String[] fields, DeserializationSchema<RowData> deserializer) {
        this.context = context;
        this.hosts = hosts;
        this.indices = indices;
        this.types = types;
        this.size = size;
        this.slices = slices;
        this.fields = fields;
        this.deserializer = deserializer;
    }

    @Override
    public void start() {
//        try {
//            RestClientBuilder builder = RestClient.builder(HttpHost.create(hosts));
//            builder.setMaxRetryTimeoutMillis(5 * 60 * 1000);
//            builder.setRequestConfigCallback((RequestConfig.Builder conf) -> conf.setSocketTimeout(5 * 60 * 1000));
//            restClient = builder.build();
//            RestHighLevelClient client = new RestHighLevelClient(restClient);
//            for (int i = 0; i < slices; i++) {
//                queue.offer(new ElasticsearchSourceSplit(indices, types, size, slices, String.valueOf(i), client, deserializer, fields));
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        SourceSplit split = queue.poll();
        if (split != null) {
            context.assignSplit(split, subtaskId);
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List splits, int subtaskId) {
        queue.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {

    }

    @Override
    public Object snapshotState() throws Exception {
        return queue;
    }

    @Override
    public void close() throws IOException {
        if (restClient != null) {
            restClient.close();
        }
        restClient = null;
    }
}
