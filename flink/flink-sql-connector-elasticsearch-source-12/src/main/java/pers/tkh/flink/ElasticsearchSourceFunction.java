package pers.tkh.flink;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

public class ElasticsearchSourceFunction extends RichParallelSourceFunction<RowData> implements ResultTypeQueryable<RowData> {
    private String hosts;
    private String indices;
    private String types;
    private int size;
    private int slices;
    private String[] fields;
    private DeserializationSchema<RowData> deserializer;
    private RestClient restClient;
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSourceFunction.class);
    /**
     * 从ES查询出来的数据的缓存队列
     */
    private Queue<SearchHit> queue = new ArrayBlockingQueue<>(200000);
    /**
     * 标记还有没有数据
     */
    private boolean hasNext = false;

    ElasticsearchSourceFunction(String hosts, String indices, String types, int size, int slices, String[] fields,
                                       DeserializationSchema<RowData> deserializer) {
        this.hosts = hosts;
        this.indices = indices;
        this.types = types;
        this.size = size;
        this.slices = slices;
        this.fields = fields;
        this.deserializer = deserializer;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return deserializer.getProducedType();
    }

    @Override
    public void open(Configuration parameters) {
        int idTask = getRuntimeContext().getIndexOfThisSubtask();
        int numTask = getRuntimeContext().getNumberOfParallelSubtasks();
        String nameTask = getRuntimeContext().getTaskNameWithSubtasks();
        LOG.info(">>>> {}总任务数{}，当前子任务{}", nameTask, numTask, idTask);

        // 超过配置的分片就不进行查询了
        if (idTask + 1 > slices) {
            LOG.info(">>>> {}超过分片数，当前子任务的ES请求取消。", nameTask);
            return;
        }
        // 启动时标记为有数据
        hasNext = true;
        // 建立ES连接
        RestClientBuilder builder = RestClient.builder(HttpHost.create(hosts));
        builder.setMaxRetryTimeoutMillis(5 * 60 * 1000);
        builder.setRequestConfigCallback((RequestConfig.Builder conf) -> conf.setSocketTimeout(5 * 60 * 1000));
        restClient = builder.build();
        // 开线程从ES抓取数据
        Thread searchThread = new Thread(() -> {
            RestHighLevelClient client = new RestHighLevelClient(restClient);
            SearchHit[] searchHits = {};
            String scrollId = null;
            // 进行第一次请求获得scrollID
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            searchSourceBuilder.slice(new SliceBuilder(idTask, slices));
            searchSourceBuilder.size(size);
            searchSourceBuilder.fetchSource(fields, null);

            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(indices);
            searchRequest.types(types);
            searchRequest.source(searchSourceBuilder);
            searchRequest.scroll(TimeValue.timeValueMinutes(5L));

            try {
                SearchResponse searchResponse = client.search(searchRequest);
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();
            } catch (IOException e) {
                LOG.error(e.toString(), e);
            }
            // 第一次请求之后的scrollId还是为空或没有数据就标记为没有数据，退出
            if (scrollId == null || searchHits == null || searchHits.length == 0) {
                hasNext = false;
                return;
            }
            // 开始读取数据写入缓存队列
            SearchScrollRequest searchScrollRequest = new SearchScrollRequest();
            searchScrollRequest.scroll(TimeValue.timeValueMinutes(5L));
            do {
                for (SearchHit searchHit : searchHits) {
                    boolean flag = queue.offer(searchHit);
                    while (!flag) {
                        flag = queue.offer(searchHit);
                    }
                }
                searchScrollRequest.scrollId(scrollId);
                try {
                    SearchResponse searchResponse = client.searchScroll(searchScrollRequest);
                    scrollId = searchResponse.getScrollId();
                    searchHits = searchResponse.getHits().getHits();
                } catch (IOException e) {
                    LOG.error(e.toString(), e);
                    break;
                }
            } while (searchHits != null && searchHits.length > 0);
            // 设置没有数据了
            hasNext = false;
            // 清理ES中的scrollId
            try {
                ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
                clearScrollRequest.addScrollId(scrollId);
                client.clearScroll(clearScrollRequest);
            } catch (IOException e) {
                LOG.error(e.toString(), e);
            }
        });
        searchThread.setDaemon(true);
        searchThread.start();
        // 开启一个缓存队列监控线程
        Thread monitorThread = new Thread(() -> {
            try {
                Thread.sleep(60000);
                LOG.info(">>>> {}当前缓存队列数据{}", nameTask, queue.size());
            } catch (InterruptedException e) {
                LOG.error(e.toString(), e);
            }
        });
        monitorThread.setDaemon(true);
        monitorThread.start();
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        while (hasNext || !queue.isEmpty()) {
            SearchHit searchHit = queue.poll();
            if (searchHit != null ) {
                ctx.collect(deserializer.deserialize(searchHit.getSourceAsString().getBytes()));
            }
        }
    }

    @Override
    public void cancel() {
        hasNext = false;
    }

    @Override
    public void close() throws Exception {
        if (restClient != null) {
            restClient.close();
        }
        restClient = null;
    }
}
