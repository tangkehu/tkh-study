package pers.tkh.flink;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.data.RowData;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.slice.SliceBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;

public class ElasticsearchSourceReader implements SourceReader<RowData, ElasticsearchSourceSplit> {

    private ElasticsearchSourceSplit split;
    private boolean hasNext = true;
    private RestHighLevelClient client;
    private String index;
    private String docType;
    private int size;
    private int slices;
    private Queue<SearchHit> queue = new ArrayBlockingQueue<>(200000);

    public ElasticsearchSourceReader(RestHighLevelClient client, String index, String docType, int size, int slices) {
        this.client = client;
        this.index = index;
        this.docType = docType;
        this.size = size;
        this.slices = slices;
    }

    public void start() {
        // 开线程从ES抓取数据
        new Thread(() -> {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            searchSourceBuilder.slice(new SliceBuilder(Integer.parseInt(split.splitId()), slices));
            searchSourceBuilder.size(size);

            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(index);
            searchRequest.types(docType);
            searchRequest.source(searchSourceBuilder);
            searchRequest.scroll(TimeValue.timeValueMinutes(5L));

            SearchScrollRequest searchScrollRequest = new SearchScrollRequest();
            searchScrollRequest.scroll(TimeValue.timeValueMinutes(5L));
            searchScrollRequest.scrollId(split.getScrollId());

            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(split.getScrollId());

            client.clearScroll(clearScrollRequest);
        }).start();
    }

    public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
        return InputStatus.MORE_AVAILABLE;
    }

    public List<ElasticsearchSourceSplit> snapshotState(long checkpointId) {
        return new ArrayList<ElasticsearchSourceSplit>(){{add(split);}};
    }

    public CompletableFuture<Void> isAvailable() {
        return hasNext ? CompletableFuture.completedFuture(null) : new CompletableFuture<>();
    }

    public void addSplits(List<ElasticsearchSourceSplit> splits) {
        // 实际上只有一个split
        for (ElasticsearchSourceSplit split : splits) {
            this.split = split;
        }
    }

    public void notifyNoMoreSplits() {
        hasNext = false;
    }

    public void close() throws Exception {

    }
}
