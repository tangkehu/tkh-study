package pers.tkh.flink;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.data.RowData;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.slice.SliceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;

public class ElasticsearchSourceReader implements SourceReader<RowData, ElasticsearchSourceSplit> {

    /**
     * ES查询任务（根据配置的slices划分成的一个个并行任务）
     */
    private ElasticsearchSourceSplit split;
    /**
     * 从ES查询出来的数据的缓存队列
     */
    private Queue<SearchHit> queue = new ArrayBlockingQueue<>(200000);
    /**
     * 标记还有没有数据
     */
    private boolean hasNext = false;
    /**
     * 上下文
     */
    private SourceReaderContext context;

    public ElasticsearchSourceReader(SourceReaderContext context) {
        this.context = context;
        this.context.sendSplitRequest();
    }

    /**
     * 从ES拿数据写入队列
     */
    public void start() {
        if (split == null || split.isEmpty()) {
            return;
        }
        // 启动时将标记为有数据
        hasNext = split.isHasNext();
        // 开线程从ES抓取数据
        new Thread(() -> {
            SearchHit[] searchHits = {};
            String scrollId = split.getScrollId();
            // 判断是否有scrollID，没有就进行第一次请求获得scrollID
            if (scrollId == null) {
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                searchSourceBuilder.query(QueryBuilders.matchAllQuery());
                searchSourceBuilder.slice(new SliceBuilder(Integer.parseInt(split.getSplitId()), split.getSlices()));
                searchSourceBuilder.size(split.getSize());
                //searchSourceBuilder.fetchSource(rowTypeInfo.getFieldNames(), null);

                SearchRequest searchRequest = new SearchRequest();
                searchRequest.indices(split.getIndices());
                searchRequest.types(split.getTypes());
                searchRequest.source(searchSourceBuilder);
                searchRequest.scroll(TimeValue.timeValueMinutes(5L));

                try {
                    SearchResponse searchResponse = split.getClient().search(searchRequest);
                    scrollId = searchResponse.getScrollId();
                    searchHits = searchResponse.getHits().getHits();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            // 第一次请求之后的scrollId还是为空或没有数据就标记为没有数据，退出
            if (scrollId == null || searchHits == null) {
                hasNext = false;
                split.setHasNext(false);
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
                split.setScrollId(scrollId);
                searchScrollRequest.scrollId(scrollId);
                try {
                    SearchResponse searchResponse = split.getClient().searchScroll(searchScrollRequest);
                    scrollId = searchResponse.getScrollId();
                    searchHits = searchResponse.getHits().getHits();
                } catch (IOException e) {
                    e.printStackTrace();
                    break;
                }
            } while (searchHits != null && searchHits.length > 0);
            // 设置没有数据了
            hasNext = false;
            split.setHasNext(false);
            // 清理ES中的scrollId
            try {
                ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
                clearScrollRequest.addScrollId(scrollId);
                split.getClient().clearScroll(clearScrollRequest);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }

    public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
        SearchHit searchHit = queue.poll();
        if (searchHit != null ) {
//            output.collect(searchHit);
            return InputStatus.MORE_AVAILABLE;
        } else if (!hasNext) {
            return InputStatus.END_OF_INPUT;
        } else {
            return pollNext(output);
        }
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
