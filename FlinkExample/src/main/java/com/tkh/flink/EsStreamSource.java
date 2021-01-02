package com.tkh.flink;

import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;
import org.apache.http.HttpHost;
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

import java.io.IOException;
import java.util.Map;

public class EsStreamSource extends RichParallelSourceFunction<Row> {
    private RestHighLevelClient client;
    private String index;
    private String docType;
    private String hosts;
    private RowTypeInfo rowTypeInfo;
    private String scrollId;
    private CaseInsensitiveMap position;
    private boolean isRunning = true;
    private boolean hasNext = true;

    public EsStreamSource(String index, String docType, String hosts, RowTypeInfo rowTypeInfo) {
        this.index = index;
        this.docType = docType;
        this.hosts = hosts;
        this.rowTypeInfo = rowTypeInfo;

        position = new CaseInsensitiveMap();
        int i = 0;
        for (String name : rowTypeInfo.getFieldNames()) {
            position.put(name, i++);
        }
    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        while (isRunning && hasNext) {
            SearchHit[] searchHits = searchHits();
            if (searchHits == null || searchHits.length < 1) continue;
            for (SearchHit searchHit : searchHits) {
                Row row = new Row(rowTypeInfo.getArity());
                for (Map.Entry<String, Object> entry : searchHit.getSourceAsMap().entrySet()) {
                    Integer p = (Integer) position.get(entry.getKey());
                    if (p == null) throw new IOException("unknown field" + entry.getValue());
                    row.setField(p, entry.getValue());
                }
                ctx.collect(row);
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void open(Configuration configuration) {
        RestClientBuilder builder = RestClient.builder(HttpHost.create(hosts));
        builder.setMaxRetryTimeoutMillis(5*60*1000);
        client = new RestHighLevelClient(builder);
    }

    @Override
    public void close() throws Exception {
        if (client == null) return;
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        client.clearScroll(clearScrollRequest).isSucceeded();
        client.close();
        client = null;
    }

    private SearchHit[] searchHits() throws IOException {
        SearchResponse searchResponse;
        if (null == scrollId) {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            searchSourceBuilder.size(100);
            searchSourceBuilder.fetchSource(rowTypeInfo.getFieldNames(), null);

            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(index);
            searchRequest.types(docType);
            searchRequest.source(searchSourceBuilder);
            searchRequest.scroll(TimeValue.timeValueMinutes(1));

            searchResponse = client.search(searchRequest);
        } else {
            SearchScrollRequest searchScrollRequest = new SearchScrollRequest();
            searchScrollRequest.scrollId(scrollId);
            searchScrollRequest.scroll(TimeValue.timeValueMinutes(1));

            searchResponse = client.searchScroll(searchScrollRequest);
        }
        scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        if (searchHits == null || searchHits.length < 1) {
            hasNext = false;
        }
        return searchHits;
    }
}
