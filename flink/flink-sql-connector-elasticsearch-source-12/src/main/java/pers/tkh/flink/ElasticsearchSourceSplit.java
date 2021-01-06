package pers.tkh.flink;

import org.apache.flink.api.connector.source.SourceSplit;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * 划分出来的ES查询任务
 */
public class ElasticsearchSourceSplit implements SourceSplit {
    /**
     * ES的索引
     */
    private String indices;
    /**
     * ES的文档类型
     */
    private String types;
    /**
     * 每批获取的数据量
     */
    private int size = 1000;
    /**
     * 分片总数
     */
    private int slices = 5;
    /**
     * 分片ID
     */
    private String splitId = "0";
    /**
     * scroll查询的ID
     */
    private String scrollId;
    /**
     * ES的客户端连接
     */
    private RestHighLevelClient client;
    /**
     * 是否还有下一批数据
     */
    private boolean hasNext = true;

    public ElasticsearchSourceSplit(String indices, String types, int size, int slices, String splitId, RestHighLevelClient client) {
        this.indices = indices;
        this.types = types;
        this.size = size;
        this.slices = slices;
        this.splitId = splitId;
        this.client = client;
    }

    public String splitId() {
        return splitId;
    }

    public String getScrollId() {
        return scrollId;
    }

    public void setScrollId(String scrollId) {
        this.scrollId = scrollId;
    }

    public String getIndices() {
        return indices;
    }

    public void setIndices(String indices) {
        this.indices = indices;
    }

    public String getTypes() {
        return types;
    }

    public void setTypes(String types) {
        this.types = types;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getSlices() {
        return slices;
    }

    public void setSlices(int slices) {
        this.slices = slices;
    }

    public String getSplitId() {
        return splitId;
    }

    public void setSplitId(String splitId) {
        this.splitId = splitId;
    }

    public RestHighLevelClient getClient() {
        return client;
    }

    public void setClient(RestHighLevelClient client) {
        this.client = client;
    }

    public boolean isHasNext() {
        return hasNext;
    }

    public void setHasNext(boolean hasNext) {
        this.hasNext = hasNext;
    }

    public boolean isEmpty() {
        return indices == null || types == null || client == null;
    }
}
