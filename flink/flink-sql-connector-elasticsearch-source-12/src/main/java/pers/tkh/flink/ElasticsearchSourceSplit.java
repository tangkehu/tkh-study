package pers.tkh.flink;

import org.apache.flink.api.connector.source.SourceSplit;

public class ElasticsearchSourceSplit implements SourceSplit {
    /**
     * 分片ID
     */
    private String splitId;
    /**
     * scroll查询的ID
     */
    private String scrollId;

    public String splitId() {
        return splitId;
    }

    public void setSplitId(String splitId) {
        this.splitId = splitId;
    }

    public String getScrollId() {
        return scrollId;
    }

    public void setScrollId(String scrollId) {
        this.scrollId = scrollId;
    }
}
