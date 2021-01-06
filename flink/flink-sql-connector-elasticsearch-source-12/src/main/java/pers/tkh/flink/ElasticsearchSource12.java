package pers.tkh.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;


public class ElasticsearchSource12 implements Source, ResultTypeQueryable {
    private String hosts;
    private String indices;
    private String types;
    private int size;
    private int slices;

    public ElasticsearchSource12(String hosts, String indices, String types, int size, int slices) {
        this.hosts = hosts;
        this.indices = indices;
        this.types = types;
        this.size = size;
        this.slices = slices;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader createReader(SourceReaderContext readerContext) throws Exception {
        return new ElasticsearchSourceReader(readerContext);
    }

    @Override
    public SplitEnumerator createEnumerator(SplitEnumeratorContext enumContext) throws Exception {
        return new ElasticsearchSplitEnumerator(enumContext, hosts, indices, types, size, slices);
    }

    @Override
    public SplitEnumerator restoreEnumerator(SplitEnumeratorContext enumContext, Object checkpoint) throws Exception {
        return null;
    }

    @Override
    public SimpleVersionedSerializer getSplitSerializer() {
        return null;
    }

    @Override
    public SimpleVersionedSerializer getEnumeratorCheckpointSerializer() {
        return null;
    }

    @Override
    public TypeInformation getProducedType() {
        return null;
    }
}
