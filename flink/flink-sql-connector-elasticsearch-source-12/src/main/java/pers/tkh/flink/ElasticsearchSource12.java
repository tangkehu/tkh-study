package pers.tkh.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;


public class ElasticsearchSource12<RowData, SplitT extends SourceSplit, EnumChkT extends SourceSplit> implements Source<RowData, SplitT, EnumChkT>, ResultTypeQueryable<RowData> {

    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    public SourceReader<RowData, SplitT> createReader(SourceReaderContext readerContext) throws Exception {
    }

    public SplitEnumerator<SplitT, EnumChkT> createEnumerator(SplitEnumeratorContext<SplitT> enumContext) throws Exception {
    }

    public SplitEnumerator<SplitT, EnumChkT> restoreEnumerator(SplitEnumeratorContext<SplitT> enumContext, EnumChkT checkpoint) throws Exception {

    }

    public SimpleVersionedSerializer<SplitT> getSplitSerializer() {

    }

    public SimpleVersionedSerializer<EnumChkT> getEnumeratorCheckpointSerializer() {

    }

    public TypeInformation<RowData> getProducedType() {

    }
}
