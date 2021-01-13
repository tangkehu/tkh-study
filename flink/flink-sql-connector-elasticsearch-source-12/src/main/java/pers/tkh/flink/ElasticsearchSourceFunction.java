package pers.tkh.flink;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;

public class ElasticsearchSourceFunction extends RichSourceFunction<RowData> implements ResultTypeQueryable<RowData> {
    private String hosts;
    private String indices;
    private String types;
    private int size;
    private int slices;
    private String[] fields;
    private DeserializationSchema<RowData> deserializer;

    public ElasticsearchSourceFunction(String hosts, String indices, String types, int size, int slices, String[] fields,
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
    public void open(Configuration parameters) throws Exception {
        System.out.println(">>>>"+parameters);
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {

    }

    @Override
    public void cancel() {

    }
}
