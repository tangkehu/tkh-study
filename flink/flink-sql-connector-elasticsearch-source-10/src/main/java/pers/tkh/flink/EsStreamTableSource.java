package pers.tkh.flink;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

public class EsStreamTableSource implements StreamTableSource<Row> {
    private final TableSchema schema;
    private final String  hosts;
    private final String index;
    private final String docType;
    private final DeserializationSchema<Row> deserializationSchema;

    public EsStreamTableSource(TableSchema schema, String index, String docType, String hosts, DeserializationSchema<Row> deserializationSchema){
        this.schema = TableSchemaUtils.checkNoGeneratedColumns(schema);
        this.hosts = Preconditions.checkNotNull(hosts);
        this.index = Preconditions.checkNotNull(index);
        this.docType = Preconditions.checkNotNull(docType);
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv){
        return execEnv.addSource(
                new EsStreamSource(index, docType, hosts,
                        new RowTypeInfo(this.schema.getFieldTypes(), this.schema.getFieldNames()))).name(this.explainSource());
    }

    public TableSchema getTableSchema(){ return this.schema; }

    public TypeInformation<Row> getReturnType() {
//        return this.deserializationSchema.getProducedType();
        return new RowTypeInfo(this.schema.getFieldTypes(), this.schema.getFieldNames());
    }

//    public DataType getProducedDataType() {
//        return new Row(new RowTypeInfo(this.schema.getFieldTypes(), this.schema.getFieldNames()).getArity());
//    }

    public String explainSource(){
        return TableConnectorUtils.generateRuntimeName(this.getClass(), this.schema.getFieldNames());
    }
}
