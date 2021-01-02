package com.tkh.flink;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.elasticsearch6.shaded.org.apache.http.HttpHost;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.flink.table.sources.StreamTableSource;


public class ElasticsearchTableSource implements StreamTableSource<Row> {
    private final TableSchema schema;
    private final List hosts;
    private final String index;
    private final String docType;
    private final DeserializationSchema<Row> deserializationSchema;
    private final List<HttpHost> httpHosts;

    protected ElasticsearchTableSource(TableSchema schema, List<ElasticsearchUpsertTableSinkBase.Host> hosts, String index, String docType, DeserializationSchema<Row> deserializationSchema) {
        this.schema = TableSchemaUtils.checkNoGeneratedColumns(schema);
        this.hosts = (List)Preconditions.checkNotNull(hosts);
        this.index = (String)Preconditions.checkNotNull(index);
        this.docType = (String)Preconditions.checkNotNull(docType);
        this.deserializationSchema = (DeserializationSchema)Preconditions.checkNotNull(deserializationSchema, "Deserialization schema must not be null.");
        this.httpHosts = (List)hosts.stream().map((host) -> {
            return new HttpHost(host.hostname, host.port, host.protocol);
        }).collect(Collectors.toList());
    }

    public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {
        RowTypeInfo rowTypeInfo = new RowTypeInfo(this.getFieldTypes(), this.getFieldNames());
        ElasticsearchInput esSource =  ElasticsearchInput.builder(
                this.httpHosts,
                this.index)
                .setRowTypeInfo(rowTypeInfo)
                .build();
        DataStreamSource streamSource = env.createInput(esSource);
        return streamSource;
//        return env.addSource(streamSource).name(this.explainSource());
    }

    public TypeInformation<Row> getReturnType() {
        return this.deserializationSchema.getProducedType();
    }

    public TableSchema getTableSchema() {
        return this.schema;
    }

    public String explainSource() {
        return TableConnectorUtils.generateRuntimeName(this.getClass(), this.schema.getFieldNames());
    }

    public String[] getFieldNames() {
        return this.schema.getFieldNames();
    }

    public TypeInformation<?>[] getFieldTypes() {
        return this.schema.getFieldTypes();
    }

}
