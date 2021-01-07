package pers.tkh.flink;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;

public class ElasticsearchTableSource implements ScanTableSource {
    private final String hosts;
    private final String indices;
    private final String types;
    private final int size;
    private final int slices;
    private final TableSchema schema;
    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

    public ElasticsearchTableSource(String hosts, String indices, String types, int size, int slices, TableSchema schema,
                                    DecodingFormat<DeserializationSchema<RowData>> decodingFormat) {
        this.hosts = hosts;
        this.indices = indices;
        this.types = types;
        this.size = size;
        this.slices = slices;
        this.schema = schema;
        this.decodingFormat = decodingFormat;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return decodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final DeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(
                runtimeProviderContext, schema.toPhysicalRowDataType());
        final ElasticsearchSource source = new ElasticsearchSource(hosts, indices, types, size, slices,
                schema.getFieldNames(), deserializer);
        return SourceProvider.of(source);
    }

    @Override
    public DynamicTableSource copy() {
        return new ElasticsearchTableSource(hosts, indices, types, size, slices, schema, decodingFormat);
    }

    @Override
    public String asSummaryString() {
        return "Elasticsearch Table Source";
    }
}
