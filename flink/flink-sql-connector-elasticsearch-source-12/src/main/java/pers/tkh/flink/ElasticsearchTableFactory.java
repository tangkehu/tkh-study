package pers.tkh.flink;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

public class ElasticsearchTableFactory implements DynamicTableSourceFactory {
    public static final ConfigOption<String> HOSTS = ConfigOptions.key("hosts").stringType().defaultValue("http://127.0.0.1:9200");
    public static final ConfigOption<String> INDICES = ConfigOptions.key("indices").stringType().noDefaultValue();
    public static final ConfigOption<String> TYPES = ConfigOptions.key("types").stringType().noDefaultValue();
    public static final ConfigOption<Integer> SIZE = ConfigOptions.key("size").intType().defaultValue(1000);
    public static final ConfigOption<Integer> SLICES = ConfigOptions.key("slices").intType().defaultValue(5);

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
                DeserializationFormatFactory.class, FactoryUtil.FORMAT);
        helper.validate();
        final ReadableConfig options = helper.getOptions();
        final String hosts = options.get(HOSTS);
        final String indices = options.get(INDICES);
        final String types = options.get(TYPES);
        final int size = options.get(SIZE);
        final int slices = options.get(SLICES);
        final TableSchema schema = context.getCatalogTable().getSchema();
        return new ElasticsearchTableSource(hosts, indices, types, size, slices, schema, decodingFormat);
    }

    @Override
    public String factoryIdentifier() {
        return "elasticsearch";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTS);
        options.add(INDICES);
        options.add(TYPES);
        options.add(FactoryUtil.FORMAT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SIZE);
        options.add(SLICES);
        return options;
    }
}
