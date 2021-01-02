package pers.tkh.flink;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.Host;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.ElasticsearchValidator;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.descriptors.StreamTableDescriptorValidator;
import org.apache.flink.table.factories.*;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;


public class ElasticsearchSourceFactory implements StreamTableSourceFactory<Row> {

    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap();
        context.put("connector.type", "elasticsearch");
        context.put("connector.version", "6");
        context.put("connector.property-version", "1");
        return context;
    }

    public List<String> supportedProperties() {
        List<String> properties = new ArrayList();
        properties.add("update-mode");
        properties.add("connector.hosts");
        properties.add("connector.hosts.#.hostname");
        properties.add("connector.hosts.#.port");
        properties.add("connector.hosts.#.protocol");
        properties.add("connector.index");
        properties.add("connector.document-type");
        properties.add("connector.key-delimiter");
        properties.add("connector.key-null-literal");
        properties.add("connector.failure-handler");
        properties.add("connector.failure-handler-class");
        properties.add("connector.flush-on-checkpoint");
        properties.add("connector.bulk-flush.max-actions");
        properties.add("connector.bulk-flush.max-size");
        properties.add("connector.bulk-flush.interval");
        properties.add("connector.bulk-flush.backoff.type");
        properties.add("connector.bulk-flush.backoff.max-retries");
        properties.add("connector.bulk-flush.backoff.delay");
        properties.add("connector.connection-max-retry-timeout");
        properties.add("connector.connection-path-prefix");
        properties.add("schema.#.data-type");
        properties.add("schema.#.type");
        properties.add("schema.#.name");
        properties.add("schema.#.expr");
        properties.add("schema.watermark.#.rowtime");
        properties.add("schema.watermark.#.strategy.expr");
        properties.add("schema.watermark.#.strategy.data-type");
        properties.add("format.*");
        return properties;
    }

    public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
        DescriptorProperties descriptorProperties = this.getValidatedProperties(properties);
        DeserializationSchema<Row> deserializationSchema = this.getDeserializationSchema(properties);
        return new ElasticsearchTableSource(TableSchemaUtils.getPhysicalSchema(descriptorProperties.getTableSchema("schema")), this.getHosts(descriptorProperties), descriptorProperties.getString("connector.index"), descriptorProperties.getString("connector.document-type"), deserializationSchema);
    }

    private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
        DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);
        (new StreamTableDescriptorValidator(true, false, true)).validate(descriptorProperties);
        (new SchemaValidator(true, false, false)).validate(descriptorProperties);
        (new ElasticsearchValidator()).validate(descriptorProperties);
        return descriptorProperties;
    }

    private List<Host> getHosts(DescriptorProperties descriptorProperties) {
        if (descriptorProperties.containsKey("connector.hosts")) {
            return ElasticsearchValidator.validateAndParseHostsString(descriptorProperties);
        } else {
            List<Map<String, String>> hosts = descriptorProperties.getFixedIndexedProperties("connector.hosts", Arrays.asList("hostname", "port", "protocol"));
            return (List)hosts.stream().map((host) -> {
                return new Host(descriptorProperties.getString((String)host.get("hostname")), descriptorProperties.getInt((String)host.get("port")), descriptorProperties.getString((String)host.get("protocol")));
            }).collect(Collectors.toList());
        }
    }

    private DeserializationSchema<Row> getDeserializationSchema(Map<String, String> properties) {
        DeserializationSchemaFactory<Row> formatFactory = (DeserializationSchemaFactory)TableFactoryService.find(DeserializationSchemaFactory.class, properties, this.getClass().getClassLoader());
        return formatFactory.createDeserializationSchema(properties);
    }
}
