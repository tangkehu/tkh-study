package com.tkh.flink;

import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;

import java.util.*;


public class EsStreamTableSourceFactory implements StreamTableSourceFactory<Row> {
    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put("connector.type", "elasticsearch");
        context.put("connector.version", "5");
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();
        properties.add("connector.hosts");
        properties.add("connector.index");
        properties.add("connector.document-type");
        properties.add("schema.#.data-type");
        properties.add("schema.#.type");
        properties.add("schema.#.name");
        properties.add("schema.#.from");
        properties.add("schema.#.expr");
        properties.add("schema.#.proctime");
        properties.add("schema.#.rowtime.timestamps.type");
        properties.add("schema.#.rowtime.timestamps.from");
        properties.add("schema.#.rowtime.timestamps.class");
        properties.add("schema.#.rowtime.timestamps.serialized");
        properties.add("schema.#.rowtime.watermarks.type");
        properties.add("schema.#.rowtime.watermarks.class");
        properties.add("schema.#.rowtime.watermarks.serialized");
        properties.add("schema.#.rowtime.watermarks.delay");
        properties.add("schema.watermark.#.rowtime");
        properties.add("schema.watermark.#.strategy.expr");
        properties.add("schema.watermark.#.strategy.data-type");
        properties.add("format.*");
        return properties;
    }

    @Override
    public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
        DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);
        (new SchemaValidator(true, false, false)).validate(descriptorProperties);
        String hosts = descriptorProperties.getString("connector.hosts");
        String index = descriptorProperties.getString("connector.index");
        String docType = descriptorProperties.getString("connector.document-type");
        DeserializationSchemaFactory<Row> formatFactory = (DeserializationSchemaFactory) TableFactoryService.find(DeserializationSchemaFactory.class, properties, this.getClass().getClassLoader());

        return new EsStreamTableSource(TableSchemaUtils.getPhysicalSchema(descriptorProperties.getTableSchema("schema")), index, docType, hosts, formatFactory.createDeserializationSchema(properties));
    }
}
