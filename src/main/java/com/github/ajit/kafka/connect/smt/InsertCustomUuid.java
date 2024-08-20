package com.github.ajit.kafka.connect.smt;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class InsertCustomUuid<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger logger = LoggerFactory.getLogger(InsertCustomUuid.class);

    private interface ConfigName {
        String ORIGINAL_FIELD_NAME = "original.field.name";
        String CUSTOM_FIELD_NAME = "custom.field.name";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.ORIGINAL_FIELD_NAME, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW,
                    "Original field name")
            .define(ConfigName.CUSTOM_FIELD_NAME, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW,
                    "Clone field name");

    private static final String PURPOSE = "Cloning a key in record";

    private String originalFieldName;
    private String customFieldName;

    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        originalFieldName = config.getString(ConfigName.ORIGINAL_FIELD_NAME);
        customFieldName = config.getString(ConfigName.CUSTOM_FIELD_NAME);
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
    }


    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
        if(!value.containsKey(originalFieldName)){
            logger.info("Original field: {}, does not exist in record.", originalFieldName);
            return null;
        }

        final Map<String, Object> updatedValue = new HashMap<>(value);
        updatedValue.put(customFieldName, value.get(originalFieldName));
        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);
        try {
            value.get(originalFieldName);
        }catch (DataException e){
            logger.info("Original field: {}, does not exist in record.", originalFieldName);
            return null;
        }
        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if(updatedSchema == null) {
            logger.info("Schema cache is null, creating new");
            logger.info("Current schema is: {}", value.schema());
            updatedSchema = makeUpdatedSchema(value.schema());
            logger.info("Updated schema is : {}", updatedSchema.fields());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);
        for (Field field : value.schema().fields()) {
            updatedValue.put(field.name(), value.get(field));
        }
        updatedValue.put(customFieldName, value.get(originalFieldName));
        return newRecord(record, updatedSchema, updatedValue);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    private Schema makeUpdatedSchema(Schema schema) {
        logger.info("Making updated schema");
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        logger.info("builder created successfully");
        logger.info("Fields are : {}", schema.fields());
        for (Field field: schema.fields()) {
            logger.info("Adding field: {}, {}", field.name(), field.schema());
            builder.field(field.name(), field.schema());

        }
        logger.info("Adding custom field with data type: {}, {}",customFieldName, Schema.STRING_SCHEMA);
        try {
            builder.field(customFieldName, SchemaBuilder.OPTIONAL_STRING_SCHEMA);
            logger.info("Custom field added successfully");
        }catch (Exception e){
            logger.info("Custom field addition failed!!");
        }
        return builder.build();
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends InsertCustomUuid<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends InsertCustomUuid<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }

    }
}
