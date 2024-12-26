package com.github.ajit.kafka.connect.smt;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;


public abstract class KeyBasedFilter <R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger logger = LoggerFactory.getLogger(KeyBasedFilter.class);

    private interface ConfigName {
        String CONFIG_KEY_NAME = "filter.key";
        String CONFIG_KEY_VALUE = "filter.value";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(KeyBasedFilter.ConfigName.CONFIG_KEY_NAME, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW,
                    "Key to check")
            .define(KeyBasedFilter.ConfigName.CONFIG_KEY_VALUE, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW,
                    "Value to filter out");

    private static final String PURPOSE = "Filter out packets based on key value pair";
    private String keyName;
    private String keyValue;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        keyName = config.getString(KeyBasedFilter.ConfigName.CONFIG_KEY_NAME);
        keyValue = config.getString(KeyBasedFilter.ConfigName.CONFIG_KEY_VALUE);
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
        if(value.containsKey(keyName) && String.valueOf(value.get(keyName)).equalsIgnoreCase(keyValue)){
            logger.info("field: {}, on which packet are to be filtered does not exist in record. --> {}", keyName, record);
            return null;
        }
        return record;
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);
        try {
            if (String.valueOf(value.get(keyName)).equalsIgnoreCase(keyValue)) {
                return null;
            }
        }catch (DataException e){
            logger.info("Original field: {}, does not exist in record.", keyName);
        }
        return record;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }



    @Override
    public void close() {}

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends KeyBasedFilter<R> {

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

    public static class Value<R extends ConnectRecord<R>> extends KeyBasedFilter<R> {

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
