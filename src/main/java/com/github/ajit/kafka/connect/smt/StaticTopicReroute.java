package com.github.ajit.kafka.connect.smt;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;


public abstract class StaticTopicReroute<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger logger = LoggerFactory.getLogger(StaticTopicReroute.class);

    private interface ConfigName {
        String TOPIC_NAME = "static.topic.name";
    }
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.TOPIC_NAME, ConfigDef.Type.STRING, "DefaultTopic", ConfigDef.Importance.HIGH,
                    "Static Topic Name");

    private static final String PURPOSE = "Routing all packets to a single user defined topic on kafka";

    private String staticTopicName;

    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        staticTopicName = config.getString(ConfigName.TOPIC_NAME);
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
        return newRecord(record, staticTopicName, null);
    }

    private R applyWithSchema(R record) {
        return newRecord(record, staticTopicName, null);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, String topicName, Map<String, Object> value);

    public static class Key<R extends ConnectRecord<R>> extends StaticTopicReroute<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, String topicName, Map<String, Object> value) {
            return record.newRecord(topicName, record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends StaticTopicReroute<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, String topicName, Map<String, Object> value) {
            return record.newRecord(topicName, record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), record.value(), record.timestamp());
        }
    }

}
