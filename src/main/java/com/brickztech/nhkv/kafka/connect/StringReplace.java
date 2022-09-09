package com.brickztech.nhkv.kafka.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.brickztech.nhkv.kafka.connect.Utils.bytesToHex;
import static com.github.jcustenborder.kafka.connect.json.Utils.requireMap;
import static com.github.jcustenborder.kafka.connect.json.Utils.requireStruct;

public class StringReplace <R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String PURPOSE = "string replace";
    private StringReplaceConfig config;

    @Override
    public R apply(R record) {
        if (record.value() == null) {
            return record;
        } else if (record.valueSchema() == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(record.value(), PURPOSE);
        Map<String, Object> updatedValue = new HashMap<>(value);
        Optional<Object> fieldValue = Optional.ofNullable(value.get(config.field));
        fieldValue.ifPresent(v -> {
            updatedValue.put(config.field, v.toString().replaceAll(config.pattern, config.replacement));
        });
        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(record.value(), PURPOSE);
        Struct updatedValue = new Struct(value.schema());
        Optional<Object> fieldValue = Optional.ofNullable(value.get(config.field));
        fieldValue.ifPresent(v -> {
            updatedValue.put(config.field, v.toString().replaceAll(config.pattern, config.replacement));
        });
        return newRecord(record, value.schema(), updatedValue);
    }

    @Override
    public ConfigDef config() {
        return StringReplaceConfig.CONFIG_DEF;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        config = new StringReplaceConfig(StringReplaceConfig.CONFIG_DEF, configs);
    }

    private R newRecord(R record, Schema updatedSchema, Object updatedValue) {
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                updatedSchema, updatedValue, record.timestamp());
    }
}
