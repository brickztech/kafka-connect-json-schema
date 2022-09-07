package com.brickztech.nhkv.kafka.connect;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.github.jcustenborder.kafka.connect.json.Utils.*;

public class HashFields<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String PURPOSE = "hash fields";
    private Cache<Schema, Schema> schemaUpdateCache;
    private HashFieldsConfig config;
    private MessageDigest digest;

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
        StringBuilder data = new StringBuilder();
        value.keySet().stream().filter(config.from::contains).forEach(key -> {
            Optional<Object> fieldValue = Optional.ofNullable(value.get(key));
            fieldValue.ifPresent(data::append);
        });
        String hashCode = bytesToHex(digest.digest(data.toString().getBytes(StandardCharsets.UTF_8)));
        Map<String, Object> updatedValue = new HashMap<>(value);
        updatedValue.put(config.field, hashCode);
        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(record.value(), PURPOSE);
        StringBuilder data = new StringBuilder();
        value.schema().fields().stream().map(Field::name).filter(config.from::contains).forEach(key -> {
            Optional<Object> fieldValue = Optional.ofNullable(value.get(key));
            fieldValue.ifPresent(data::append);
        });
        String hashCode = bytesToHex(digest.digest(data.toString().getBytes(StandardCharsets.UTF_8)));
        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }
        Struct updatedValue = new Struct(updatedSchema);
        value.schema().fields().stream().forEach(field -> updatedValue.put(field.name(), value.get(field)));
        updatedValue.put(config.field, hashCode);
        return newRecord(record, updatedSchema, updatedValue);
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        schema.fields().stream().forEach(field -> builder.field(field.name(), field.schema()));
        builder.field(config.field, Schema.STRING_SCHEMA);
        return builder.build();
    }

    private String bytesToHex(byte[] hash) {
        StringBuilder hexString = new StringBuilder(2 * hash.length);
        for (int i = 0; i < hash.length; i++) {
            String hex = Integer.toHexString(0xff & hash[i]);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }

    @Override
    public ConfigDef config() {
        return HashFieldsConfig.CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> map) {
        try {
            config = new HashFieldsConfig(HashFieldsConfig.CONFIG_DEF, map);
            digest = MessageDigest.getInstance("SHA-256");
            schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private R newRecord(R record, Schema updatedSchema, Object updatedValue) {
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                updatedSchema, updatedValue, record.timestamp());
    }

}
