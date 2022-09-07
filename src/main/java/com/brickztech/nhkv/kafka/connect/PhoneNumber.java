package com.brickztech.nhkv.kafka.connect;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import javax.swing.text.MaskFormatter;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;

import static com.github.jcustenborder.kafka.connect.json.Utils.requireMap;
import static com.github.jcustenborder.kafka.connect.json.Utils.requireStruct;

public class PhoneNumber<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String PURPOSE = "phone number formatter";
    private Map<Pattern, Function<String, String>> patterns;
    private Cache<Schema, Schema> schemaUpdateCache;
    private PhoneNumberConfig config;
    private MaskFormatter formatter7;
    private MaskFormatter formatter8;

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
        config.fields.stream().forEach(field -> updatedValue.put(field, formatNumber((String) value.get(field))));
        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(record.value(), PURPOSE);
        Struct updatedValue = new Struct(value.schema());
        value.schema().fields().stream().forEach(field -> updatedValue.put(field.name(), value.get(field)));
        config.fields.stream().forEach(field -> updatedValue.put(field, formatNumber(value.getString(field))));
        return newRecord(record, value.schema(), updatedValue);
    }

    private String formatNumber(String phoneNumber) {
        if (phoneNumber != null) {
            try {
                String cleanedNumber = phoneNumber.replaceAll("[ \\+\\_()/-]", "");
                for (Map.Entry<Pattern, Function<String, String>> entry : patterns.entrySet()) {
                    if (entry.getKey().matcher(cleanedNumber).find()) {
                        String value = entry.getValue().apply(cleanedNumber);
                        if (value.startsWith("1")) {
                            return formatter7.valueToString(value).trim();
                        }
                        return formatter8.valueToString(value).trim();
                    }
                }
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }
        return phoneNumber;
    }

    @Override
    public ConfigDef config() {
        return PhoneNumberConfig.CONFIG_DEF;
    }

    private R newRecord(R record, Schema updatedSchema, Object updatedValue) {
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                updatedSchema, updatedValue, record.timestamp());
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        try {
            config = new PhoneNumberConfig(PhoneNumberConfig.CONFIG_DEF, configs);
            schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
            patterns = new HashMap<>();
            patterns.put(Pattern.compile("\\b[1-9]\\d{7,8}\\b"), phoneNumber -> phoneNumber);
            patterns.put(Pattern.compile("\\b06[1-9]\\d{7,8}\\b"), phoneNumber -> phoneNumber.substring(2));
            patterns.put(Pattern.compile("\\b36[1-9]\\d{7,8}\\b"), phoneNumber -> phoneNumber.substring(2));
            patterns.put(Pattern.compile("\\b0036[1-9]\\d{7,8}\\b"), phoneNumber -> phoneNumber.substring(4));
            formatter7 = new MaskFormatter("(#) ### ####");
            formatter7.setValueContainsLiteralCharacters(false);
            formatter8 = new MaskFormatter("(##) ### ####");
            formatter8.setValueContainsLiteralCharacters(false);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

}
