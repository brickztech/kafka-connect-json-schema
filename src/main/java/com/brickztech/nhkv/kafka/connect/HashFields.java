package com.brickztech.nhkv.kafka.connect;

import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationTip;
import com.github.jcustenborder.kafka.connect.utils.config.Title;
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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.brickztech.nhkv.kafka.connect.Utils.bytesToHex;
import static com.github.jcustenborder.kafka.connect.json.Utils.*;

@Title("Hash fields transformation")
@Description("The HashFields will generate SHA-256 fingerprint from specified fields.")
@DocumentationTip("This transformation expects data to be in either Struct or Map format.")
public class HashFields<R extends ConnectRecord<R>> implements Transformation<R> {
	
    private static final String PURPOSE = "hash fields";
    private Cache<Schema, Schema> schemaUpdateCache;
    private HashFieldsConfig config;
    private MessageDigest digest;
    
    static final Properties aliasProps = new Properties();
    static {
    	try {
    		InputStream confStream = HashFields.class.getClassLoader().getResourceAsStream("com/brickztech/nhkv/kafka/connect/field-alias.properties");
    		if(confStream!=null) {
    			aliasProps.load(confStream);
    		}else {
    			throw new RuntimeException("alias config stream is null");
    		}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
    	if(aliasProps.size()==0) {
    		throw new RuntimeException("alias config is not found");
    	}else {
    		System.out.println("aliasProps.size="+aliasProps.size());
    	}
    }
    

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
        config.from.stream().forEach(key -> {
            Optional<Object> fieldValue = Optional.ofNullable(value.get(key));
            fieldValue.ifPresent(data::append);
            fieldValue.ifPresent(v -> data.append(key).append(':').append(v));
        });
        String hashCode = bytesToHex(digest.digest(data.toString().getBytes(StandardCharsets.UTF_8)));
        Map<String, Object> updatedValue = new HashMap<>(value);
        updatedValue.put(config.field, hashCode);
        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(record.value(), PURPOSE);
        StringBuilder hashData = new StringBuilder();
        
        config.from.stream().forEach(key -> {
            Optional<Object> field = Optional.ofNullable(value.get(key));
            if(field.isPresent()) {
            	Object fieldValue = field.get();
            	if(isNotEmpty(fieldValue)) {
                    String jsonField = HashFields.aliasProps.getProperty(key.toLowerCase(), key);
                	hashData.append(jsonField).append(':').append(fieldValue);
            	}
            }
        });
        String hashCode = ""; 
        if(hashData.length() > 0) {
        	hashCode = bytesToHex(digest.digest(hashData.toString().getBytes(StandardCharsets.UTF_8)));
        }
        
        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }
        Struct updatedValue = new Struct(updatedSchema);
        value.schema().fields().stream().forEach(field -> updatedValue.put(field.name(), value.get(field)));
        
        String hashName = HashFields.aliasProps.getProperty(config.field.toLowerCase(), config.field);
        updatedValue.put(hashName, hashCode);
        return newRecord(record, updatedSchema, updatedValue);
    }

	private boolean isNotEmpty(Object fieldValue) {
		return (fieldValue!=null && !(fieldValue instanceof String)) ||
				(fieldValue!=null && (fieldValue instanceof String) && !((String)fieldValue).trim().equals(""));
	}

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        schema.fields().stream().forEach(field -> builder.field(field.name(), field.schema()));
        builder.field(config.field, Schema.STRING_SCHEMA);
        return builder.build();
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
