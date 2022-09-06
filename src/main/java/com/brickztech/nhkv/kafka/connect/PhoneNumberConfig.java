package com.brickztech.nhkv.kafka.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PhoneNumberConfig extends AbstractConfig {

    public static final String FIELDS_CONF = "fields";
    public static final String FIELDS_DOC = "List of phone field names";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELDS_CONF, ConfigDef.Type.LIST, Collections.emptyList(), ConfigDef.Importance.HIGH, FIELDS_DOC);

    public final List<String> fields;

    public PhoneNumberConfig(ConfigDef definition, Map<?, ?> originals) {
        super(definition, originals);
        fields = getList(FIELDS_CONF);
    }

}
