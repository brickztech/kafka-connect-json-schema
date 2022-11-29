package com.brickztech.nhkv.kafka.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class HashFieldsConfig extends AbstractConfig {

    public static final String FIELD_CONF = "field";
    public static final String FIELD_DOC = "Field name that contains the hash value";

    public static final String FROM_CONF = "from";
    public static final String FROM_DOC = "Hash value calculated from these fields";

    public static final String OPTIONAL_CONF = "optional";
    public static final String OPTIONAL_DOC = "Schema of field definition optional";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_CONF, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, FIELD_DOC)
            .define(FROM_CONF, ConfigDef.Type.LIST, Collections.emptyList(), ConfigDef.Importance.HIGH, FROM_DOC)
            .define(OPTIONAL_CONF, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.HIGH, OPTIONAL_DOC);

    public final String field;
    public final List<String> from;
    public final boolean optional;

    public HashFieldsConfig(ConfigDef definition, Map<?, ?> originals) {
        super(definition, originals);
        field = getString(FIELD_CONF);
        from = getList(FROM_CONF);
        optional = getBoolean(OPTIONAL_CONF);
    }

}
