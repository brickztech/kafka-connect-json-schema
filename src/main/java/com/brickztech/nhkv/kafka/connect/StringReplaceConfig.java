package com.brickztech.nhkv.kafka.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Collections;
import java.util.Map;

public class StringReplaceConfig extends AbstractConfig {

    public static final String FIELD_CONF = "field";
    public static final String FIELD_DOC = "Field name that contains the value";

    public static final String PATTERN_CONF = "pattern";
    public static final String PATTERN_DOC = "Pattern passed to String.replaceAll(pattern, replacement)";

    public static final String REPLACEMENT_CONF = "replacement";
    public static final String REPLACEMENT_DOC = "Replacement value passed to String.replaceAll(pattern, replacement)";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_CONF, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, FIELD_DOC)
            .define(PATTERN_CONF, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, PATTERN_DOC)
            .define(REPLACEMENT_CONF, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, REPLACEMENT_DOC);

    public final String field;
    public final String pattern;
    public final String replacement;

    public StringReplaceConfig(ConfigDef definition, Map<?, ?> originals) {
        super(definition, originals);
        field = getString(FIELD_CONF);
        pattern = getString(PATTERN_CONF);
        replacement = getString(REPLACEMENT_CONF);
    }

}
