package com.brickztech.nhkv.kafka.connect;

import com.github.jcustenborder.kafka.connect.json.FromJson;
import com.github.jcustenborder.kafka.connect.json.FromJsonTest;
import com.github.jcustenborder.kafka.connect.json.JsonConfig;
import com.github.jcustenborder.kafka.connect.utils.SinkRecordHelper;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class StringReplaceTest {

    StringReplace<SinkRecord> replaceTransform;
    SinkRecord fromJsonTransformed;

    @BeforeEach
    public void beforeEach() throws IOException {
        byte[] input = ByteStreams.toByteArray(FromJsonTest.class.getResourceAsStream("invoice.data.json"));
        File schemaFile = new File("src/test/resources/com/github/jcustenborder/kafka/connect/json/tszinvoices.json");
        Map<String, String> settings = ImmutableMap.of(
                JsonConfig.SCHEMA_URL_CONF, schemaFile.toURI().toString(),
                JsonConfig.VALIDATE_JSON_ENABLED_CONF, "true",
                JsonConfig.NUMBER_TO_TEXT_ENABLED_CONF, "true",
                JsonConfig.TRIM_AND_NULLIFY_TEXT_ENABLED_CONF, "true"
        );
        FromJson<SinkRecord> fromJson = new FromJson.Value<>();
        fromJson.configure(settings);
        SinkRecord inputRecord = SinkRecordHelper.write("foo", new SchemaAndValue(Schema.STRING_SCHEMA, "foo"), new SchemaAndValue(Schema.BYTES_SCHEMA, input));
        this.fromJsonTransformed = fromJson.apply(inputRecord);
        this.replaceTransform = new StringReplace<>();
    }

    @Test
    public void testBatchId() {
        Map<String, String> settings = ImmutableMap.of(
                StringReplaceConfig.FIELD_CONF, "batch_id",
                StringReplaceConfig.PATTERN_CONF, "[/:. ]",
                StringReplaceConfig.REPLACEMENT_CONF, ""
        );
        replaceTransform.configure(settings);
        SinkRecord hashedRecord = replaceTransform.apply(fromJsonTransformed);
        Struct hashed = (Struct) hashedRecord.value();
        String cleaned = hashed.getString("batch_id");
        assertThat(cleaned, is("20220725095157861"));
    }

}