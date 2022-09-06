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

class PhoneNumberTest {

    private PhoneNumber<SinkRecord> phoneTransform;
    private SinkRecord fromJsonTransformed;

    @BeforeEach
    public void beforeEach() throws IOException {
        byte[] input = ByteStreams.toByteArray(FromJsonTest.class.getResourceAsStream("customer.json"));
        File schemaFile = new File("src/test/resources/com/github/jcustenborder/kafka/connect/json/nhkv/tszcustomers.json");
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
        this.phoneTransform = new PhoneNumber<>();
    }

    @Test
    public void testFormat() {
        Map<String, String> settings = ImmutableMap.of(
                PhoneNumberConfig.FIELDS_CONF, "cust_phone,service_user_phone"
        );
        phoneTransform.configure(settings);
        SinkRecord alteredRecord = phoneTransform.apply(fromJsonTransformed);
        Struct altered = (Struct) alteredRecord.value();
        assertThat(altered.getString("cust_phone"), is("(70) 361 7824"));
        assertThat(altered.getString("service_user_phone"), is("(30) 372 0469"));
    }

}