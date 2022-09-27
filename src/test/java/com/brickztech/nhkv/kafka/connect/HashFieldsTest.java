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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import static com.brickztech.nhkv.kafka.connect.Utils.bytesToHex;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

class HashFieldsTest {
    private static final Logger log = LoggerFactory.getLogger(HashFieldsTest.class);

    HashFields<SinkRecord> hashTransform;
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
        this.hashTransform = new HashFields<>();
    }

    @Test
    public void testWithoutNullFields() {
        Map<String, String> settings = ImmutableMap.of(
                HashFieldsConfig.FIELD_CONF, "IntegratorIndex",
                HashFieldsConfig.FROM_CONF, "public_service_int_provider_id,public_service_int_provider_name"
        );
        hashTransform.configure(settings);
        SinkRecord hashedRecord = hashTransform.apply(fromJsonTransformed);
        Struct hashed = (Struct) hashedRecord.value();
        String hashCode = hashed.getString("IntegratorIndex");
        assertThat(hashed.getString("IntegratorIndex"), is(notNullValue()));
        log.info("{} = {}", settings.get(HashFieldsConfig.FIELD_CONF), hashCode);
    }
    @Test
    public void aliasFieldsEquality() {
        Map<String, String> settings = ImmutableMap.of(
                HashFieldsConfig.FIELD_CONF, "Index1",
                HashFieldsConfig.FROM_CONF, "inv_place_of_service_zip" // aliassed to zip
        );
        hashTransform.configure(settings);
        SinkRecord hashedRecord = hashTransform.apply(fromJsonTransformed);
        Struct hashed = (Struct) hashedRecord.value();
        String hashCode1 = hashed.getString("Index1");
        
        Map<String, String> settings2 = ImmutableMap.of(
                HashFieldsConfig.FIELD_CONF, "Index2",
                HashFieldsConfig.FROM_CONF, "inv_service_user_zip" // aliassed to zip
        );
        hashTransform.configure(settings2);
        hashedRecord = hashTransform.apply(fromJsonTransformed);
        hashed = (Struct) hashedRecord.value();
        String hashCode2 = hashed.getString("Index2");
        
        assertThat(hashCode1, is(hashCode2));
    }
    
    @Test
    public void testWithNullFields() {
        Map<String, String> settings = ImmutableMap.of(
                HashFieldsConfig.FIELD_CONF, "HashFieldName",
                HashFieldsConfig.FROM_CONF, "inv_zip,inv_city,inv_tax_nr,inv_bank_account_nr"
        );
        hashTransform.configure(settings);
        SinkRecord hashedRecord = hashTransform.apply(fromJsonTransformed);
        Struct hashed = (Struct) hashedRecord.value();
        String hashCode = hashed.getString("HashFieldName");
        assertThat(hashCode, is(notNullValue()));
        log.info("{} = {}", settings.get(HashFieldsConfig.FIELD_CONF), hashCode);
    }

    @Test
    public void whenAllFieldsNullThanEmptyStringHashExpected() throws NoSuchAlgorithmException {
        Map<String, String> settings = ImmutableMap.of(
                HashFieldsConfig.FIELD_CONF, "HashFieldName",
                HashFieldsConfig.FROM_CONF, "repdoc_tax_nr,repdoc_group_tax_nr,repdoc_foreign_tax_nr_eu"
        );
        hashTransform.configure(settings);
        SinkRecord hashedRecord = hashTransform.apply(fromJsonTransformed);
        Struct hashed = (Struct) hashedRecord.value();
        String hashCode = hashed.getString("HashFieldName");
        assertThat(hashCode, is(""));
        log.info("{} = {}", settings.get(HashFieldsConfig.FIELD_CONF), hashCode);
    }

    @Test
    public void whenSwapedThenDiffers() {
        Map<String, String> settings1 = ImmutableMap.of(
                HashFieldsConfig.FIELD_CONF, "FieldPair1",
                HashFieldsConfig.FROM_CONF, "repdoc_invoice_name,repdoc_invoice_zip"
        );
        Map<String, String> settings2 = ImmutableMap.of(
                HashFieldsConfig.FIELD_CONF, "FieldPair2",
                HashFieldsConfig.FROM_CONF, "repdoc_invoice_city,repdoc_invoice_address"
        );
        hashTransform.configure(settings1);
        SinkRecord hashedRecord = hashTransform.apply(fromJsonTransformed);
        Struct hashed = (Struct) hashedRecord.value();
        String hashCode1 = hashed.getString("FieldPair1");

        hashTransform.configure(settings2);
        hashedRecord = hashTransform.apply(fromJsonTransformed);
        hashed = (Struct) hashedRecord.value();
        String hashCode2 = hashed.getString("FieldPair2");

        assertThat(hashCode1, not(hashCode2));
    }

    @Test
    public void whenFieldNameInvalidThenException() {
        Map<String, String> settings = ImmutableMap.of(
                HashFieldsConfig.FIELD_CONF, "IntegratorIndex",
                HashFieldsConfig.FROM_CONF, "public_service_int_provder_id,public_service_int_provder_name"
        );
        hashTransform.configure(settings);
        try {
            SinkRecord hashedRecord = hashTransform.apply(fromJsonTransformed);
            Struct hashed = (Struct) hashedRecord.value();
            String hashCode = hashed.getString("IntegratorIndex");
            assertThat(hashed.getString("IntegratorIndex"), is(notNullValue()));
            log.info("{} = {}", settings.get(HashFieldsConfig.FIELD_CONF), hashCode);
            assertThat("org.apache.kafka.connect.errors.DataException exception expected", false);
        } catch (org.apache.kafka.connect.errors.DataException ignore) {
        }
    }

}