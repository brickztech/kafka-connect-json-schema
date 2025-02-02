package com.github.jcustenborder.kafka.connect.json;

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
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FromJsonTest {
    private static final Logger log = LoggerFactory.getLogger(FromJsonTest.class);
    FromJson<SinkRecord> transform;

    @BeforeEach
    public void beforeEach() {
        this.transform = new FromJson.Value<>();
    }

    @Test
    public void basic() throws IOException {
        byte[] input = ByteStreams.toByteArray(this.getClass().getResourceAsStream(
                "basic.data.json"
        ));
        File schemaFile = new File("src/test/resources/com/github/jcustenborder/kafka/connect/json/basic.schema.json");
        Map<String, String> settings = ImmutableMap.of(
                JsonConfig.SCHEMA_URL_CONF, schemaFile.toURI().toString()
        );
        this.transform.configure(settings);
        SinkRecord inputRecord = SinkRecordHelper.write("foo", new SchemaAndValue(Schema.STRING_SCHEMA, "foo"), new SchemaAndValue(Schema.BYTES_SCHEMA, input));
        SinkRecord transformedRecord = this.transform.apply(inputRecord);
        assertNotNull(transformedRecord);
        assertNotNull(transformedRecord.value());
        assertTrue(transformedRecord.value() instanceof Struct);
        Struct actual = (Struct) transformedRecord.value();
        log.info("actual = '{}'", actual);
    }

    @Test
    public void customdate() throws IOException {
        byte[] input = ByteStreams.toByteArray(this.getClass().getResourceAsStream(
                "customdate.data.json"
        ));
        File schemaFile = new File("src/test/resources/com/github/jcustenborder/kafka/connect/json/customdate.schema.json");
        Map<String, String> settings = ImmutableMap.of(
                JsonConfig.SCHEMA_URL_CONF, schemaFile.toURI().toString()
        );
        this.transform.configure(settings);
        SinkRecord inputRecord = SinkRecordHelper.write("foo", new SchemaAndValue(Schema.STRING_SCHEMA, "foo"), new SchemaAndValue(Schema.BYTES_SCHEMA, input));
        SinkRecord transformedRecord = this.transform.apply(inputRecord);
        assertNotNull(transformedRecord);
        assertNotNull(transformedRecord.value());
        assertTrue(transformedRecord.value() instanceof Struct);
        Struct actual = (Struct) transformedRecord.value();
        log.info("actual = '{}'", actual);
    }

    @Test
    public void validate() throws IOException {
        byte[] input = ByteStreams.toByteArray(this.getClass().getResourceAsStream("invoice.data.json"));
        File schemaFile = new File("src/test/resources/com/github/jcustenborder/kafka/connect/json/tszinvoices.json");
        Map<String, String> settings = ImmutableMap.of(
                JsonConfig.SCHEMA_URL_CONF, schemaFile.toURI().toString(),
                JsonConfig.VALIDATE_JSON_ENABLED_CONF, "true",
                JsonConfig.NUMBER_TO_TEXT_ENABLED_CONF, "true",
                JsonConfig.TRIM_AND_NULLIFY_TEXT_ENABLED_CONF, "true"
        );
        this.transform.configure(settings);
        SinkRecord inputRecord = SinkRecordHelper.write("foo", new SchemaAndValue(Schema.STRING_SCHEMA, "foo"), new SchemaAndValue(Schema.BYTES_SCHEMA, input));
        SinkRecord transformedRecord = this.transform.apply(inputRecord);
        assertTrue(transformedRecord.valueSchema().field("inv_actual_balance").schema().isOptional());
        assertTrue(transformedRecord.valueSchema().field("inv_net_amount").schema().type().equals(Schema.Type.STRING));
        assertTrue(transformedRecord.valueSchema().field("tsz_invoice_nr").schema().type().equals(Schema.Type.STRING));
        Struct transformedValue = (Struct) transformedRecord.value();
        String invZip = transformedValue.getString("inv_zip");
        assertThat(invZip.length(), is(invZip.trim().length()));
        assertThat(transformedValue.getString("inv_tax_nr"), is(nullValue()));
        assertThat(transformedValue.getString("inv_bank_account_nr"), is(nullValue()));
    }


    @Test
    public void wikiMediaRecentChange() throws IOException {
        byte[] input = ByteStreams.toByteArray(this.getClass().getResourceAsStream(
                "wikimedia.recentchange.data.json"
        ));
        File schemaFile = new File("src/test/resources/com/github/jcustenborder/kafka/connect/json/wikimedia.recentchange.schema.json");
        Map<String, String> settings = ImmutableMap.of(
                JsonConfig.SCHEMA_URL_CONF, schemaFile.toURI().toString(),
                JsonConfig.VALIDATE_JSON_ENABLED_CONF, "true",
                JsonConfig.EXCLUDE_LOCATIONS_CONF, "#/properties/log_params"
        );
        this.transform.configure(settings);
        SinkRecord inputRecord = SinkRecordHelper.write("foo", new SchemaAndValue(Schema.STRING_SCHEMA, "foo"), new SchemaAndValue(Schema.BYTES_SCHEMA, input));
        assertNotNull(inputRecord);
    }

    @Test
    public void foo() {
        String timestamp = "2020-01-07 04:47:05.0000000";
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSS")
                .withZone(ZoneId.of("UTC"));
        log.info(dateFormat.format(LocalDateTime.now()));
        ZonedDateTime dateTime = ZonedDateTime.parse(timestamp, dateFormat);
    }

}
