package com.github.jcustenborder.kafka.connect.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.everit.json.schema.ValidationException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class NhkvSchemaTest {

    @Test
    public void loadSchema() throws IOException {
        File schemaFile = new File("src/test/resources/com/github/jcustenborder/kafka/connect/json/nhkv/tszcustomers.json");
        FileInputStream inputStream = new FileInputStream(schemaFile);
        org.everit.json.schema.Schema schema = Utils.loadSchema(inputStream);
        File file = new File("src/test/resources/com/github/jcustenborder/kafka/connect/json/nhkv/customer.json");
        JSONObject data = Utils.loadObject(new FileInputStream(file));
        try {
            schema.validate(data);
        } catch (ValidationException e) {
            e.getAllMessages().stream().filter(msg -> !msg.contains("expected: null, found")).forEach(System.out::println);
            throw e;
        }
    }

}
