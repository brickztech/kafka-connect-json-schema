/**
 * Copyright © 2020 Jeremy Custenborder (jcustenborder@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.json;

import org.everit.json.schema.internal.DateFormatValidator;
import org.everit.json.schema.internal.DateTimeFormatValidator;
import org.everit.json.schema.internal.TimeFormatValidator;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;

public class TestUtils {
    public static org.everit.json.schema.Schema jsonSchema(JSONObject rawSchema) {
        return SchemaLoader.builder()
                .draftV7Support()
                .addFormatValidator(new DateFormatValidator())
                .addFormatValidator(new TimeFormatValidator())
                .addFormatValidator(new DateTimeFormatValidator())
                .schemaJson(rawSchema)
                .build()
                .load()
                .build();
    }
}
