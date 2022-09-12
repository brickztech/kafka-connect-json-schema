package com.brickztech.nhkv.kafka.connect;

import org.apache.kafka.common.protocol.types.Field;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.swing.text.MaskFormatter;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class MaskFormatterTest {

    private static Map<Pattern, Function<String, String>> patterns = new HashMap<>();
    static {
        patterns.put(Pattern.compile("\\b[1-9]\\d{7,8}\\b"), phoneNumber -> phoneNumber);
        patterns.put(Pattern.compile("\\b06[1-9]\\d{7,8}\\b"), phoneNumber -> phoneNumber.substring(2));
        patterns.put(Pattern.compile("\\b36[1-9]\\d{7,8}\\b"), phoneNumber -> phoneNumber.substring(2));
        patterns.put(Pattern.compile("\\b0036[1-9]\\d{7,8}\\b"), phoneNumber -> phoneNumber.substring(4));
    }

    private Map<String, String> FIXTURES;
    private MaskFormatter formatter7;
    private MaskFormatter formatter8;
    private Pattern numbers = Pattern.compile("\\d+");

    @BeforeEach
    private void beforeEach() throws ParseException {
        formatter7 = new MaskFormatter("(#) ### ####");
        formatter7.setValueContainsLiteralCharacters(false);
        formatter8 = new MaskFormatter("(##) ### ####");
        formatter8.setValueContainsLiteralCharacters(false);

        FIXTURES = new HashMap<>();
        FIXTURES.put("(+36)70/361-78-24", "(70) 361 7824");
        FIXTURES.put("1/436-05-13", "(1) 436 0513");
        FIXTURES.put("06-30-37-20-469", "(30) 372 0469");
        FIXTURES.put("(20) 45-66-007", "(20) 456 6007");
        FIXTURES.put("(20)9-857-510", "(20) 985 7510");
        FIXTURES.put("(20/918-4370)", "(20) 918 4370");

        FIXTURES.put("66/256-100/131", "66/256-100/131");
        FIXTURES.put("+49 172 9947994", "+49 172 9947994");
        FIXTURES.put("ZK:05.01.-10.3", "ZK:05.01.-10.3");
        FIXTURES.put("TEL.MÁSÉ LETT", "TEL.MÁSÉ LETT");
        FIXTURES.put("NAGY IMRE FIA", "NAGY IMRE FIA");
        FIXTURES.put("MITYÓKÉ", "MITYÓKÉ");
        FIXTURES.put("93/519-911,912", "93/519-911,912");
        FIXTURES.put("93/519-911;912", "93/519-911;912");
        FIXTURES.put("93/519-911.912", "93/519-911.912");
    }


    @Test
    public void testFormatNumber() throws ParseException {
        for (Map.Entry<String, String> entry: FIXTURES.entrySet()) {
            assertThat(formatNumber(entry.getKey()), is(entry.getValue()));
        }
    }

    private String formatNumber(String phoneNumber) throws ParseException {
//        String cleanedNumber = phoneNumber.replaceAll("[, \\+\\_()/-]", "");
        String cleanedNumber = phoneNumber.replaceAll("\\D+", "");
        for (Map.Entry<Pattern, Function<String, String>> entry : patterns.entrySet()) {
            if (entry.getKey().matcher(cleanedNumber).find()) {
                String value = entry.getValue().apply(cleanedNumber);
                if (value.startsWith("1")) {
                    return formatter7.valueToString(value).trim();
                }
                return formatter8.valueToString(value).trim();
            }
        }
        return phoneNumber;
    }

}
