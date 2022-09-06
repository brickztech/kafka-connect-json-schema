package com.brickztech.nhkv.kafka.connect;

import org.junit.jupiter.api.Test;

import javax.swing.text.MaskFormatter;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
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

    private String formatNumber(String phoneNumber) throws ParseException {
        MaskFormatter formatter7 = new MaskFormatter("(#) ### ####");
        formatter7.setValueContainsLiteralCharacters(false);
        MaskFormatter formatter8 = new MaskFormatter("(##) ### ####");
        formatter8.setValueContainsLiteralCharacters(false);
        String cleanedNumber = phoneNumber.replaceAll("[\\+\\_()/-]", "").replaceAll(" ", "");
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

    @Test
    public void testCases() throws ParseException {
        assertThat(formatNumber("(+36)70/361-78-24"), is("(70) 361 7824"));
        assertThat(formatNumber("1/436-05-13"), is("(1) 436 0513"));
        assertThat(formatNumber("06-30-37-20-469"), is("(30) 372 0469"));
        assertThat(formatNumber("(20) 45-66-007"), is("(20) 456 6007"));
        assertThat(formatNumber("(20)9-857-510"), is("(20) 985 7510"));
        assertThat(formatNumber("(20/918-4370)"), is("(20) 918 4370"));
        assertThat(formatNumber("66/256-100/131"), is("66/256-100/131"));
        assertThat(formatNumber("+49 172 9947994"), is("+49 172 9947994"));
    }

}
