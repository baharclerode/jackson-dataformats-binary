package com.fasterxml.jackson.dataformat.avro.interop.annotations;

import lombok.Data;
import org.apache.avro.Schema;
import org.apache.avro.reflect.AvroDefault;
import org.junit.Test;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.dataformat.avro.interop.ApacheAvroInteropUtil;

import static org.assertj.core.api.Assertions.assertThat;

public class AvroDefaultTest {

    @Data
    public static class RecordWithDefaults {

        @AvroDefault("\"Test Field\"")
        private String stringField;

    }

    @Data
    @JsonClassDescription("Class Description")
    public static class RecordWithJacksonDefaults {

        @JsonProperty(defaultValue = "\"test\"", required = true)
        @JsonPropertyDescription("Test Description")
        private String stringField;

    }

    @Test
    public void testStringAvroDefault() {
        Schema apacheSchema = ApacheAvroInteropUtil.getApacheSchema(RecordWithDefaults.class);
        Schema jacksonSchema = ApacheAvroInteropUtil.getJacksonSchema(RecordWithDefaults.class);
        //
        assertThat(jacksonSchema.getField("stringField").defaultValue()).isEqualTo(apacheSchema.getField("stringField").defaultValue());
    }

    @Test
    public void testStringJacksonDefault() {
        Schema jacksonSchema = ApacheAvroInteropUtil.getJacksonSchema(RecordWithJacksonDefaults.class);
        //
        assertThat(jacksonSchema.getField("stringField").defaultValue().asText()).isEqualTo("test");
        assertThat(jacksonSchema.getField("stringField").doc()).isEqualTo("Test Description");
        assertThat(jacksonSchema.getDoc()).isEqualTo("Class Description");
    }

}
