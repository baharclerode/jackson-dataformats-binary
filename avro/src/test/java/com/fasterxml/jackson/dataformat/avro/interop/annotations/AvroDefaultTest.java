package com.fasterxml.jackson.dataformat.avro.interop.annotations;

import org.apache.avro.Schema;
import org.apache.avro.reflect.AvroDefault;
import org.junit.Test;

import com.fasterxml.jackson.dataformat.avro.interop.ApacheAvroInteropUtil;

import lombok.Data;

import static org.assertj.core.api.Assertions.assertThat;

public class AvroDefaultTest {

    @Data
    public static class RecordWithDefaults {

        @AvroDefault("\"Test Field\"")
        private String stringField;

    }

    @Test
    public void testStringDefault() {
        Schema apacheSchema = ApacheAvroInteropUtil.getApacheSchema(RecordWithDefaults.class);
        Schema jacksonSchema = ApacheAvroInteropUtil.getJacksonSchema(RecordWithDefaults.class);
        //
        assertThat(jacksonSchema.getField("stringField").defaultValue()).isEqualTo(apacheSchema.getField("stringField").defaultValue());
    }

}
