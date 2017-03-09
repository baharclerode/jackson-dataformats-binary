package com.fasterxml.jackson.dataformat.avro.interop.annotations;

import org.apache.avro.reflect.AvroIgnore;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.dataformat.avro.interop.InteropTestBase;

import static com.fasterxml.jackson.dataformat.avro.interop.ApacheAvroInteropUtil.apacheDeserializer;
import static com.fasterxml.jackson.dataformat.avro.interop.ApacheAvroInteropUtil.getJacksonSchema;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class AvroIgnoreTest extends InteropTestBase
{
    static class RecordWithIgnoredField {
        public RecordWithIgnoredField() {}

        @AvroIgnore
        public String ignoredField;
        public String notIgnoredField;

    }

    @Before
    public void setup() {
        // 2.8 doesn't generate schemas with compatible namespaces for Apache deserializer
        Assume.assumeTrue(deserializeFunctor != apacheDeserializer || schemaFunctor != getJacksonSchema);
    }

    @Test
    public void testFieldIgnored() {
        RecordWithIgnoredField r = new RecordWithIgnoredField();
        r.ignoredField = "fail";
        r.notIgnoredField = "success";

        RecordWithIgnoredField processedR = roundTrip(r);
        assertThat(processedR, is(not(nullValue())));
        assertThat(processedR.ignoredField, is(nullValue()));
        assertThat(processedR.notIgnoredField, is(equalTo("success")));
    }
}
