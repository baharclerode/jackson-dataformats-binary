package com.fasterxml.jackson.dataformat.avro.interop.records;

import lombok.Data;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.dataformat.avro.interop.InteropTestBase;

import static com.fasterxml.jackson.dataformat.avro.interop.ApacheAvroInteropUtil.apacheDeserializer;
import static com.fasterxml.jackson.dataformat.avro.interop.ApacheAvroInteropUtil.getJacksonSchema;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests serializing primitive array fields on records
 */
public class RecordWithPrimitiveWrapperArrayTest extends InteropTestBase {
    @Data
    public static class TestRecord {
        private Byte[]      byteArrayField      = new Byte[0];
        private Short[]     shortArrayField     = new Short[0];
        private Character[] characterArrayField = new Character[0];
        private Integer[]   integerArrayField   = new Integer[0];
        private Long[]      longArrayField      = new Long[0];
        private Float[]     floatArrayField     = new Float[0];
        private Double[]    doubleArrayField    = new Double[0];
        private String[]    stringArrayField    = new String[0];
    }

    @Before
    public void setup() {
        // 2.8 doesn't generate schemas with compatible namespaces for Apache deserializer
        Assume.assumeTrue(deserializeFunctor != apacheDeserializer || schemaFunctor != getJacksonSchema);
    }

    @Test
    public void testByteField() {
        TestRecord record = new TestRecord();
        record.byteArrayField = new Byte[]{1, 0, -1, Byte.MIN_VALUE, Byte.MAX_VALUE};
        //
        TestRecord result = roundTrip(record);
        //
        assertThat(result.byteArrayField).isEqualTo(record.byteArrayField);
    }

    @Test
    public void testCharacterField() {
        TestRecord record = new TestRecord();
        record.characterArrayField = new Character[]{1, 0, Character.MIN_VALUE, Character.MAX_VALUE};
        //
        TestRecord result = roundTrip(record);
        //
        assertThat(result.characterArrayField).isEqualTo(record.characterArrayField);
    }

    @Test
    public void testDoubleField() {
        TestRecord record = new TestRecord();
        record.doubleArrayField = new Double[]{1D, 0D, -1D, Double.MIN_VALUE, Double.MAX_VALUE};
        //
        TestRecord result = roundTrip(record);
        //
        assertThat(result.doubleArrayField).isEqualTo(record.doubleArrayField);
    }

    @Test
    public void testFloatField() {
        TestRecord record = new TestRecord();
        record.floatArrayField = new Float[]{1F, 0F, -1F, Float.MIN_VALUE, Float.MAX_VALUE};
        //
        TestRecord result = roundTrip(record);
        //
        assertThat(result.floatArrayField).isEqualTo(record.floatArrayField);
    }

    @Test
    public void testInteger() {
        TestRecord record = new TestRecord();
        record.integerArrayField = new Integer[]{1, 0, -1, Integer.MIN_VALUE, Integer.MAX_VALUE};
        //
        TestRecord result = roundTrip(record);
        //
        assertThat(result.integerArrayField).isEqualTo(record.integerArrayField);
    }

    @Test
    public void testLongField() {
        TestRecord record = new TestRecord();
        record.longArrayField = new Long[]{1L, 0L, -1L, Long.MIN_VALUE, Long.MAX_VALUE};
        //
        TestRecord result = roundTrip(record);
        //
        assertThat(result.longArrayField).isEqualTo(record.longArrayField);
    }

    @Test
    public void testShortField() {
        TestRecord record = new TestRecord();
        record.shortArrayField = new Short[]{1, 0, -1, Short.MIN_VALUE, Short.MAX_VALUE};
        //
        TestRecord result = roundTrip(record);
        //
        assertThat(result.shortArrayField).isEqualTo(record.shortArrayField);
    }

    @Test
    public void testStringField() {
        TestRecord record = new TestRecord();
        record.stringArrayField = new String[]{"", "one", "HelloWorld"};
        //
        TestRecord result = roundTrip(record);
        //
        assertThat(result.stringArrayField).isEqualTo(record.stringArrayField);
    }
}
