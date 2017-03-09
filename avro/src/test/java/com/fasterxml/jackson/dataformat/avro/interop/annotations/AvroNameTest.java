package com.fasterxml.jackson.dataformat.avro.interop.annotations;

import org.apache.avro.reflect.AvroName;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.dataformat.avro.AvroTestBase;
import com.fasterxml.jackson.dataformat.avro.interop.InteropTestBase;

import static com.fasterxml.jackson.dataformat.avro.interop.ApacheAvroInteropUtil.apacheDeserializer;
import static com.fasterxml.jackson.dataformat.avro.interop.ApacheAvroInteropUtil.getJacksonSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests the {@link AvroName @AvroName} annotation
 */
public class AvroNameTest extends InteropTestBase
{
    public static class RecordWithRenamed {
        @AvroName("newName")
        public String someField;

        @Override
        public boolean equals(Object o) {
            return ((RecordWithRenamed) o).someField.equals(someField);
        }
    }

    public static class RecordWithNameCollision {
        @AvroName("otherField")
        public String firstField;

        public String otherField;
    }

    @Before
    public void setup() {
        // 2.8 doesn't generate schemas with compatible namespaces for Apache deserializer
        Assume.assumeTrue(deserializeFunctor != apacheDeserializer || schemaFunctor != getJacksonSchema);
    }

    @Test
    public void testRecordWithRenamedField() {
        RecordWithRenamed original = new RecordWithRenamed();
        original.someField = "blah";
        RecordWithRenamed result = roundTrip(original);
        assertThat(result).isEqualTo(original);
    }

    public void testRecordWithNameCollision() {
        try {
            schemaFunctor.apply(RecordWithNameCollision.class);
            fail("Should not pass");
        } catch (IllegalArgumentException e) {
            AvroTestBase.verifyException(e, "foobar");
        }
    }
}
