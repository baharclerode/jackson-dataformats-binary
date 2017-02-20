package com.fasterxml.jackson.dataformat.avro.interop.annotations;

import org.apache.avro.Schema;
import org.apache.avro.reflect.AvroSchema;
import org.apache.avro.reflect.Nullable;
import org.junit.Test;

import com.fasterxml.jackson.dataformat.avro.interop.InteropTestBase;

import lombok.Data;

import static org.assertj.core.api.Assertions.assertThat;

public class AvroSchemaTest extends InteropTestBase {

    @Data
    @AvroSchema("{\"type\":\"string\"}")
    public static class OverriddenClassSchema {

        private int field;
    }

    @Data
    public static class OverriddenFieldSchema {

        @AvroSchema("{\"type\":\"int\"}")
        private String                myField;

        @Nullable
        private OverriddenClassSchema recursiveOverride;

        @Nullable
        @AvroSchema("{\"type\":\"long\"}")
        private OverriddenClassSchema precedenceField;
    }

    @Test
    public void testTypeOverride() {
        Schema schema = schemaFunctor.apply(OverriddenClassSchema.class);
        //
        assertThat(schema.getType()).isEqualTo(Schema.Type.STRING);
    }

    @Test
    public void testFieldOverride() {
        Schema schema = schemaFunctor.apply(OverriddenFieldSchema.class);
        //
        assertThat(schema.getType()).isEqualTo(Schema.Type.RECORD);
        assertThat(schema.getField("myField").schema().getType()).isEqualTo(Schema.Type.INT);
    }

    @Test
    public void testRecursiveFieldOverride() {
        Schema schema = schemaFunctor.apply(OverriddenFieldSchema.class);
        //
        assertThat(schema.getType()).isEqualTo(Schema.Type.RECORD);
        assertThat(schema.getField("recursiveOverride").schema().getType()).isEqualTo(Schema.Type.UNION);
        assertThat(schema.getField("recursiveOverride").schema().getTypes().get(0).getType()).isEqualTo(Schema.Type.NULL);
        assertThat(schema.getField("recursiveOverride").schema().getTypes().get(1).getType()).isEqualTo(Schema.Type.STRING);
    }

    @Test
    public void testOverridePrecedence() {
        Schema schema = schemaFunctor.apply(OverriddenFieldSchema.class);
        //
        assertThat(schema.getType()).isEqualTo(Schema.Type.RECORD);
        assertThat(schema.getField("precedenceField").schema().getType()).isEqualTo(Schema.Type.LONG);
    }

}
