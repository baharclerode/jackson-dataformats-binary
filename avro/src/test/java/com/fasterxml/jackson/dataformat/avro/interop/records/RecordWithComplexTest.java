package com.fasterxml.jackson.dataformat.avro.interop.records;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.reflect.Nullable;
import org.junit.Test;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.dataformat.avro.interop.InteropTestBase;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests records involving complex value types (Lists, Records, Maps, Enums)
 */
public class RecordWithComplexTest extends InteropTestBase {

    @Data
    @ToString(callSuper = true)
    @EqualsAndHashCode(callSuper = true)
    @RequiredArgsConstructor
    public static class RecursiveDummyRecord extends DummyRecord {

        @Nullable
        private DummyRecord next;

        private Map<String, Integer> simpleMap = new HashMap<>();

        private Map<String, RecursiveDummyRecord> recursiveMap = new HashMap<>();

        private List<Integer> requiredList = new ArrayList<>();

        @JsonProperty(required = true)
        private DummyEnum requiredEnum = DummyEnum.EAST;

        @Nullable
        private DummyEnum optionalEnum = null;

        public RecursiveDummyRecord(String firstValue, Integer secondValue, DummyRecord next) {
            super(firstValue, secondValue);
            this.next = next;
        }
    }

    @Test
    public void testRecordWithRecordValues() {
        RecursiveDummyRecord original = new RecursiveDummyRecord("Hello", 12353, new DummyRecord("World", 234));
        //
        RecursiveDummyRecord result = roundTrip(RecursiveDummyRecord.class, original);
        //
        assertThat(result).isEqualTo(original);
    }

    @Test
    public void testEmptyRecordWithRecordValues() {
        Map<String, DummyRecord> original = new HashMap<>();
        //
        Map<String, DummyRecord> result = roundTrip(type(Map.class, String.class, DummyRecord.class), original);
        //
        assertThat(result).isEqualTo(original);
    }

    @Test(expected = Exception.class)
    public void testRecordWithNullRequiredFields() {
        RecursiveDummyRecord original = new RecursiveDummyRecord(null, 12353, new DummyRecord("World", 234));
        //
        RecursiveDummyRecord result = roundTrip(RecursiveDummyRecord.class, original);
    }

    @Test(expected = Exception.class)
    public void testRecordWithMissingRequiredEnumFields() {
        RecursiveDummyRecord original = new RecursiveDummyRecord("Hello", 12353, new DummyRecord("World", 234));
        original.setRequiredEnum(null);
        //
        RecursiveDummyRecord result = roundTrip(RecursiveDummyRecord.class, original);
    }

    @Test
    public void testRecordWithOptionalEnumField() {
        RecursiveDummyRecord original = new RecursiveDummyRecord("Hello", 12353, new DummyRecord("World", 234));
        original.setOptionalEnum(DummyEnum.SOUTH);
        //
        RecursiveDummyRecord result = roundTrip(RecursiveDummyRecord.class, original);
        //
        assertThat(result).isEqualTo(original);
    }

    @Test
    public void testRecordWithListFields() {
        RecursiveDummyRecord original = new RecursiveDummyRecord("Hello", 12353, new DummyRecord("World", 234));
        original.getRequiredList().add(9682584);
        //
        RecursiveDummyRecord result = roundTrip(RecursiveDummyRecord.class, original);
        //
        assertThat(result).isEqualTo(original);
        assertThat(result.getRequiredList()).isEqualTo(original.getRequiredList());
    }

    @Test
    public void testRecordWithMapFields() {
        RecursiveDummyRecord original = new RecursiveDummyRecord("Hello", 12353, new DummyRecord("World", 234));
        original.getSimpleMap().put("Hello World", 9682584);
        //
        RecursiveDummyRecord result = roundTrip(RecursiveDummyRecord.class, original);
        //
        assertThat(result).isEqualTo(original);
        assertThat(result.getSimpleMap().get("Hello World")).isEqualTo(original.getSimpleMap().get("Hello World"));
    }

}
