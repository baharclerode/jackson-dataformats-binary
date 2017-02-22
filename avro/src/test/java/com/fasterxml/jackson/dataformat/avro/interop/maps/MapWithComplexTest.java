package com.fasterxml.jackson.dataformat.avro.interop.maps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.fasterxml.jackson.dataformat.avro.interop.InteropTestBase;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests Maps involving complex value types (Lists, Records, Maps, Enums)
 */
public class MapWithComplexTest extends InteropTestBase {

    @Test
    public void testMapWithRecordValues() {
        Map<String, DummyRecord> original = new HashMap<>();
        original.put("one", new DummyRecord("test", 2));
        original.put("two", new DummyRecord("test 2", 1235));
        original.put("three", new DummyRecord("test 3", -234));
        //
        Map<String, DummyRecord> result = roundTrip(type(Map.class, String.class, DummyRecord.class), original);
        //
        assertThat(result).isEqualTo(original);
    }

    @Test
    public void testEmptyMapWithRecordValues() {
        Map<String, DummyRecord> original = new HashMap<>();
        //
        Map<String, DummyRecord> result = roundTrip(type(Map.class, String.class, DummyRecord.class), original);
        //
        assertThat(result).isEqualTo(original);
    }

    @Test(expected = Exception.class)
    public void testMapWithNullValues() {
        Map<String, DummyRecord> original = new HashMap<>();
        original.put("test", null);
        //
        Map<String, DummyRecord> result = roundTrip(type(Map.class, String.class, DummyRecord.class), original);
        //
        assertThat(result).isEqualTo(original);
    }

    @Test
    public void testMapWithEnumValues() {
        Map<String, DummyEnum> original = new HashMap<>();
        original.put("one", DummyEnum.EAST);
        original.put("two", DummyEnum.WEST);
        //
        Map<String, DummyEnum> result = roundTrip(type(Map.class, String.class, DummyEnum.class), original);
        //
        assertThat(result).isEqualTo(original);
    }

    @Test
    public void testMapWithListValues() {
        Map<String, List<List<String>>> original = new HashMap<>();
        original.put("one", new ArrayList<List<String>>());
        original.get("one").add(Collections.singletonList("Hello"));
        original.put("two", new ArrayList<List<String>>());
        original.get("two").add(Collections.singletonList("World"));
        //
        Map<String, List<List<String>>> result =
            roundTrip(type(Map.class, String.class, type(List.class, type(List.class, String.class))), original);
        //
        assertThat(result).isEqualTo(original);
    }

    @Test
    public void testMapWithMapValues() {
        Map<String, Map<String, Integer>> original = new HashMap<>();
        original.put("one", Collections.singletonMap("Hello", 1));
        original.put("two", Collections.singletonMap("World", 2));
        //
        Map<String, Map<String, Integer>> result =
            roundTrip(type(Map.class, String.class, type(Map.class, String.class, Integer.class)), original);
        //
        assertThat(result).isEqualTo(original);
    }

}
