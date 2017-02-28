package com.fasterxml.jackson.dataformat.avro.interop.arrays;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.fasterxml.jackson.dataformat.avro.interop.InteropTestBase;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests lists involving complex element types (Lists, Records, Maps, Enums)
 */
public class ListWithComplexTest extends InteropTestBase {

    @Test
    public void testListWithRecordElements() {
        List<DummyRecord> original = new ArrayList<>();
        original.add(new DummyRecord("test", 2));
        original.add(new DummyRecord("test 2", 1235));
        original.add(new DummyRecord("test 3", -234));
        //
        List<DummyRecord> result = roundTrip(type(List.class, DummyRecord.class), original);
        //
        assertThat(result).isEqualTo(original);
    }

    @Test
    public void testEmptyListWithRecordElements() {
        List<DummyRecord> original = new ArrayList<>();
        //
        List<DummyRecord> result = roundTrip(type(List.class, DummyRecord.class), original);
        //
        assertThat(result).isEqualTo(original);
    }

    @Test(expected = Exception.class)
    public void testListWithNullElements() {
        List<DummyRecord> original = new ArrayList<>();
        original.add(null);
        //
        List<DummyRecord> result = roundTrip(type(List.class, DummyRecord.class), original);
        //
        assertThat(result).isEqualTo(original);
    }

    @Test
    public void testListWithEnumElements() {
        List<DummyEnum> original = new ArrayList<>();
        original.add(DummyEnum.EAST);
        original.add(DummyEnum.WEST);
        //
        List<DummyEnum> result = roundTrip(type(List.class, DummyEnum.class), original);
        //
        assertThat(result).isEqualTo(original);
    }

    @Test
    public void testListWithListElements() {
        List<List<List<String>>> original = new ArrayList<>();
        original.add(new ArrayList<List<String>>());
        original.get(0).add(Collections.singletonList("Hello"));
        original.add(new ArrayList<List<String>>());
        original.get(1).add(Collections.singletonList("World"));
        //
        List<List<List<String>>> result = roundTrip(type(List.class, type(List.class, type(List.class, String.class))), original);
        //
        assertThat(result).isEqualTo(original);
    }

    @Test
    public void testListWithMapElements() {
        List<Map<String, Integer>> original = new ArrayList<>();
        original.add(Collections.singletonMap("Hello", 1));
        original.add(Collections.singletonMap("World", 2));
        //
        List<Map<String, Integer>> result = roundTrip(type(List.class, type(Map.class, String.class, Integer.class)), original);
        //
        assertThat(result).isEqualTo(original);
    }

}
