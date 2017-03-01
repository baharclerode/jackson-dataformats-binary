package com.fasterxml.jackson.dataformat.avro.deser;

import java.io.IOException;

import org.apache.avro.io.BinaryDecoder;

import com.fasterxml.jackson.core.JsonToken;

/**
 * Entity that encapsulates details of accessing value of a single field
 * of a "Record" (Avro term, corresponding roughly to JSON Object).
 */
public abstract class AvroFieldReader
{
    protected final String _name;
    protected final boolean _isSkipper;
    protected final String _typeId;

    protected AvroFieldReader(String name, boolean isSkipper, String typeId) {
        _name = name;
        _isSkipper = isSkipper;
        _typeId = typeId;
    }

    public static AvroFieldReader construct(String name, AvroStructureReader structureReader) {
        return new Structured(name, false, structureReader);
    }

    public static AvroFieldReader constructSkipper(String name, AvroStructureReader structureReader) {
        return new Structured(name, true, structureReader);
    }

    public final String getName() { return _name; }
    public final boolean isSkipper() { return _isSkipper; }

    public abstract JsonToken readValue(AvroReadContext parent,
            AvroParserImpl parser, BinaryDecoder avroDecoder) throws IOException;

    public abstract void skipValue(BinaryDecoder decoder) throws IOException;

    public String getTypeId() {
        return _typeId;
    }

    /**
     * Implementation used for non-scalar-valued (structured) fields
     */
    private final static class Structured extends AvroFieldReader {
        protected final AvroStructureReader _reader;

        public Structured(String name, boolean skipper, AvroStructureReader r) {
            super(name, skipper, null);
            _reader = r;
        }

        @Override
        public JsonToken readValue(AvroReadContext parent,
                AvroParserImpl parser, BinaryDecoder decoder)
            throws IOException
        {
            return _reader.newReader(parent, parser, decoder).nextToken();
        }

        @Override
        public void skipValue(BinaryDecoder decoder) throws IOException {
            _reader.skipValue(decoder);
        }

        @Override
        public String getTypeId() {
            return _reader.getTypeId();
        }
    }
}
