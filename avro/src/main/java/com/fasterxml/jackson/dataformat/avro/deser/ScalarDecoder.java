package com.fasterxml.jackson.dataformat.avro.deser;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaHelper;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;

/**
 * Helper classes for reading non-structured values, and can thereby usually
 * be accessed using simpler interface (although sometimes not: if so,
 * instances are wrapped in <code>ScalarDecoderWrapper</code>s).
 */
public abstract class ScalarDecoder
{
    protected abstract JsonToken decodeValue(AvroParserImpl parser, Decoder decoder)
        throws IOException;

    protected abstract void skipValue(Decoder decoder)
        throws IOException;

    public abstract AvroFieldReader asFieldReader(String name, boolean skipper);

    public abstract String getTypeId();
    
    /*
    /**********************************************************************
    /* Decoder implementations
    /**********************************************************************
     */

    protected final static class BooleanDecoder extends ScalarDecoder
    {
        @Override
        protected JsonToken decodeValue(AvroParserImpl parser, Decoder decoder) throws IOException {
            return decoder.readBoolean() ? JsonToken.VALUE_TRUE : JsonToken.VALUE_FALSE;
        }

        @Override
        protected void skipValue(Decoder decoder) throws IOException
        {
            // for some reason, no `skipBoolean()` so:
            decoder.skipFixed(1);
        }

        @Override
        public String getTypeId() {
            return AvroSchemaHelper.getTypeId(boolean.class);
        }

        @Override
        public AvroFieldReader asFieldReader(String name, boolean skipper) {
            return new FR(name, skipper, getTypeId());
        }
        
        private final static class FR extends AvroFieldReader {
            public FR(String name, boolean skipper, String typeId) {
                super(name, skipper, typeId);
            }

            @Override
            public JsonToken readValue(AvroReadContext parent,
                    AvroParserImpl parser, BinaryDecoder decoder) throws IOException
            {
                return decoder.readBoolean() ? JsonToken.VALUE_TRUE : JsonToken.VALUE_FALSE;
            }

            @Override
            public void skipValue(BinaryDecoder decoder) throws IOException {
                // for some reason, no `skipBoolean()` so:
                decoder.skipFixed(1);
            }
        }
    }

    protected final static class BytesDecoder extends ScalarDecoder
    {
        @Override
        public JsonToken decodeValue(AvroParserImpl parser, Decoder decoder) throws IOException {
            ByteBuffer bb = parser.borrowByteBuffer();
            bb = decoder.readBytes(bb);
            return parser.setBytes(bb);
        }

        @Override
        protected void skipValue(Decoder decoder) throws IOException {
            decoder.skipBytes();
        }

        @Override
        public String getTypeId() {
            return AvroSchemaHelper.getTypeId(byte[].class);
        }

        @Override
        public AvroFieldReader asFieldReader(String name, boolean skipper) {
            return new FR(name, skipper, getTypeId());
        }
        
        private final static class FR extends AvroFieldReader {
            public FR(String name, boolean skipper, String typeId) {
                super(name, skipper, typeId);
            }

            @Override
            public JsonToken readValue(AvroReadContext parent,
                    AvroParserImpl parser, BinaryDecoder decoder) throws IOException {
                ByteBuffer bb = parser.borrowByteBuffer();
                bb = decoder.readBytes(bb);
                return parser.setBytes(bb);
            }

            @Override
            public void skipValue(BinaryDecoder decoder) throws IOException {
                decoder.skipBytes();
            }
        }
    }

    protected final static class DoubleReader extends ScalarDecoder
    {
        @Override
        public JsonToken decodeValue(AvroParserImpl parser, Decoder decoder) throws IOException {
            return parser.setNumber(decoder.readDouble());
        }

        @Override
        protected void skipValue(Decoder decoder) throws IOException {
            // doubles have fixed length of 8 bytes
            decoder.skipFixed(8);
        }

        @Override
        public String getTypeId() {
            return AvroSchemaHelper.getTypeId(double.class);
        }

        @Override
        public AvroFieldReader asFieldReader(String name, boolean skipper) {
            return new FR(name, skipper, getTypeId());
        }
        
        private final static class FR extends AvroFieldReader {
            public FR(String name, boolean skipper, String typeId) {
                super(name, skipper, typeId);
            }

            @Override
            public JsonToken readValue(AvroReadContext parent,
                    AvroParserImpl parser, BinaryDecoder decoder) throws IOException {
                return parser.setNumber(decoder.readDouble());
            }

            @Override
            public void skipValue(BinaryDecoder decoder) throws IOException {
                decoder.skipFixed(8);
            }
        }
    }
    
    protected final static class FloatReader extends ScalarDecoder {
        @Override
        public JsonToken decodeValue(AvroParserImpl parser, Decoder decoder) throws IOException {
            return parser.setNumber(decoder.readFloat());
        }

        @Override
        protected void skipValue(Decoder decoder) throws IOException {
            // floats have fixed length of 4 bytes
            decoder.skipFixed(4);
        }

        @Override
        public String getTypeId() {
            return AvroSchemaHelper.getTypeId(float.class);
        }

        @Override
        public AvroFieldReader asFieldReader(String name, boolean skipper) {
            return new FR(name, skipper, getTypeId());
        }
        
        private final static class FR extends AvroFieldReader {
            public FR(String name, boolean skipper, String typeId) {
                super(name, skipper, typeId);
            }

            @Override
            public JsonToken readValue(AvroReadContext parent,
                    AvroParserImpl parser, BinaryDecoder decoder) throws IOException {
                return parser.setNumber(decoder.readFloat());
            }

            @Override
            public void skipValue(BinaryDecoder decoder) throws IOException {
                decoder.skipFixed(4);
            }
        }
    }
    
    protected final static class IntReader extends ScalarDecoder
    {
        private final String _typeId;

        public IntReader(String typeId) {
            _typeId = typeId;
        }

        public IntReader() {
            this(AvroSchemaHelper.getTypeId(int.class));
        }

        @Override
        public JsonToken decodeValue(AvroParserImpl parser, Decoder decoder) throws IOException {
            if (Character.class.getName().equals(getTypeId())) {
                return parser.setString(Character.toString((char) decoder.readInt()));
            }
            return parser.setNumber(decoder.readInt());
        }

        @Override
        protected void skipValue(Decoder decoder) throws IOException {
            // ints use variable-length zigzagging; alas, no native skipping
            decoder.readInt();
        }

        @Override
        public String getTypeId() {
            return _typeId;
        }

        @Override
        public AvroFieldReader asFieldReader(String name, boolean skipper) {
            return new FR(name, skipper, getTypeId());
        }

        private final static class FR extends AvroFieldReader {
            public FR(String name, boolean skipper, String typeId) {
                super(name, skipper, typeId);
            }

            @Override
            public JsonToken readValue(AvroReadContext parent,
                    AvroParserImpl parser, BinaryDecoder decoder) throws IOException {
                return parser.setNumber(decoder.readInt());
            }

            @Override
            public void skipValue(BinaryDecoder decoder) throws IOException {
                decoder.readInt();
            }
        }
    }
    
    protected final static class LongReader extends ScalarDecoder
    {
        @Override
        public JsonToken decodeValue(AvroParserImpl parser, Decoder decoder) throws IOException {
            return parser.setNumber(decoder.readLong());
        }

        @Override
        protected void skipValue(Decoder decoder) throws IOException {
            // longs use variable-length zigzagging; alas, no native skipping
            decoder.readLong();
        }

        @Override
        public String getTypeId() {
            return AvroSchemaHelper.getTypeId(long.class);
        }

        @Override
        public AvroFieldReader asFieldReader(String name, boolean skipper) {
            return new FR(name, skipper, getTypeId());
        }
        
        private final static class FR extends AvroFieldReader {
            public FR(String name, boolean skipper, String typeId) {
                super(name, skipper, typeId);
            }

            @Override
            public JsonToken readValue(AvroReadContext parent,
                    AvroParserImpl parser, BinaryDecoder decoder) throws IOException {
                return parser.setNumber(decoder.readLong());
            }

            @Override
            public void skipValue(BinaryDecoder decoder) throws IOException {
                decoder.readLong();
            }
        }
    }
    
    protected final static class NullReader extends ScalarDecoder
    {
        @Override
        public JsonToken decodeValue(AvroParserImpl parser, Decoder decoder) {
            return JsonToken.VALUE_NULL;
        }

        @Override
        protected void skipValue(Decoder decoder) throws IOException {
            ; // value implied
        }

        @Override
        public String getTypeId() {
            return null;
        }

        @Override
        public AvroFieldReader asFieldReader(String name, boolean skipper) {
            return new FR(name, skipper);
        }
        
        private final static class FR extends AvroFieldReader {
            public FR(String name, boolean skipper) {
                super(name, skipper, null);
            }

            @Override
            public JsonToken readValue(AvroReadContext parent,
                    AvroParserImpl parser, BinaryDecoder decoder) throws IOException {
                return JsonToken.VALUE_NULL;
            }

            @Override
            public void skipValue(BinaryDecoder decoder) throws IOException { }
        }
    }
    
    protected final static class StringReader extends ScalarDecoder
    {
        private final String _typeId;

        public StringReader(String typeId) {
            _typeId = typeId;
        }

        public StringReader() {
            this(AvroSchemaHelper.getTypeId(String.class));
        }

        @Override
        public JsonToken decodeValue(AvroParserImpl parser, Decoder decoder) throws IOException {
            return parser.setString(decoder.readString());
        }

        @Override
        protected void skipValue(Decoder decoder) throws IOException {
            decoder.skipString();
        }

        @Override
        public String getTypeId() {
            return _typeId;
        }

        @Override
        public AvroFieldReader asFieldReader(String name, boolean skipper) {
            return new FR(name, skipper, getTypeId());
        }
        
        private final static class FR extends AvroFieldReader {
            public FR(String name, boolean skipper, String typeId) {
                super(name, skipper, typeId);
            }

            @Override
            public JsonToken readValue(AvroReadContext parent,
                    AvroParserImpl parser, BinaryDecoder decoder) throws IOException {
                return parser.setString(decoder.readString());
            }

            @Override
            public void skipValue(BinaryDecoder decoder) throws IOException {
                decoder.skipString();
            }
        }
    }

    protected final static class ScalarUnionDecoder extends ScalarDecoder
    {
        public final ScalarDecoder[] _readers;

        public ScalarUnionDecoder(ScalarDecoder[] readers) {
            _readers = readers;
        }

        @Override
        protected JsonToken decodeValue(AvroParserImpl parser, Decoder decoder) throws IOException {
            return _checkIndex(decoder.readIndex()).decodeValue(parser, decoder);
        }

        @Override
        protected void skipValue(Decoder decoder) throws IOException
        {
            _checkIndex(decoder.readIndex()).skipValue(decoder);
        }

        private ScalarDecoder _checkIndex(int index) throws IOException {
            if (index < 0 || index >= _readers.length) {
                throw new IOException(String.format(
                        "Invalid Union index (%s); union only has %d types", index, _readers.length));
            }
            return _readers[index];
        }

        @Override
        public String getTypeId() {
            return null;
        }

        @Override
        public AvroFieldReader asFieldReader(String name, boolean skipper) {
            return new FR(name, skipper, _readers);
        }
        
        private final static class FR extends AvroFieldReader {
            public final ScalarDecoder[] _readers;

            public FR(String name, boolean skipper, ScalarDecoder[] readers) {
                super(name, skipper, null);
                _readers = readers;
            }

            @Override
            public JsonToken readValue(AvroReadContext parent,
                    AvroParserImpl parser, BinaryDecoder decoder) throws IOException {
                return _checkIndex(decoder.readIndex()).decodeValue(parser, decoder);
            }

            @Override
            public void skipValue(BinaryDecoder decoder) throws IOException {
                _checkIndex(decoder.readIndex()).skipValue(decoder);
            }

            private ScalarDecoder _checkIndex(int index) throws IOException {
                if (index < 0 || index >= _readers.length) {
                    throw new IOException(String.format(
                            "Invalid Union index (%s); union only has %d types", index, _readers.length));
                }
                return _readers[index];
            }
        }
    }

    protected final static class EnumDecoder extends ScalarDecoder
    {
        protected final String _name;
        protected final String[] _values;
        
        public EnumDecoder(String name, List<String> enumNames)
        {
            _name = name;
            _values = enumNames.toArray(new String[enumNames.size()]);
        }

        @Override
        public JsonToken decodeValue(AvroParserImpl parser, Decoder decoder) throws IOException {
            return parser.setString(_checkIndex(decoder.readEnum()));
        }

        @Override
        protected void skipValue(Decoder decoder) throws IOException {
            _checkIndex(decoder.readEnum());
        }

        private final String _checkIndex(int index) throws IOException {
            if (index < 0 || index >= _values.length) {
                throw new IOException(String.format(
                        "Invalid Enum index (%s); enum '%s' only has %d types",
                        index, _name, _values.length));
            }
            return _values[index];
        }

        @Override
        public String getTypeId() {
            return _name;
        }

        @Override
        public AvroFieldReader asFieldReader(String name, boolean skipper) {
            return new FR(name, skipper, this, _name);
        }
        
        private final static class FR extends AvroFieldReader {
            protected final String[] _values;

            public FR(String name, boolean skipper, EnumDecoder base, String typeId) {
                super(name, skipper, typeId);
                _values = base._values;
            }

            @Override
            public JsonToken readValue(AvroReadContext parent,
                    AvroParserImpl parser, BinaryDecoder decoder) throws IOException {
                return parser.setString(_checkIndex(decoder.readEnum()));
            }

            @Override
            public void skipValue(BinaryDecoder decoder) throws IOException {
                _checkIndex(decoder.readEnum());
            }

            private final String _checkIndex(int index) throws IOException {
                if (index < 0 || index >= _values.length) {
                    throw new IOException(String.format(
                            "Invalid Enum index (%s); enum '%s' only has %d types",
                            index, _name, _values.length));
                }
                return _values[index];
            }
        }
    }

    protected final static class FixedDecoder
        extends ScalarDecoder
    {
        private final int _size;
        private final String _typeId;
        
        public FixedDecoder(int fixedSize, String typeId) {
            _size = fixedSize;
            _typeId = typeId;
        }
        
        @Override
        public JsonToken decodeValue(AvroParserImpl parser, Decoder decoder) throws IOException
        {
            byte[] data = new byte[_size];
            decoder.readFixed(data);
            return parser.setBytes(data);
        }

        @Override
        protected void skipValue(Decoder decoder) throws IOException {
            decoder.skipFixed(_size);
        }

        @Override
        public String getTypeId() {
            return _typeId;
        }

        @Override
        public AvroFieldReader asFieldReader(String name, boolean skipper) {
            return new FR(name, skipper, _size, _typeId);
        }
        
        private final static class FR extends AvroFieldReader {
            private final int _size;

            public FR(String name, boolean skipper, int size, String typeId) {
                super(name, skipper, typeId);
                _size = size;
            }

            @Override
            public JsonToken readValue(AvroReadContext parent,
                    AvroParserImpl parser, BinaryDecoder decoder) throws IOException
            {
                byte[] data = new byte[_size];
                decoder.readFixed(data);
                return parser.setBytes(data);
            }

            @Override
            public void skipValue(BinaryDecoder decoder) throws IOException {
                decoder.skipFixed(_size);
            }
        }
    }
}
