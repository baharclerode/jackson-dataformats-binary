package com.fasterxml.jackson.dataformat.avro.deser;

import java.io.IOException;

import org.apache.avro.io.BinaryDecoder;

import com.fasterxml.jackson.core.JsonToken;

abstract class RecordReader extends AvroStructureReader
{
    protected final static int STATE_START = 0;
    protected final static int STATE_NAME = 1;
    protected final static int STATE_VALUE = 2;
    protected final static int STATE_END = 3;
    protected final static int STATE_DONE = 4;

    protected final AvroFieldReader[] _fieldReaders;
    protected final BinaryDecoder _decoder;
    protected final AvroParserImpl _parser;

    protected String _currentName;

    protected int _state;
    protected final int _count;

    protected RecordReader(AvroReadContext parent,
            AvroFieldReader[] fieldReaders,
            BinaryDecoder decoder, AvroParserImpl parser, String typeId)
    {
        super(parent, TYPE_OBJECT, typeId);
        _fieldReaders = fieldReaders;
        _decoder = decoder;
        _parser = parser;
        _count = fieldReaders.length;
    }

    @Override
    public abstract RecordReader newReader(AvroReadContext parent,
            AvroParserImpl parser, BinaryDecoder decoder);

    @Override
    public String getCurrentName() { return _currentName; }

    @Override
    public final void skipValue(BinaryDecoder decoder) throws IOException {
        for (int i = 0, end = _fieldReaders.length; i < end; ++i) {
            _fieldReaders[i].skipValue(decoder);
        }
    }

    protected final JsonToken _nextAtEndObject() throws IOException
    {
        AvroReadContext parent = getParent();
        // as per [dataformats-binary#38], may need to reset, instead of bailing out
        if (parent.inRoot()) {
            if (!DecodeUtil.isEnd(_decoder)) {
                _state = STATE_START;
                _index = 0;
                return (_currToken = JsonToken.END_OBJECT);
            }
        }
        _state = STATE_DONE;
        _parser.setAvroContext(getParent());
        return (_currToken = JsonToken.END_OBJECT);
    }

    @Override
    public void appendDesc(StringBuilder sb)
    {
        sb.append('{');
        if (_currentName != null) {
            sb.append('"');
            sb.append(_currentName);
            sb.append('"');
        } else {
            sb.append('?');
        }
        sb.append('}');
    }

    @Override
    public String getTypeId() {
        if (_currToken == JsonToken.END_OBJECT || _currToken == JsonToken.START_OBJECT) {
            return super.getTypeId();
        }
        if (_currToken == JsonToken.FIELD_NAME) {
            return _fieldReaders[_index].getTypeId();
        }
        return _fieldReaders[_index-1].getTypeId();
    }

    /*
    /**********************************************************************
    /* Implementations
    /**********************************************************************
     */

    public final static class Std
        extends RecordReader
    {
        public Std(AvroFieldReader[] fieldReaders, String typeId) {
            super(null, fieldReaders, null, null, typeId);
        }

        public Std(AvroReadContext parent,
                AvroFieldReader[] fieldReaders,
                BinaryDecoder decoder, AvroParserImpl parser, String typeId) {
            super(parent, fieldReaders, decoder, parser, typeId);
        }
        
        @Override
        public RecordReader newReader(AvroReadContext parent,
                AvroParserImpl parser, BinaryDecoder decoder) {
            return new Std(parent, _fieldReaders, decoder, parser, _typeId);
        }

        @Override
        public JsonToken nextToken() throws IOException
        {
            switch (_state) {
            case STATE_START:
                _parser.setAvroContext(this);
                _state = (_count > 0) ? STATE_NAME : STATE_END;
                {
                    JsonToken t = JsonToken.START_OBJECT;
                    _currToken = t;
                    return t;
                }
            case STATE_NAME:
                if (_index < _count) {
                    _currentName = _fieldReaders[_index].getName();
                    _state = STATE_VALUE;
                    {
                        JsonToken t = JsonToken.FIELD_NAME;
                        _currToken = t;
                        return t;
                    }
                }
                return _nextAtEndObject();
            case STATE_VALUE:
                break;
            case STATE_END:
                return _nextAtEndObject();
            case STATE_DONE:
            default:
                throwIllegalState(_state);
            }
            _state = STATE_NAME;
            AvroFieldReader field = _fieldReaders[_index];
            ++_index;
            JsonToken t = field.readValue(this, _parser, _decoder);
            _currToken = t;
            return t;
        }

        @Override
        public String nextFieldName() throws IOException
        {
            if (_state == STATE_NAME) {
                if (_index < _count) {
                    String name = _fieldReaders[_index].getName();
                    _currentName = name;
                    _state = STATE_VALUE;
                    _currToken = JsonToken.FIELD_NAME;
                    return name;
                }
                _nextAtEndObject();
            } else {
                nextToken();
            }
            return null;
        }
    }

    public final static class Resolving
        extends RecordReader
    {
        public Resolving(AvroFieldReader[] fieldReaders, String typeId) {
            super(null, fieldReaders, null, null, typeId);
        }
        public Resolving(AvroReadContext parent,
                AvroFieldReader[] fieldReaders,
                BinaryDecoder decoder, AvroParserImpl parser, String typeId) {
            super(parent, fieldReaders, decoder, parser, typeId);
        }

        @Override
        public RecordReader newReader(AvroReadContext parent,
                AvroParserImpl parser, BinaryDecoder decoder) {
            return new Resolving(parent, _fieldReaders, decoder, parser, _typeId);
        }

        @Override
        public JsonToken nextToken() throws IOException
        {
            switch (_state) {
            case STATE_START:
                _parser.setAvroContext(this);
                _state = (_count > 0) ? STATE_NAME : STATE_END;
                {
                    JsonToken t = JsonToken.START_OBJECT;
                    _currToken = t;
                    return t;
                }
            case STATE_NAME:
                while (_index < _count) {
                    AvroFieldReader r = _fieldReaders[_index];
                    if (r.isSkipper()) {
                        ++_index;
                        r.skipValue(_decoder);
                        continue;
                    }
                    _currentName = r.getName();
                    _state = STATE_VALUE;
                    return (_currToken = JsonToken.FIELD_NAME);
                }
                return _nextAtEndObject();
            case STATE_VALUE:
                break;
            case STATE_END:
                return _nextAtEndObject();
            case STATE_DONE:
            default:
                throwIllegalState(_state);
            }
            _state = STATE_NAME;
            AvroFieldReader field = _fieldReaders[_index];
            ++_index;
            JsonToken t = field.readValue(this, _parser, _decoder);
            _currToken = t;
            return t;
        }

        @Override
        public String nextFieldName() throws IOException
        {
            if (_state == STATE_NAME) {
                while (_index < _count) {
                    AvroFieldReader r = _fieldReaders[_index];
                    if (r.isSkipper()) {
                        ++_index;
                        r.skipValue(_decoder);
                        continue;
                    }
                    String name = r.getName();
                    _currentName = name;
                    _state = STATE_VALUE;
                    _currToken = JsonToken.FIELD_NAME;
                    return name;
                }
                _nextAtEndObject();
            } else {
                nextToken();
            }
            return null;
        }
    }
}
