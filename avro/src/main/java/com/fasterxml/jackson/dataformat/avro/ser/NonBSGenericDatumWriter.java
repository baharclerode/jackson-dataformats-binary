package com.fasterxml.jackson.dataformat.avro.ser;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;

/**
 * Need to sub-class to prevent encoder from crapping on writing an optional
 * Enum value (see [dataformat-avro#12])
 * 
 * @since 2.5
 */
public class NonBSGenericDatumWriter<D>
	extends GenericDatumWriter<D>
{
	public NonBSGenericDatumWriter(Schema root) {
		super(root);
	}

	@Override
	public int resolveUnion(Schema union, Object datum) {
		return AvroWriteContext.resolveUnionIndex(union, datum);
	}

	@Override
	protected void write(Schema schema, Object datum, Encoder out) throws IOException {
	    if ((schema.getType() == Type.DOUBLE) && datum instanceof BigDecimal) {
	        out.writeDouble(((BigDecimal)datum).doubleValue());
		} else if (datum instanceof String && schema.getType() == Type.ARRAY && schema.getElementType().getType() == Type.INT) {
			ArrayList<Integer> chars = new ArrayList<>(((String) datum).length());
			char[]             src   = ((String) datum).toCharArray();
			for (int i = 0; i < src.length; i++) {
				chars.add((int) src[i]);
			}
			super.write(schema, chars, out);
		} else if (datum instanceof String && ((String) datum).length() == 1 && schema.getType() == Type.INT) {
			super.write(schema, (int) ((String) datum).charAt(0), out);
		} else {
	        super.write(schema, datum, out);
	    }
	}
}
