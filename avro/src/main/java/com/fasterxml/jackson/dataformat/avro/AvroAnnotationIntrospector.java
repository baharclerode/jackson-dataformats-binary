package com.fasterxml.jackson.dataformat.avro;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.PropertyName;
import com.fasterxml.jackson.databind.introspect.Annotated;
import com.fasterxml.jackson.databind.introspect.AnnotatedMember;

import org.apache.avro.reflect.AvroDefault;
import org.apache.avro.reflect.AvroIgnore;
import org.apache.avro.reflect.AvroName;

/**
 * Adds support for the following annotations from the Apache Avro implementation:
 * <ul>
 * <li>{@link AvroIgnore @AvroIgnore} - Alias for <code>JsonIgnore</code></li>
 * <li>{@link AvroName @AvroName("custom Name")} - Alias for <code>JsonProperty("custom name")</code></li>
 * <li>{@link AvroDefault @AvroDefault("1234")} - Alias for <code>JsonProperty(defaultValue = "1234")</code></li>
 * </ul>
 */
public class AvroAnnotationIntrospector extends AnnotationIntrospector
{
    private static final long serialVersionUID = 1L;

    @Override
    public Version version() {
        return PackageVersion.VERSION;
    }

    @Override
    public boolean hasIgnoreMarker(AnnotatedMember m) {
        return _findAnnotation(m, AvroIgnore.class) != null;
    }

    @Override
    public PropertyName findNameForSerialization(Annotated a) {
        return _findName(a);
    }

    @Override
    public PropertyName findNameForDeserialization(Annotated a) {
        return _findName(a);
    }

    @Override
    public String findPropertyDefaultValue(Annotated m) {
        AvroDefault ann = _findAnnotation(m, AvroDefault.class);
        return (ann == null) ? null : ann.value();
    }

    protected PropertyName _findName(Annotated a)
    {
        AvroName ann = _findAnnotation(a, AvroName.class);
        return (ann == null) ? null : PropertyName.construct(ann.value());
    }
	
	@Override
    public Boolean hasRequiredMarker(AnnotatedMember m) {
        if (_hasAnnotation(m, Nullable.class)) {
            return false;
        }
        // Appears to be a bug in POJOPropertyBuilder.getMetadata()
        // Can't specify a default unless property is known to be required or not
        // If we have a default but no annotations indicating required or not, assume true.
        //if (_hasAnnotation(m, AvroDefault.class) && !_hasAnnotation(m, JsonProperty.class)) {
        //    return true;
        //}
        return null;
    }
}
