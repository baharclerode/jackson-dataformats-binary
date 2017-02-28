package com.fasterxml.jackson.dataformat.avro;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.PropertyName;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.introspect.Annotated;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.databind.introspect.AnnotatedConstructor;
import com.fasterxml.jackson.databind.introspect.AnnotatedMember;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.jsontype.TypeResolverBuilder;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaHelper;

import org.apache.avro.reflect.*;

/**
 * Adds support for the following annotations from the Apache Avro implementation:
 * <ul>
 * <li>{@link AvroIgnore @AvroIgnore} - Alias for <code>JsonIgnore</code></li>
 * <li>{@link AvroName @AvroName("custom Name")} - Alias for <code>JsonProperty("custom name")</code></li>
 * <li>{@link AvroDefault @AvroDefault("1234")} - Alias for <code>JsonProperty(defaultValue = "1234")</code></li>
 * <li>{@link Stringable @Stringable} - Alias for <code>JsonCreator</code> on the constructor and <code>JsonValue</code> on
 * the {@link #toString()} method. </li>
 * <li>{@link Union @Union} - Alias for <code>JsonSubTypes</code></li>
 * </ul>
 */
public class AvroAnnotationIntrospector extends AnnotationIntrospector {

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

    protected PropertyName _findName(Annotated a) {
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

    @Override
    public boolean hasCreatorAnnotation(Annotated a) {
        return a instanceof AnnotatedConstructor
               && ((AnnotatedConstructor) a).getTypeContext() instanceof AnnotatedClass
               && (
                      (AnnotatedConstructor) a
                  ).getParameterCount()
                  == 1
               && String.class.equals(((AnnotatedConstructor) a).getRawParameterType(0));
    }

    @Override
    public Object findSerializer(Annotated a) {
        if (a instanceof AnnotatedClass && AvroSchemaHelper.isStringable((AnnotatedClass) a)) {
            return ToStringSerializer.class;
        }
        return null;
    }

    @Override
    public List<NamedType> findSubtypes(Annotated a) {
        Union union = _findAnnotation(a, Union.class);
        if (union == null) {
            return null;
        }
        ArrayList<NamedType> names = new ArrayList<>(union.value().length);
        for (Class<?> subtype : union.value()) {
            names.add(new NamedType(subtype, AvroSchemaHelper.getTypeId(TypeFactory.defaultInstance().constructType(subtype))));
        }
        return names;
    }

    @Override
    public TypeResolverBuilder<?> findTypeResolver(MapperConfig<?> config, AnnotatedClass ac, JavaType baseType) {
        return _findTypeResolver(config, ac, baseType);
    }

    @Override
    public TypeResolverBuilder<?> findPropertyTypeResolver(MapperConfig<?> config, AnnotatedMember am, JavaType baseType) {
        return _findTypeResolver(config, am, baseType);
    }

    @Override
    public TypeResolverBuilder<?> findPropertyContentTypeResolver(MapperConfig<?> config, AnnotatedMember am, JavaType containerType) {
        return _findTypeResolver(config, am, containerType);
    }

    protected TypeResolverBuilder<?> _findTypeResolver(MapperConfig<?> config, Annotated ann, JavaType baseType) {
        TypeResolverBuilder<?> resolver = new AvroTypeResolverBuilder();
        // TODO defaultImpl
        return resolver;
    }
}
