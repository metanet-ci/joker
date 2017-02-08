package cs.bilkent.joker.operator.schema.runtime;


import java.util.ArrayList;
import java.util.List;

import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkArgument;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.operator.schema.annotation.PortSchemaScope;
import static cs.bilkent.joker.operator.schema.annotation.PortSchemaScope.EXTENDABLE_FIELD_SET;
import static java.util.Collections.unmodifiableList;

/**
 * Builder for {@link PortRuntimeSchema}
 */
public final class PortRuntimeSchemaBuilder
{

    private final PortSchemaScope scope;

    private final List<RuntimeSchemaField> fields;

    PortRuntimeSchemaBuilder ()
    {
        this.scope = EXTENDABLE_FIELD_SET;
        this.fields = new ArrayList<>();
    }

    PortRuntimeSchemaBuilder ( final PortSchemaScope scope, final List<RuntimeSchemaField> fields )
    {
        checkArgument( scope != null, "scope can't be null" );
        checkArgument( fields != null, "fields can't be null" );
        this.scope = scope;
        this.fields = new ArrayList<>();
        this.fields.addAll( fields );
    }

    /**
     * Adds the given field to the port schema
     *
     * @param fieldName
     *         name of the field
     * @param type
     *         type of the field
     *
     * @return the current port runtime schema builder object
     */
    public PortRuntimeSchemaBuilder addField ( final String fieldName, final Class<?> type )
    {
        checkArgument( fieldName != null, "field name must be provided" );
        checkArgument( type != null, "field type must be provided" );
        addField( new RuntimeSchemaField( fieldName, type ) );

        return this;
    }

    /**
     * Adds the given field to the port schema
     *
     * @param field
     *         to be added
     *
     * @return the current port runtime schema builder object
     */
    public PortRuntimeSchemaBuilder addField ( final RuntimeSchemaField field )
    {
        checkArgument( field != null, "field must be provided" );
        checkState( scope == EXTENDABLE_FIELD_SET, "port getSchema with " + EXTENDABLE_FIELD_SET + " can't be modified" );
        checkState( fields.stream().noneMatch( f -> f.name.equals( field.name ) ), field.name + " already exists in port getSchema" );
        fields.add( field );

        return this;
    }

    /**
     * Returns the fields added to this builder
     *
     * @return the fields added to this builder
     */
    public List<RuntimeSchemaField> getFields ()
    {
        return unmodifiableList( fields );
    }

    /**
     * Builds the {@link PortRuntimeSchema} object using the fields added to this builder
     *
     * @return the {@link PortRuntimeSchema} object using the fields added to this builder
     */
    public PortRuntimeSchema build ()
    {
        return new PortRuntimeSchema( fields );
    }

}
