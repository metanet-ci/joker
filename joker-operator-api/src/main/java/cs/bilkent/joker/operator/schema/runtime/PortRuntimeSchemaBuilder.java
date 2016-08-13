package cs.bilkent.joker.operator.schema.runtime;


import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.operator.schema.annotation.PortSchemaScope;
import static cs.bilkent.joker.operator.schema.annotation.PortSchemaScope.EXTENDABLE_FIELD_SET;


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

    public PortRuntimeSchemaBuilder addField ( final String fieldName, final Class<?> type )
    {
        checkArgument( fieldName != null, "field name must be provided" );
        checkArgument( type != null, "field type must be provided" );
        addField( new RuntimeSchemaField( fieldName, type ) );

        return this;
    }

    public PortRuntimeSchemaBuilder addField ( final RuntimeSchemaField field )
    {
        checkArgument( field != null, "field must be provided" );
        checkState( scope == EXTENDABLE_FIELD_SET, "port schema with " + EXTENDABLE_FIELD_SET + " can't be modified" );
        checkState( fields.stream().noneMatch( f -> f.name.equals( field.name ) ), field.name + " already exists in port schema" );
        fields.add( field );

        return this;
    }

    public List<RuntimeSchemaField> getFields ()
    {
        return fields;
    }

    public PortRuntimeSchema build ()
    {
        return new PortRuntimeSchema( fields );
    }

}
