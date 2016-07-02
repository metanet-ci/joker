package cs.bilkent.zanza.flow;


import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.zanza.operator.schema.annotation.PortSchemaScope;
import static cs.bilkent.zanza.operator.schema.annotation.PortSchemaScope.EXTENDABLE_FIELD_SET;
import cs.bilkent.zanza.operator.schema.runtime.PortRuntimeSchema;
import cs.bilkent.zanza.operator.schema.runtime.RuntimeSchemaField;


public class PortRuntimeSchemaBuilder
{

    private final int portIndex;

    private final PortSchemaScope scope;

    private final List<RuntimeSchemaField> fields;

    PortRuntimeSchemaBuilder ( final int portIndex )
    {
        checkArgument( portIndex >= 0, "port index must be non negative" );
        this.portIndex = portIndex;
        this.scope = EXTENDABLE_FIELD_SET;
        this.fields = new ArrayList<>();
    }

    PortRuntimeSchemaBuilder ( final int portIndex, final PortSchemaScope scope, final List<RuntimeSchemaField> fields )
    {
        checkArgument( portIndex >= 0, "port index must be non negative" );
        checkArgument( scope != null, "scope can't be null" );
        checkArgument( fields != null, "fields can't be null" );
        this.portIndex = portIndex;
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
        checkState( scope == EXTENDABLE_FIELD_SET,
                    "port schema of port index: " + portIndex + "  with " + EXTENDABLE_FIELD_SET + " can't be modified" );
        checkState( fields.stream().noneMatch( f -> f.name.equals( field.name ) ),
                    field.name + " already exists in port schema of port index: " + portIndex );
        fields.add( field );

        return this;
    }

    public int getPortIndex ()
    {
        return portIndex;
    }

    public List<RuntimeSchemaField> getFields ()
    {
        return fields;
    }

    public PortRuntimeSchema build ()
    {
        return new PortRuntimeSchema( portIndex, fields );
    }

}
