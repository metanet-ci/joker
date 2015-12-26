package cs.bilkent.zanza.operator.schema.runtime;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;

public class PortRuntimeSchema
{

    private final int portIndex;

    private final List<RuntimeSchemaField> fields;

    public PortRuntimeSchema ( final int portIndex, final List<RuntimeSchemaField> fields )
    {
        this.portIndex = portIndex;
        this.fields = unmodifiableList( new ArrayList<>( fields ) );
    }

    public int getPortIndex ()
    {
        return portIndex;
    }

    public List<RuntimeSchemaField> getFields ()
    {
        return fields;
    }

    /**
     * Checks if all of the fields in the other schema has a corresponding compatible field in this schema or not
     *
     * @param other
     *         schema to check
     *
     * @return true if all of the fields in the other schema has a corresponding compatible field in this schema
     */
    public boolean isCompatibleWith ( final PortRuntimeSchema other )
    {
        for ( RuntimeSchemaField otherField : other.getFields() )
        {
            boolean match = false;
            for ( RuntimeSchemaField thisField : this.fields )
            {
                if ( thisField.isCompatibleWith( otherField ) )
                {
                    match = true;
                    break;
                }
            }

            if ( !match )
            {
                return false;
            }
        }

        return true;
    }

}
