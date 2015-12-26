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

}
