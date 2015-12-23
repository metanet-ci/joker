package cs.bilkent.zanza.operator.schema.runtime;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.unmodifiableList;

public class OperatorRuntimeSchema
{

    private final List<PortRuntimeSchema> inputSchemas;

    private final List<PortRuntimeSchema> outputSchemas;

    public OperatorRuntimeSchema ( final List<PortRuntimeSchema> inputSchemas, final List<PortRuntimeSchema> outputSchemas )
    {
        checkArgument( inputSchemas != null );
        checkArgument( outputSchemas != null );
        this.inputSchemas = unmodifiableList( new ArrayList<>( inputSchemas ) );
        this.outputSchemas = unmodifiableList( new ArrayList<>( outputSchemas ) );
    }

    public PortRuntimeSchema getInputSchema ( final int portIndex )
    {
        return inputSchemas.get( portIndex );
    }

    public PortRuntimeSchema getOutputSchema ( final int portIndex )
    {
        return outputSchemas.get( portIndex );
    }

    public List<PortRuntimeSchema> getInputSchemas ()
    {
        return inputSchemas;
    }

    public List<PortRuntimeSchema> getOutputSchemas ()
    {
        return outputSchemas;
    }

}
