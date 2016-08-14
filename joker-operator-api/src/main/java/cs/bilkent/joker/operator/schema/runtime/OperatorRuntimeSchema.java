package cs.bilkent.joker.operator.schema.runtime;


import java.util.ArrayList;
import java.util.List;

import static cs.bilkent.joker.com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.operator.schema.annotation.OperatorSchema;
import static java.util.Collections.unmodifiableList;


/**
 * Runtime representation of {@link OperatorSchema}
 */
public final class OperatorRuntimeSchema
{

    private final List<PortRuntimeSchema> inputSchemas;

    private final List<PortRuntimeSchema> outputSchemas;

    public OperatorRuntimeSchema ( final List<PortRuntimeSchema> inputSchemas, final List<PortRuntimeSchema> outputSchemas )
    {
        checkArgument( inputSchemas != null, "null input port runtime schemas!" );
        checkArgument( outputSchemas != null, "null output port runtime schemas!" );
        this.inputSchemas = unmodifiableList( new ArrayList<>( inputSchemas ) );
        this.outputSchemas = unmodifiableList( new ArrayList<>( outputSchemas ) );
    }

    public int getInputPortCount ()
    {
        return inputSchemas.size();
    }

    public int getOutputPortCount ()
    {
        return outputSchemas.size();
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

    @Override
    public String toString ()
    {
        return "OperatorRuntimeSchema{" + "inputSchemas=" + inputSchemas + ", outputSchemas=" + outputSchemas + '}';
    }

}
