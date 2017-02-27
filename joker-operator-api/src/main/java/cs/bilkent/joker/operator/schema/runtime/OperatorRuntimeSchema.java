package cs.bilkent.joker.operator.schema.runtime;


import java.util.ArrayList;
import java.util.List;

import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.operator.schema.annotation.OperatorSchema;
import static java.util.Collections.unmodifiableList;


/**
 * Runtime representation of {@link OperatorSchema}
 */
public class OperatorRuntimeSchema
{

    private final List<PortRuntimeSchema> inputSchemas;

    private final List<PortRuntimeSchema> outputSchemas;

    OperatorRuntimeSchema ( final List<PortRuntimeSchema> inputSchemas, final List<PortRuntimeSchema> outputSchemas )
    {
        checkArgument( inputSchemas != null, "null input port runtime schemas!" );
        checkArgument( outputSchemas != null, "null output port runtime schemas!" );
        this.inputSchemas = unmodifiableList( new ArrayList<>( inputSchemas ) );
        this.outputSchemas = unmodifiableList( new ArrayList<>( outputSchemas ) );
    }

    /**
     * Returns input port count of the operator
     *
     * @return input port count of the operator
     */
    public int getInputPortCount ()
    {
        return inputSchemas.size();
    }

    /**
     * Returns output port count of the operator
     *
     * @return output port count of the operator
     */
    public int getOutputPortCount ()
    {
        return outputSchemas.size();
    }

    /**
     * Returns the port schema for the given input port
     *
     * @param portIndex
     *         to get the port schema
     *
     * @return the port schema for the given input port
     */
    public PortRuntimeSchema getInputSchema ( final int portIndex )
    {
        return inputSchemas.get( portIndex );
    }

    /**
     * Returns the port schema for the given output port
     *
     * @param portIndex
     *         to get the port schema
     *
     * @return the port schema for the given output port
     */
    public PortRuntimeSchema getOutputSchema ( final int portIndex )
    {
        return outputSchemas.get( portIndex );
    }

    /**
     * Returns the schemas defined for input ports of the operator
     *
     * @return the schemas defined for input ports of the operator
     */
    public List<PortRuntimeSchema> getInputSchemas ()
    {
        return inputSchemas;
    }

    /**
     * Returns the schemas defined for output ports of the operator
     *
     * @return the schemas defined for output ports of the operator
     */
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
