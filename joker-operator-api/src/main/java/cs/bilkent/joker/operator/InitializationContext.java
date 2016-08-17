package cs.bilkent.joker.operator;


import java.util.List;

import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.operator.schema.runtime.TupleSchema;


/**
 * Contains information about configuration and initialization state of an operator
 */
public interface InitializationContext
{

    /**
     * ID of the operator instance given during building the {@link OperatorDef}
     *
     * @return id of the operator instance given during building the {@link OperatorDef}
     */
    String getId ();

    /**
     * Input port count of the operator in the runtime
     *
     * @return input port count of the operator in the runtime
     */
    int getInputPortCount ();

    /**
     * Output port count of the operator in the runtime
     *
     * @return output port count of the operator in the runtime
     */
    int getOutputPortCount ();

    /**
     * Returns {@code true} if the input port specified with the port index is connected to an active upstream operator
     *
     * @param portIndex
     *         to check the input port
     *
     * @return {@code true} if the input port specified with the port index is connected to an active upstream operator
     */
    boolean isInputPortOpen ( int portIndex );

    /**
     * Returns {@code true} if the input port specified with the port index has no active upstream operator
     *
     * @param portIndex
     *         to check status of the input port
     *
     * @return {@code true} if the input port specified with the port index has no active upstream operator
     */
    default boolean isInputPortClosed ( int portIndex )
    {
        return !isInputPortOpen( portIndex );
    }

    /**
     * Returns schema of the tuples received via the specified input port of an operator
     *
     * @param portIndex
     *         to access the {@link TupleSchema}
     *
     * @return schema of the tuples received via the specified input port of an operator
     */
    TupleSchema getInputPortSchema ( int portIndex );

    /**
     * Returns schema of the tuples created by an operator instance for an output port. Please keep in mind that tuple schemas should be
     * provided to the output tuples for more efficient processing
     *
     * @param portIndex
     *         to access the {@link TupleSchema}
     *
     * @return schema of the tuples created by an operator instance for an output port
     */
    TupleSchema getOutputPortSchema ( int portIndex );

    /**
     * Names of the fields which are used for partitioning the {@link Tuple} instances for the {@link Operator}
     *
     * @return names of the fields which are used for partitioning the {@link Tuple} instances for the {@link Operator}
     */
    List<String> getPartitionFieldNames ();

    /**
     * Configuration of the operator instance given during building the {@link FlowDef}
     *
     * @return configuration of the operator instance given during building the {@link FlowDef}
     */
    OperatorConfig getConfig ();

}
