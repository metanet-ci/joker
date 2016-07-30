package cs.bilkent.zanza.operator;


import java.util.List;

import cs.bilkent.zanza.flow.FlowDef;
import cs.bilkent.zanza.operator.schema.annotation.OperatorSchema;
import cs.bilkent.zanza.operator.schema.runtime.OperatorRuntimeSchema;


/**
 * Contains information about configuration and initialization of an operator
 */
public interface InitializationContext
{

    /**
     * ID of the operator instance given during building the {@link FlowDef}
     *
     * @return id of the operator instance given during building the {@link FlowDef}
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
     * Returns true if the input port specified with the port index is connected to an upstream operator
     *
     * @param portIndex
     *         to check the input port
     *
     * @return true if the input port specified with the port index is connected to an upstream operator
     */
    boolean isInputPortOpen ( int portIndex );

    /**
     * Returns true if the input port specified with the port index is not connected to an upstream operator
     *
     * @param portIndex
     *         to check the input port
     *
     * @return true if the input port specified with the port index is not connected to an upstream operator
     */
    default boolean isInputPortClosed ( int portIndex )
    {
        return !isInputPortOpen( portIndex );
    }

    /**
     * Schema of the operator that has been built in the run time. It is the combination of the schema given in {@link OperatorSchema}
     * with the {@link Operator} class and given in {@link OperatorRuntimeSchema} during the {@link FlowDef} composition
     *
     * @return schema of the operator that has been built in the run time. It is the combination of the schema given in
     * {@link OperatorSchema}
     * with the {@link Operator} class and given in {@link OperatorRuntimeSchema} during the {@link FlowDef} composition
     */
    OperatorRuntimeSchema getRuntimeSchema ();

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
