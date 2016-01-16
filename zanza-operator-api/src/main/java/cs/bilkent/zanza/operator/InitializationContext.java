package cs.bilkent.zanza.operator;


import java.util.List;

import cs.bilkent.zanza.flow.FlowDefinition;
import cs.bilkent.zanza.operator.schema.annotation.OperatorSchema;
import cs.bilkent.zanza.operator.schema.runtime.OperatorRuntimeSchema;


/**
 * Contains information about configuration and initialization of an operator
 */
public interface InitializationContext
{

    /**
     * Name of the operator instance given during building the {@link FlowDefinition}
     *
     * @return name of the operator instance given during building the {@link FlowDefinition}
     */
    String getName ();

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
     * Schema of the operator that has been built in the run time. It is the combination of the schema given in {@link OperatorSchema}
     * with the {@link Operator} class and given in {@link OperatorRuntimeSchema} during the {@link FlowDefinition} composition
     *
     * @return schema of the operator that has been built in the run time. It is the combination of the schema given in
     * {@link OperatorSchema}
     * with the {@link Operator} class and given in {@link OperatorRuntimeSchema} during the {@link FlowDefinition} composition
     */
    OperatorRuntimeSchema getRuntimeSchema ();

    /**
     * Names of the fields which are used for partitioning the {@link Tuple} instances for the {@link Operator}
     *
     * @return names of the fields which are used for partitioning the {@link Tuple} instances for the {@link Operator}
     */
    List<String> getPartitionFieldNames ();

    /**
     * Configuration of the operator instance given during building the {@link FlowDefinition}
     *
     * @return configuration of the operator instance given during building the {@link FlowDefinition}
     */
    OperatorConfig getConfig ();

}
