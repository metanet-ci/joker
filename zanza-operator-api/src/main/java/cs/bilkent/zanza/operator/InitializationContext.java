package cs.bilkent.zanza.operator;

import cs.bilkent.zanza.flow.FlowDefinition;
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

    OperatorRuntimeSchema getRuntimeSchema ();

    PartitionKeyExtractor getPartitionKeyExtractor ();

    /**
     * Configuration of the operator instance given during building the {@link FlowDefinition}
     *
     * @return configuration of the operator instance given during building the {@link FlowDefinition}
     */
    OperatorConfig getConfig();

}
