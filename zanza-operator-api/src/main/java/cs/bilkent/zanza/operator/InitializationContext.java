package cs.bilkent.zanza.operator;

import cs.bilkent.zanza.flow.FlowDefinition;

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
     * Configuration of the operator instance given during building the {@link FlowDefinition}
     *
     * @return configuration of the operator instance given during building the {@link FlowDefinition}
     */
    OperatorConfig getConfig();

}
