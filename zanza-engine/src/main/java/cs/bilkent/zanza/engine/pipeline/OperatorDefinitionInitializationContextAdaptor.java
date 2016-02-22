package cs.bilkent.zanza.engine.pipeline;

import java.util.List;

import cs.bilkent.zanza.flow.OperatorDefinition;
import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.OperatorConfig;
import cs.bilkent.zanza.operator.schema.runtime.OperatorRuntimeSchema;

class OperatorDefinitionInitializationContextAdaptor implements InitializationContext
{

    private final OperatorDefinition operatorDefinition;

    public OperatorDefinitionInitializationContextAdaptor ( final OperatorDefinition operatorDefinition )
    {
        this.operatorDefinition = operatorDefinition;
    }

    @Override
    public String getId ()
    {
        return operatorDefinition.id();
    }

    @Override
    public int getInputPortCount ()
    {
        return operatorDefinition.inputPortCount();
    }

    @Override
    public int getOutputPortCount ()
    {
        return operatorDefinition.outputPortCount();
    }

    @Override
    public OperatorRuntimeSchema getRuntimeSchema ()
    {
        return operatorDefinition.schema();
    }

    @Override
    public List<String> getPartitionFieldNames ()
    {
        return operatorDefinition.partitionFieldNames();
    }

    @Override
    public OperatorConfig getConfig ()
    {
        return operatorDefinition.config();
    }

}
