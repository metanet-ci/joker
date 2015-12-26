package cs.bilkent.zanza.utils;

import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.OperatorConfig;
import cs.bilkent.zanza.operator.PartitionKeyExtractor;
import cs.bilkent.zanza.operator.schema.runtime.OperatorRuntimeSchema;

public class SimpleInitializationContext implements InitializationContext
{
    private String name;

    private int inputPortCount;

    private int outputPortCount;

    private PartitionKeyExtractor partitionKeyExtractor;

    private OperatorRuntimeSchema runtimeSchema;

    private OperatorConfig config = new OperatorConfig();

    public void setName ( final String name )
    {
        this.name = name;
    }

    @Override
    public String getName ()
    {
        return name;
    }

    @Override
    public int getInputPortCount ()
    {
        return inputPortCount;
    }

    @Override
    public int getOutputPortCount ()
    {
        return outputPortCount;
    }

    public void setInputPortCount ( final int inputPortCount )
    {
        this.inputPortCount = inputPortCount;
    }

    public void setOutputPortCount ( final int outputPortCount )
    {
        this.outputPortCount = outputPortCount;
    }

    @Override
    public OperatorRuntimeSchema getRuntimeSchema ()
    {
        return runtimeSchema;
    }

    public void setRuntimeSchema ( final OperatorRuntimeSchema runtimeSchema )
    {
        this.runtimeSchema = runtimeSchema;
    }

    @Override
    public PartitionKeyExtractor getPartitionKeyExtractor ()
    {
        return partitionKeyExtractor;
    }

    public void setPartitionKeyExtractor ( final PartitionKeyExtractor partitionKeyExtractor )
    {
        this.partitionKeyExtractor = partitionKeyExtractor;
    }

    @Override
    public OperatorConfig getConfig ()
    {
        return config;
    }

}
