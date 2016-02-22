package cs.bilkent.zanza.utils;

import java.util.List;

import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.OperatorConfig;
import cs.bilkent.zanza.operator.schema.runtime.OperatorRuntimeSchema;
import static java.util.Collections.emptyList;


public class SimpleInitializationContext implements InitializationContext
{
    private String name;

    private int inputPortCount;

    private int outputPortCount;

    private List<String> partitionFieldNames = emptyList();

    private OperatorRuntimeSchema runtimeSchema;

    private OperatorConfig config = new OperatorConfig();

    public void setName ( final String name )
    {
        this.name = name;
    }

    @Override
    public String getId ()
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
    public List<String> getPartitionFieldNames ()
    {
        return partitionFieldNames;
    }

    public void setPartitionFieldNames ( final List<String> partitionFieldNames )
    {
        this.partitionFieldNames = partitionFieldNames;
    }

    public void setConfig ( final OperatorConfig config )
    {
        this.config = config;
    }

    @Override
    public OperatorConfig getConfig ()
    {
        return config;
    }

}
