package cs.bilkent.joker.operator.impl;

import java.util.Arrays;
import java.util.List;

import cs.bilkent.joker.operator.InitializationContext;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchema;
import static java.util.Collections.emptyList;


public class InitializationContextImpl implements InitializationContext
{
    private String id;

    private int inputPortCount;

    private int outputPortCount;

    private List<String> partitionFieldNames = emptyList();

    private OperatorRuntimeSchema runtimeSchema;

    private OperatorConfig config = new OperatorConfig();

    private boolean[] upstreamConnectionStatuses;

    public InitializationContextImpl ()
    {
    }

    public InitializationContextImpl ( final String id,
                                       final int inputPortCount,
                                       final int outputPortCount,
                                       final List<String> partitionFieldNames,
                                       final OperatorRuntimeSchema runtimeSchema,
                                       final OperatorConfig config,
                                       final boolean[] upstreamConnectionStatuses )
    {
        this.id = id;
        this.inputPortCount = inputPortCount;
        this.outputPortCount = outputPortCount;
        this.partitionFieldNames = partitionFieldNames;
        this.runtimeSchema = runtimeSchema;
        this.config = config;
        this.upstreamConnectionStatuses = Arrays.copyOf( upstreamConnectionStatuses, upstreamConnectionStatuses.length );
    }

    public void setId ( final String id )
    {
        this.id = id;
    }

    @Override
    public String getId ()
    {
        return id;
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

    @Override
    public boolean isInputPortOpen ( final int portIndex )
    {
        return upstreamConnectionStatuses[ portIndex ];
    }

    @Override
    public OperatorRuntimeSchema getRuntimeSchema ()
    {
        return runtimeSchema;
    }

    @Override
    public List<String> getPartitionFieldNames ()
    {
        return partitionFieldNames;
    }

    @Override
    public OperatorConfig getConfig ()
    {
        return config;
    }

    public void setInputPortCount ( final int inputPortCount )
    {
        this.inputPortCount = inputPortCount;
    }

    public void setOutputPortCount ( final int outputPortCount )
    {
        this.outputPortCount = outputPortCount;
    }

    public void setRuntimeSchema ( final OperatorRuntimeSchema runtimeSchema )
    {
        this.runtimeSchema = runtimeSchema;
    }

    public void setPartitionFieldNames ( final List<String> partitionFieldNames )
    {
        this.partitionFieldNames = partitionFieldNames;
    }

    public void setConfig ( final OperatorConfig config )
    {
        this.config = config;
    }

    public void setUpstreamConnectionStatuses ( final boolean[] upstreamConnectionStatuses )
    {
        this.upstreamConnectionStatuses = Arrays.copyOf( upstreamConnectionStatuses, upstreamConnectionStatuses.length );
    }
}
