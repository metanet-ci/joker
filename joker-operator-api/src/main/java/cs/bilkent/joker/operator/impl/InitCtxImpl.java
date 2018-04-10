package cs.bilkent.joker.operator.impl;

import java.util.Arrays;
import java.util.List;

import cs.bilkent.joker.operator.InitCtx;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchema;
import cs.bilkent.joker.operator.schema.runtime.TupleSchema;


public class InitCtxImpl implements InitCtx
{
    private String id;

    private int inputPortCount;

    private int outputPortCount;

    private List<String> partitionFieldNames;

    private OperatorRuntimeSchema runtimeSchema;

    private OperatorConfig config;

    private boolean[] upstreamConnectionStatuses;

    public InitCtxImpl ( final OperatorDef operatorDef, final boolean[] upstreamConnectionStatuses )
    {
        this.id = operatorDef.getId();
        this.inputPortCount = operatorDef.getInputPortCount();
        this.outputPortCount = operatorDef.getOutputPortCount();
        this.partitionFieldNames = operatorDef.getPartitionFieldNames();
        this.runtimeSchema = operatorDef.getSchema();
        this.config = operatorDef.getConfig();
        this.upstreamConnectionStatuses = Arrays.copyOf( upstreamConnectionStatuses, upstreamConnectionStatuses.length );
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
    public TupleSchema getInputPortSchema ( final int portIndex )
    {
        return runtimeSchema.getInputSchema( portIndex );
    }

    @Override
    public TupleSchema getOutputPortSchema ( final int portIndex )
    {
        return runtimeSchema.getOutputSchema( portIndex );
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

}
