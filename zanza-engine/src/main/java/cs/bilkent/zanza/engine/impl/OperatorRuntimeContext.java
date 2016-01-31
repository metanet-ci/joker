package cs.bilkent.zanza.engine.impl;

import java.util.List;
import java.util.function.Function;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.flow.OperatorDefinition;
import cs.bilkent.zanza.kvstore.KVStore;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.InvocationContext.InvocationReason;
import cs.bilkent.zanza.operator.InvocationResult;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.scheduling.SchedulingStrategy;
import cs.bilkent.zanza.utils.SimpleInvocationContext;


public class OperatorRuntimeContext
{

    private final OperatorDefinition operatorDef;

    private final List<String> partitionFieldNames;

    private final Operator operator;

    private final TupleQueueContext tupleQueueContext;

    private final Function<PortsToTuples, KVStore> kvStoreProvider;

    private SchedulingStrategy schedulingStrategy;

    public OperatorRuntimeContext ( final OperatorDefinition operatorDef,
                                    final Operator operator,
                                    final TupleQueueContext tupleQueueContext,
                                    final Function<PortsToTuples, KVStore> kvStoreProvider,
                                    final SchedulingStrategy initialSchedulingStrategy )
    {
        this.operatorDef = operatorDef;
        this.partitionFieldNames = operatorDef.partitionFieldNames;
        this.operator = operator;
        this.tupleQueueContext = tupleQueueContext;
        this.kvStoreProvider = kvStoreProvider;
        this.schedulingStrategy = initialSchedulingStrategy;
    }

    public SchedulingStrategy getSchedulingStrategy ()
    {
        return schedulingStrategy;
    }

    public List<String> getPartitionFieldNames ()
    {
        return partitionFieldNames;
    }

    public void addToTupleQueue ( final PortsToTuples input )
    {
        tupleQueueContext.add( input );
    }


    public InvocationResult invoke ( final InvocationReason reason, final PortsToTuples input )
    {
        final KVStore kvStore = kvStoreProvider.apply( input );
        final InvocationContext invocationContext = new SimpleInvocationContext( reason, input, kvStore );
        final InvocationResult invocationResult = operator.process( invocationContext );
        schedulingStrategy = invocationResult.getSchedulingStrategy();

        return invocationResult;
    }

}
