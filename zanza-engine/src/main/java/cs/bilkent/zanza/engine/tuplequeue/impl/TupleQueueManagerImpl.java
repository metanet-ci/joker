package cs.bilkent.zanza.engine.tuplequeue.impl;


import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.config.ThreadingOption;
import static cs.bilkent.zanza.engine.config.ThreadingOption.SINGLE_THREADED;
import cs.bilkent.zanza.engine.partition.PartitionKeyFunction;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueManager;
import cs.bilkent.zanza.engine.tuplequeue.impl.context.DefaultTupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.impl.context.PartitionedTupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.impl.queue.BoundedTupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.impl.queue.UnboundedTupleQueue;
import cs.bilkent.zanza.flow.OperatorDefinition;
import static cs.bilkent.zanza.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import cs.bilkent.zanza.utils.Pair;


class TupleQueueManagerImpl implements TupleQueueManager
{

    private final ConcurrentMap<Pair<String, Integer>, TupleQueueContext> tupleQueueContexts = new ConcurrentHashMap<>();

    private int initialQueueCapacity = 10;

    public TupleQueueManagerImpl ()
    {
    }

    @Override
    public TupleQueueContext createTupleQueueContext ( final OperatorDefinition operatorDefinition,
                                                       final ThreadingOption threadingOption,
                                                       final int replicaIndex )
    {
        checkArgument( operatorDefinition != null );
        checkArgument( threadingOption != null );
        checkArgument( replicaIndex >= 0 );

        final Supplier<TupleQueue> tupleQueueSupplier = getTupleQueueSupplier( threadingOption, initialQueueCapacity );
        final Function<Pair<String, Integer>, TupleQueueContext> constructor = getTupleQueueContextConstructor( operatorDefinition,
                                                                                                                tupleQueueSupplier );

        return tupleQueueContexts.computeIfAbsent( Pair.of( operatorDefinition.id(), replicaIndex ), constructor );
    }

    private Supplier<TupleQueue> getTupleQueueSupplier ( final ThreadingOption threadingOption, final int queueCapacity )
    {
        return threadingOption == SINGLE_THREADED
               ? () -> new UnboundedTupleQueue( queueCapacity )
               : () -> new BoundedTupleQueue( queueCapacity );
    }

    private Function<Pair<String, Integer>, TupleQueueContext> getTupleQueueContextConstructor ( final OperatorDefinition
                                                                                                         operatorDefinition,
                                                                                                 final Supplier<TupleQueue>
                                                                                                         tupleQueueSupplier )
    {
        return operatorDefinition.operatorType() == PARTITIONED_STATEFUL
               ? s -> new PartitionedTupleQueueContext( operatorDefinition.id(),
                                                        operatorDefinition.inputPortCount(),
                                                        new PartitionKeyFunction( operatorDefinition.partitionFieldNames() ),
                                                        tupleQueueSupplier )
               : s -> new DefaultTupleQueueContext( operatorDefinition.id(), operatorDefinition.inputPortCount(), tupleQueueSupplier );
    }

    @Override
    public boolean releaseTupleQueueContext ( final String operatorId, int replicaIndex )
    {
        final TupleQueueContext tupleQueueContext = tupleQueueContexts.remove( Pair.of( operatorId, replicaIndex ) );
        final boolean removed = tupleQueueContext != null;
        if ( removed )
        {
            tupleQueueContext.clear();
        }

        return removed;
    }

}
