package cs.bilkent.zanza.engine.tuplequeue.impl;


import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueManager;
import static cs.bilkent.zanza.engine.tuplequeue.TupleQueueManager.TupleQueueThreading.SINGLE_THREADED;
import cs.bilkent.zanza.engine.tuplequeue.impl.context.DefaultTupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.impl.context.PartitionedTupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.impl.queue.BlockingTupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.impl.queue.SingleThreadedTupleQueue;
import static cs.bilkent.zanza.engine.util.Preconditions.checkOperatorTypeAndPartitionKeyFieldNames;
import cs.bilkent.zanza.operator.spec.OperatorType;
import static cs.bilkent.zanza.operator.spec.OperatorType.PARTITIONED_STATEFUL;


class TupleQueueManagerImpl implements TupleQueueManager
{

    private static final Logger LOGGER = LoggerFactory.getLogger( TupleQueueManagerImpl.class );


    private final ConcurrentMap<String, TupleQueueContext> tupleQueueContexts = new ConcurrentHashMap<>();

    public TupleQueueManagerImpl ()
    {
    }

    @Override
    public TupleQueueContext createTupleQueueContext ( final String operatorId,
                                                       final int inputPortCount,
                                                       final OperatorType operatorType,
                                                       final List<String> partitionFieldNames,
                                                       final TupleQueueThreading tupleQueueThreading,
                                                       final int initialQueueCapacity )
    {
        checkArgument( operatorId != null );
        checkArgument( inputPortCount > 0 );
        checkArgument( operatorType != null );
        checkOperatorTypeAndPartitionKeyFieldNames( operatorType, partitionFieldNames );
        checkArgument( initialQueueCapacity > 0 );

        final Supplier<TupleQueue> tupleQueueSupplier = getTupleQueueSupplier( tupleQueueThreading, initialQueueCapacity );
        final Function<String, TupleQueueContext> tupleQueueContextConstructor = getTupleQueueContextConstructor( operatorId,
                                                                                                                  inputPortCount,
                                                                                                                  operatorType,
                                                                                                                  partitionFieldNames,
                                                                                                                  tupleQueueSupplier );

        return tupleQueueContexts.computeIfAbsent( operatorId, tupleQueueContextConstructor );
    }

    private Supplier<TupleQueue> getTupleQueueSupplier ( final TupleQueueThreading tupleQueueThreading, final int queueCapacity )
    {
        return tupleQueueThreading == SINGLE_THREADED
               ? () -> new SingleThreadedTupleQueue( queueCapacity ) : () -> new BlockingTupleQueue( queueCapacity );
    }

    private Function<String, TupleQueueContext> getTupleQueueContextConstructor ( final String operatorId,
                                                                                  final int inputPortCount,
                                                                                  final OperatorType operatorType,
                                                                                  final List<String> partitionFieldNames,
                                                                                  final Supplier<TupleQueue> tupleQueueSupplier )
    {
        return operatorType == PARTITIONED_STATEFUL
               ? s -> new PartitionedTupleQueueContext( operatorId,
                                                        inputPortCount,
                                                        partitionFieldNames,
                                                        tupleQueueSupplier )
               : s -> new DefaultTupleQueueContext( operatorId, inputPortCount, tupleQueueSupplier );
    }

    @Override
    public boolean releaseTupleQueueContext ( final String operatorId )
    {
        final TupleQueueContext tupleQueueContext = tupleQueueContexts.remove( operatorId );
        final boolean removed = tupleQueueContext != null;
        if ( removed )
        {
            tupleQueueContext.clear();
        }

        return removed;
    }

}
