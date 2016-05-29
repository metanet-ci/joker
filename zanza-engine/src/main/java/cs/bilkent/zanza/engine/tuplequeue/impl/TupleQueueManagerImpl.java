package cs.bilkent.zanza.engine.tuplequeue.impl;


import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import javax.annotation.concurrent.NotThreadSafe;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.config.ThreadingPreference;
import static cs.bilkent.zanza.engine.config.ThreadingPreference.SINGLE_THREADED;
import cs.bilkent.zanza.engine.config.ZanzaConfig;
import cs.bilkent.zanza.engine.partition.PartitionKeyFunction;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueManager;
import cs.bilkent.zanza.engine.tuplequeue.impl.context.DefaultTupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.impl.context.EmptyTupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.impl.context.PartitionedTupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.impl.queue.MultiThreadedTupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.impl.queue.SingleThreadedTupleQueue;
import cs.bilkent.zanza.flow.OperatorDefinition;
import static cs.bilkent.zanza.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import cs.bilkent.zanza.utils.Pair;

@NotThreadSafe
public class TupleQueueManagerImpl implements TupleQueueManager
{

    private final ConcurrentMap<Pair<String, Integer>, TupleQueueContext> tupleQueueContexts = new ConcurrentHashMap<>();

    private int initialTupleQueueCapacity;

    public TupleQueueManagerImpl ()
    {
    }

    @Override
    public void init ( final ZanzaConfig config )
    {
        initialTupleQueueCapacity = config.getTupleQueueManagerConfig().getTupleQueueInitialSize();
    }

    @Override
    public TupleQueueContext createEmptyTupleQueueContext ( final OperatorDefinition operatorDefinition, final int replicaIndex )
    {
        checkArgument( operatorDefinition != null );
        checkArgument( replicaIndex >= 0 );

        final String operatorId = operatorDefinition.id();
        final int inputPortCount = operatorDefinition.inputPortCount();

        final Function<Boolean, TupleQueue> tupleQueueConstructor = getTupleQueueConstructor( SINGLE_THREADED, initialTupleQueueCapacity );
        return tupleQueueContexts.computeIfAbsent( Pair.of( operatorId, replicaIndex ),
                                                   p -> new EmptyTupleQueueContext( operatorId, inputPortCount, tupleQueueConstructor ) );
    }

    @Override
    public TupleQueueContext createTupleQueueContext ( final OperatorDefinition operatorDefinition,
                                                       final ThreadingPreference threadingPreference,
                                                       final int replicaIndex )
    {
        checkArgument( operatorDefinition != null );
        checkArgument( threadingPreference != null );
        checkArgument( replicaIndex >= 0 );

        final Function<Boolean, TupleQueue> tupleQueueConstructor = getTupleQueueConstructor( threadingPreference,
                                                                                              initialTupleQueueCapacity );
        final Function<Pair<String, Integer>, TupleQueueContext> constructor = getTupleQueueContextConstructor( operatorDefinition,
                                                                                                                threadingPreference,
                                                                                                                tupleQueueConstructor );

        return tupleQueueContexts.computeIfAbsent( Pair.of( operatorDefinition.id(), replicaIndex ), constructor );
    }

    private Function<Boolean, TupleQueue> getTupleQueueConstructor ( final ThreadingPreference threadingPreference,
                                                                     final int queueCapacity )
    {
        return threadingPreference == SINGLE_THREADED
               ? ( capacityCheckEnabled ) -> new SingleThreadedTupleQueue( queueCapacity )
               : ( capacityCheckEnabled ) -> new MultiThreadedTupleQueue( queueCapacity, capacityCheckEnabled );
    }

    private Function<Pair<String, Integer>, TupleQueueContext> getTupleQueueContextConstructor ( final OperatorDefinition
                                                                                                         operatorDefinition,
                                                                                                 final ThreadingPreference
                                                                                                         threadingPreference,
                                                                                                 final Function<Boolean, TupleQueue>
                                                                                                         tupleQueueConstructor )
    {
        return operatorDefinition.operatorType() == PARTITIONED_STATEFUL
               ? p -> new PartitionedTupleQueueContext( operatorDefinition.id(),
                                                        operatorDefinition.inputPortCount(),
                                                        threadingPreference,
                                                        new PartitionKeyFunction( operatorDefinition.partitionFieldNames() ),
                                                        tupleQueueConstructor )
               : p -> new DefaultTupleQueueContext( operatorDefinition.id(),
                                                    operatorDefinition.inputPortCount(),
                                                    threadingPreference,
                                                    tupleQueueConstructor );
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
