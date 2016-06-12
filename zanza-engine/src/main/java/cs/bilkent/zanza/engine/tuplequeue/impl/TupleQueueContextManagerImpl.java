package cs.bilkent.zanza.engine.tuplequeue.impl;


import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.config.ThreadingPreference;
import static cs.bilkent.zanza.engine.config.ThreadingPreference.SINGLE_THREADED;
import cs.bilkent.zanza.engine.config.ZanzaConfig;
import cs.bilkent.zanza.engine.partition.PartitionKeyFunction;
import cs.bilkent.zanza.engine.partition.PartitionService;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContextManager;
import cs.bilkent.zanza.engine.tuplequeue.impl.context.DefaultTupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.impl.context.PartitionedTupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.impl.queue.MultiThreadedTupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.impl.queue.SingleThreadedTupleQueue;
import cs.bilkent.zanza.flow.OperatorDefinition;
import static cs.bilkent.zanza.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import cs.bilkent.zanza.utils.Pair;
import cs.bilkent.zanza.utils.Triple;

@NotThreadSafe
public class TupleQueueContextManagerImpl implements TupleQueueContextManager
{

    private static final Logger LOGGER = LoggerFactory.getLogger( TupleQueueContextManagerImpl.class );


    private PartitionService partitionService;

    private final Map<Triple<Integer, Integer, String>, TupleQueueContext> singleTupleQueueContexts = new HashMap<>();

    private final Map<Pair<Integer, String>, PartitionedTupleQueueContext[]> partitionedTupleQueueContexts = new HashMap<>();

    private final Map<String, TupleQueueContainer[]> tupleQueueContainersByOperatorId = new HashMap<>();

    private int initialTupleQueueCapacity;

    private int partitionCount;

    public TupleQueueContextManagerImpl ()
    {
    }

    public void setPartitionService ( final PartitionService partitionService )
    {
        this.partitionService = partitionService;
    }

    @Override
    public void init ( final ZanzaConfig config )
    {
        initialTupleQueueCapacity = config.getTupleQueueManagerConfig().getTupleQueueInitialSize();
        partitionCount = config.getPartitionServiceConfig().partitionCount;
    }

    @Override
    public TupleQueueContext createDefaultTupleQueueContext ( final int regionId,
                                                              final int replicaIndex,
                                                              final OperatorDefinition operatorDefinition,
                                                              final ThreadingPreference threadingPreference )
    {
        checkArgument( operatorDefinition != null );
        checkArgument( operatorDefinition.operatorType() != PARTITIONED_STATEFUL );
        checkArgument( threadingPreference != null );
        checkArgument( replicaIndex >= 0 );

        final Function<Boolean, TupleQueue> tupleQueueConstructor = getTupleQueueConstructor( threadingPreference,
                                                                                              initialTupleQueueCapacity );
        final String operatorId = operatorDefinition.id();
        final int inputPortCount = operatorDefinition.inputPortCount();

        final Function<Triple<Integer, Integer, String>, TupleQueueContext> c = t -> {
            LOGGER.info( "created single tuple queue context for regionId={} replicaIndex={} operatorId={}",
                         regionId,
                         replicaIndex,
                         operatorId );
            return new DefaultTupleQueueContext( operatorId, inputPortCount, threadingPreference, tupleQueueConstructor );
        };

        return singleTupleQueueContexts.computeIfAbsent( Triple.of( regionId, replicaIndex, operatorId ), c );
    }

    @Override
    public PartitionedTupleQueueContext[] createPartitionedTupleQueueContext ( final int regionId,
                                                                               final int replicaCount,
                                                                               final OperatorDefinition operatorDefinition,
                                                                               final ThreadingPreference threadingPreference )
    {
        checkArgument( operatorDefinition != null );
        checkArgument( operatorDefinition.operatorType() == PARTITIONED_STATEFUL );
        checkArgument( threadingPreference != null );
        checkArgument( replicaCount > 0 );
        final Function<Boolean, TupleQueue> tupleQueueConstructor = getTupleQueueConstructor( threadingPreference,
                                                                                              initialTupleQueueCapacity );
        final String operatorId = operatorDefinition.id();
        final int inputPortCount = operatorDefinition.inputPortCount();

        final Function<Pair<Integer, String>, PartitionedTupleQueueContext[]> c = p -> {
            final PartitionedTupleQueueContext[] tupleQueueContexts = new PartitionedTupleQueueContext[ replicaCount ];
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                final TupleQueueCapacityState tupleQueueCapacityState = new TupleQueueCapacityState( inputPortCount );

                final TupleQueueContainer[] containers = getOrCreateTupleQueueContainers( operatorId,
                                                                                          inputPortCount,
                                                                                          partitionCount,
                                                                                          threadingPreference,
                                                                                          tupleQueueCapacityState,
                                                                                          tupleQueueConstructor );
                final int[] partitions = partitionService.getOrCreatePartitionDistribution( regionId, replicaCount );
                final PartitionKeyFunction partitionKeyExtractor = new PartitionKeyFunction( operatorDefinition.partitionFieldNames() );
                tupleQueueContexts[ replicaIndex ] = new PartitionedTupleQueueContext( operatorId,
                                                                                       inputPortCount,
                                                                                       partitionCount,
                                                                                       replicaIndex,
                                                                                       threadingPreference,
                                                                                       partitionKeyExtractor,
                                                                                       tupleQueueCapacityState,
                                                                                       containers,
                                                                                       partitions );
            }

            LOGGER.info( "created partitioned tuple queue context for regionId={} replicaCount={} operatorId={}",
                         regionId,
                         replicaCount,
                         operatorId );
            return tupleQueueContexts;
        };

        return partitionedTupleQueueContexts.computeIfAbsent( Pair.of( regionId, operatorId ), c );

    }

    private Function<Boolean, TupleQueue> getTupleQueueConstructor ( final ThreadingPreference threadingPreference,
                                                                     final int queueCapacity )
    {
        return threadingPreference == SINGLE_THREADED
               ? ( capacityCheckEnabled ) -> new SingleThreadedTupleQueue( queueCapacity )
               : ( capacityCheckEnabled ) -> new MultiThreadedTupleQueue( queueCapacity, capacityCheckEnabled );
    }

    @Override
    public boolean releaseTupleQueueContext ( final int regionId, final int replicaIndex, final String operatorId )
    {
        final TupleQueueContext tupleQueueContext = singleTupleQueueContexts.remove( Triple.of( regionId, replicaIndex, operatorId ) );
        if ( tupleQueueContext != null )
        {
            tupleQueueContext.clear();
            return true;
        }

        LOGGER.warn( "no single tuple queue context to release for regionId={} replicaIndex={} operatorId={}",
                     regionId,
                     replicaIndex,
                     operatorId );
        return false;
    }

    @Override
    public boolean releasePartitionedTupleQueueContexts ( final int regionId, final String operatorId )
    {
        final TupleQueueContext[] tupleQueueContexts = partitionedTupleQueueContexts.remove( Pair.of( regionId, operatorId ) );
        if ( tupleQueueContexts != null )
        {
            releaseTupleQueueContainers( operatorId );
            return true;
        }

        LOGGER.warn( "no partitioned tuple queue context to release for regionId={} operatorId={}", regionId, operatorId );
        return false;
    }

    private TupleQueueContainer[] getOrCreateTupleQueueContainers ( final String operatorId,
                                                                    final int inputPortCount,
                                                                    final int partitionCount,
                                                                    final ThreadingPreference threadingPreference,
                                                                    final TupleQueueCapacityState tupleQueueCapacityState,
                                                                    final Function<Boolean, TupleQueue> tupleQueueConstructor )
    {
        return tupleQueueContainersByOperatorId.computeIfAbsent( operatorId, s -> {
            final TupleQueueContainer[] containers = new TupleQueueContainer[ partitionCount ];
            for ( int i = 0; i < partitionCount; i++ )
            {
                containers[ i ] = new TupleQueueContainer( operatorId,
                                                           inputPortCount,
                                                           threadingPreference,
                                                           tupleQueueCapacityState,
                                                           tupleQueueConstructor );
            }

            return containers;
        } );
    }

    private boolean releaseTupleQueueContainers ( final String operatorId )
    {
        final TupleQueueContainer[] tupleQueueContainers = tupleQueueContainersByOperatorId.remove( operatorId );

        if ( tupleQueueContainers != null )
        {
            LOGGER.info( "tuple queue containers of operator {} are released.", operatorId );
            for ( TupleQueueContainer container : tupleQueueContainers )
            {
                container.clear();
            }

            return true;
        }

        LOGGER.error( "no tuple queue containers are found for operator {}.", operatorId );
        return false;
    }

}
