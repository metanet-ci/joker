package cs.bilkent.zanza.engine.tuplequeue.impl;


import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.config.ThreadingPreference;
import static cs.bilkent.zanza.engine.config.ThreadingPreference.MULTI_THREADED;
import static cs.bilkent.zanza.engine.config.ThreadingPreference.SINGLE_THREADED;
import cs.bilkent.zanza.engine.config.ZanzaConfig;
import cs.bilkent.zanza.engine.partition.PartitionKeyFunction;
import cs.bilkent.zanza.engine.partition.PartitionKeyFunctionFactory;
import cs.bilkent.zanza.engine.partition.PartitionService;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContextManager;
import cs.bilkent.zanza.engine.tuplequeue.impl.context.DefaultTupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.impl.context.PartitionedTupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.impl.queue.MultiThreadedTupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.impl.queue.SingleThreadedTupleQueue;
import cs.bilkent.zanza.flow.OperatorDef;
import static cs.bilkent.zanza.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import cs.bilkent.zanza.utils.Pair;
import cs.bilkent.zanza.utils.Triple;

@Singleton
@NotThreadSafe
public class TupleQueueContextManagerImpl implements TupleQueueContextManager
{

    private static final Logger LOGGER = LoggerFactory.getLogger( TupleQueueContextManagerImpl.class );


    private final PartitionService partitionService;

    private final PartitionKeyFunctionFactory partitionKeyFunctionFactory;

    private final int initialTupleQueueCapacity;

    private final Map<Triple<Integer, Integer, String>, TupleQueueContext> singleTupleQueueContexts = new HashMap<>();

    private final Map<Pair<Integer, String>, PartitionedTupleQueueContext[]> partitionedTupleQueueContexts = new HashMap<>();

    private final Map<String, TupleQueueContainer[]> tupleQueueContainersByOperatorId = new HashMap<>();


    @Inject
    public TupleQueueContextManagerImpl ( final ZanzaConfig zanzaConfig,
                                          final PartitionService partitionService,
                                          final PartitionKeyFunctionFactory partitionKeyFunctionFactory )
    {
        this.partitionService = partitionService;
        this.partitionKeyFunctionFactory = partitionKeyFunctionFactory;
        this.initialTupleQueueCapacity = zanzaConfig.getTupleQueueManagerConfig().getTupleQueueInitialSize();
    }

    @Override
    public TupleQueueContext createDefaultTupleQueueContext ( final int regionId, final int replicaIndex, final OperatorDef operatorDef,
                                                              final ThreadingPreference threadingPreference )
    {
        checkArgument( operatorDef != null );
        checkArgument( operatorDef.operatorType() != PARTITIONED_STATEFUL || threadingPreference == MULTI_THREADED );
        checkArgument( threadingPreference != null );
        checkArgument( replicaIndex >= 0 );

        final Function<Boolean, TupleQueue> tupleQueueConstructor = getTupleQueueConstructor( threadingPreference,
                                                                                              initialTupleQueueCapacity );
        final String operatorId = operatorDef.id();
        final int inputPortCount = operatorDef.inputPortCount();

        final Function<Triple<Integer, Integer, String>, TupleQueueContext> c = t ->
        {
            LOGGER.info( "created single tuple queue context for regionId={} replicaIndex={} operatorId={}",
                         regionId,
                         replicaIndex,
                         operatorId );
            return new DefaultTupleQueueContext( operatorId, inputPortCount, threadingPreference, tupleQueueConstructor );
        };

        return singleTupleQueueContexts.computeIfAbsent( Triple.of( regionId, replicaIndex, operatorId ), c );
    }

    public TupleQueueContext getDefaultTupleQueueContext ( final int regionId, final int replicaIndex, final OperatorDef operatorDef )
    {
        return singleTupleQueueContexts.get( Triple.of( regionId, replicaIndex, operatorDef.id() ) );
    }

    @Override
    public PartitionedTupleQueueContext[] createPartitionedTupleQueueContext ( final int regionId,
                                                                               final int replicaCount,
                                                                               final OperatorDef operatorDef )
    {
        checkArgument( operatorDef != null );
        checkArgument( operatorDef.operatorType() == PARTITIONED_STATEFUL );
        checkArgument( replicaCount > 0 );
        final Function<Boolean, TupleQueue> tupleQueueConstructor = getTupleQueueConstructor( SINGLE_THREADED, initialTupleQueueCapacity );
        final String operatorId = operatorDef.id();
        final int inputPortCount = operatorDef.inputPortCount();

        final Function<Pair<Integer, String>, PartitionedTupleQueueContext[]> c = p ->
        {
            final PartitionedTupleQueueContext[] tupleQueueContexts = new PartitionedTupleQueueContext[ replicaCount ];
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                final TupleQueueContainer[] containers = getOrCreateTupleQueueContainers( operatorId,
                                                                                          inputPortCount,
                                                                                          partitionService.getPartitionCount(),
                                                                                          tupleQueueConstructor );
                final int[] partitions = partitionService.getOrCreatePartitionDistribution( regionId, replicaCount );
                final PartitionKeyFunction partitionKeyExtractor = partitionKeyFunctionFactory.createPartitionKeyFunction( operatorDef
                                                                                                                                   .partitionFieldNames() );
                tupleQueueContexts[ replicaIndex ] = new PartitionedTupleQueueContext( operatorId,
                                                                                       inputPortCount,
                                                                                       partitionService.getPartitionCount(),
                                                                                       replicaIndex,
                                                                                       partitionKeyExtractor,
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

    public PartitionedTupleQueueContext[] getPartitionedTupleQueueContexts ( final int regionId, final OperatorDef operatorDef )
    {
        return partitionedTupleQueueContexts.get( Pair.of( regionId, operatorDef.id() ) );
    }

    private Function<Boolean, TupleQueue> getTupleQueueConstructor ( final ThreadingPreference threadingPreference,
                                                                     final int queueCapacity )
    {
        return threadingPreference == SINGLE_THREADED
               ? ( capacityCheckEnabled ) -> new SingleThreadedTupleQueue( queueCapacity )
               : ( capacityCheckEnabled ) -> new MultiThreadedTupleQueue( queueCapacity, capacityCheckEnabled );
    }

    @Override
    public boolean releaseDefaultTupleQueueContext ( final int regionId, final int replicaIndex, final String operatorId )
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
                                                                    final Function<Boolean, TupleQueue> tupleQueueConstructor )
    {
        return tupleQueueContainersByOperatorId.computeIfAbsent( operatorId, s ->
        {
            final TupleQueueContainer[] containers = new TupleQueueContainer[ partitionCount ];
            for ( int i = 0; i < partitionCount; i++ )
            {
                containers[ i ] = new TupleQueueContainer( operatorId, inputPortCount, i, tupleQueueConstructor );
            }

            return containers;
        } );
    }

    private boolean releaseTupleQueueContainers ( final String operatorId )
    {
        final TupleQueueContainer[] tupleQueueContainers = tupleQueueContainersByOperatorId.remove( operatorId );

        if ( tupleQueueContainers != null )
        {
            LOGGER.info( "Releasing tuple queue containers of operator {}", operatorId );
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
