package cs.bilkent.joker.engine.region.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.config.ThreadingPreference;
import static cs.bilkent.joker.engine.config.ThreadingPreference.MULTI_THREADED;
import static cs.bilkent.joker.engine.config.ThreadingPreference.SINGLE_THREADED;
import cs.bilkent.joker.engine.kvstore.KVStoreContext;
import cs.bilkent.joker.engine.kvstore.KVStoreContextManager;
import cs.bilkent.joker.engine.kvstore.impl.DefaultKVStoreContext;
import cs.bilkent.joker.engine.kvstore.impl.EmptyKVStoreContext;
import cs.bilkent.joker.engine.kvstore.impl.PartitionedKVStoreContext;
import cs.bilkent.joker.engine.pipeline.OperatorReplica;
import cs.bilkent.joker.engine.pipeline.PipelineId;
import cs.bilkent.joker.engine.pipeline.PipelineReplica;
import cs.bilkent.joker.engine.pipeline.PipelineReplicaId;
import cs.bilkent.joker.engine.pipeline.impl.tuplesupplier.CachedTuplesImplSupplier;
import cs.bilkent.joker.engine.pipeline.impl.tuplesupplier.NonCachedTuplesImplSupplier;
import cs.bilkent.joker.engine.region.Region;
import cs.bilkent.joker.engine.region.RegionConfig;
import cs.bilkent.joker.engine.region.RegionManager;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueContextManager;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.context.DefaultTupleQueueContext;
import cs.bilkent.joker.engine.tuplequeue.impl.context.EmptyTupleQueueContext;
import cs.bilkent.joker.engine.tuplequeue.impl.context.PartitionedTupleQueueContext;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.pool.BlockingTupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.pool.NonBlockingTupleQueueDrainerPool;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.spec.OperatorType;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;

@Singleton
@NotThreadSafe
public class RegionManagerImpl implements RegionManager
{

    private static final Logger LOGGER = LoggerFactory.getLogger( RegionManagerImpl.class );


    private final JokerConfig config;

    private final KVStoreContextManager kvStoreContextManager;

    private final TupleQueueContextManager tupleQueueContextManager;

    private final Map<Integer, Region> regions = new HashMap<>();

    @Inject
    public RegionManagerImpl ( final JokerConfig config,
                               final KVStoreContextManager kvStoreContextManager,
                               final TupleQueueContextManager tupleQueueContextManager )
    {
        this.config = config;
        this.kvStoreContextManager = kvStoreContextManager;
        this.tupleQueueContextManager = tupleQueueContextManager;
    }

    @Override
    public Region createRegion ( final FlowDef flow, final RegionConfig regionConfig )
    {
        checkState( !regions.containsKey( regionConfig.getRegionId() ), "Region %s is already created!", regionConfig.getRegionId() );
        checkPipelineStartIndices( regionConfig );

        final int regionId = regionConfig.getRegionId();
        final int replicaCount = regionConfig.getReplicaCount();
        final int pipelineCount = regionConfig.getPipelineStartIndices().size();

        LOGGER.info( "Creating components for regionId={} pipelineCount={} replicaCount={}", regionId, pipelineCount, replicaCount );

        final PipelineReplica[][] pipelineReplicas = new PipelineReplica[ pipelineCount ][ replicaCount ];

        for ( int pipelineId = 0; pipelineId < pipelineCount; pipelineId++ )
        {
            final OperatorDef[] operatorDefs = regionConfig.getOperatorDefs( pipelineId );
            final int operatorCount = operatorDefs.length;
            final OperatorReplica[][] operatorReplicas = new OperatorReplica[ replicaCount ][ operatorCount ];
            LOGGER.info( "Initializing pipeline instance for regionId={} pipelineId={} with {} operators",
                         regionId,
                         pipelineId,
                         operatorCount );

            final PipelineReplicaId[] pipelineReplicaIds = createPipelineReplicaIds( regionId, replicaCount, pipelineId );

            for ( int operatorIndex = 0; operatorIndex < operatorCount; operatorIndex++ )
            {
                final OperatorDef operatorDef = operatorDefs[ operatorIndex ];

                final boolean isFirstOperator = operatorIndex == 0;
                final TupleQueueContext[] tupleQueueContexts = createTupleQueueContexts( flow,
                                                                                         regionId,
                                                                                         replicaCount,
                                                                                         isFirstOperator,
                                                                                         operatorDef );

                final TupleQueueDrainerPool[] drainerPools = createTupleQueueDrainerPools( regionId,
                                                                                           replicaCount,
                                                                                           isFirstOperator,
                                                                                           operatorDef );

                final KVStoreContext[] kvStoreContexts = createKvStoreContexts( regionId, replicaCount, operatorDef );

                final boolean isLastOperator = operatorIndex == ( operatorCount - 1 );
                final Supplier<TuplesImpl>[] outputSuppliers = createOutputSuppliers( regionId, replicaCount, isLastOperator, operatorDef );

                for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
                {
                    operatorReplicas[ replicaIndex ][ operatorIndex ] = new OperatorReplica( pipelineReplicaIds[ replicaIndex ],
                                                                                             operatorDef,
                                                                                             tupleQueueContexts[ replicaIndex ],
                                                                                             kvStoreContexts[ replicaIndex ],
                                                                                             drainerPools[ replicaIndex ],
                                                                                             outputSuppliers[ replicaIndex ] );
                }
            }

            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                LOGGER.info( "Creating pipeline instance for regionId={} replicaIndex={} pipelineId={}",
                             regionId,
                             replicaIndex,
                             pipelineId );
                final OperatorReplica[] pipelineOperatorReplicas = operatorReplicas[ replicaIndex ];
                final OperatorType regionType =
                        pipelineId == 0 ? regionConfig.getRegionDef().getRegionType() : operatorDefs[ 0 ].operatorType();
                final TupleQueueContext pipelineTupleQueueContext = createPipelineTupleQueueContext( flow,
                                                                                                     regionId,
                                                                                                     replicaIndex,
                                                                                                     regionType,
                                                                                                     pipelineOperatorReplicas );

                pipelineReplicas[ pipelineId ][ replicaIndex ] = new PipelineReplica( config,
                                                                                      pipelineReplicaIds[ replicaIndex ],
                                                                                      pipelineOperatorReplicas,
                                                                                      pipelineTupleQueueContext );
            }
        }

        final Region region = new Region( regionConfig, pipelineReplicas );
        regions.put( regionId, region );
        return region;
    }

    @Override
    public void releaseRegion ( final int regionId )
    {
        final Region region = regions.remove( regionId );
        checkArgument( region != null, "Region %s not found to release", regionId );

        final RegionConfig regionConfig = region.getConfig();
        final int replicaCount = regionConfig.getReplicaCount();
        final int pipelineCount = regionConfig.getPipelineStartIndices().size();

        for ( int pipelineId = 0; pipelineId < pipelineCount; pipelineId++ )
        {
            final PipelineReplica[] pipelineReplicas = region.getPipelineReplicas( pipelineId );
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                final PipelineReplica pipelineReplica = pipelineReplicas[ replicaIndex ];

                final TupleQueueContext selfUpstreamTupleQueueContext = pipelineReplica.getSelfUpstreamTupleQueueContext();
                if ( selfUpstreamTupleQueueContext instanceof DefaultTupleQueueContext )
                {
                    LOGGER.info( "Releasing default tuple queue context of pipeline {}", pipelineReplica.id() );
                    final OperatorDef[] operatorDefs = regionConfig.getOperatorDefs( pipelineId );
                    final String operatorId = operatorDefs[ 0 ].id();
                    checkState( tupleQueueContextManager.releaseDefaultTupleQueueContext( regionId, replicaIndex, operatorId ),
                                "Cannot release default tuple queue context of regionId=%s replicaIndex=%s operator=%s",
                                regionId,
                                replicaIndex,
                                operatorId );
                }

                for ( int i = 0; i < pipelineReplica.getOperatorCount(); i++ )
                {
                    final OperatorReplica operator = pipelineReplica.getOperator( i );
                    final OperatorDef operatorDef = operator.getOperatorDef();
                    final TupleQueueContext queue = operator.getQueue();
                    if ( queue instanceof DefaultTupleQueueContext )
                    {
                        LOGGER.info( "Releasing default tuple queue context of Operator {} in Pipeline {} replicaIndex {}",
                                     operatorDef.id(),
                                     pipelineReplica.id(),
                                     replicaIndex );
                        checkState( tupleQueueContextManager.releaseDefaultTupleQueueContext( regionId, replicaIndex, operatorDef.id() ),
                                    "Cannot release default tuple queue context of Operator %s in Pipeline %s replicaIndex %s",
                                    operatorDef.id(),
                                    pipelineReplica.id(),
                                    replicaIndex );
                    }
                    else if ( queue instanceof PartitionedTupleQueueContext && replicaIndex == 0 )
                    {
                        LOGGER.info( "Releasing partitioned tuple queue context of Operator {} in Pipeline {}",
                                     operatorDef.id(),
                                     pipelineReplica.id() );
                        checkState( tupleQueueContextManager.releasePartitionedTupleQueueContexts( regionId, operatorDef.id() ),
                                    "Cannot release partitioned tuple queue context of Operator %s in Pipeline %s",
                                    operatorDef.id(),
                                    pipelineReplica.id() );
                    }

                    if ( replicaIndex == 0 )
                    {
                        if ( operatorDef.operatorType() == STATEFUL )
                        {
                            LOGGER.info( "Releasing default kv store context of Operator {} in Pipeline {}",
                                         operatorDef.id(),
                                         pipelineReplica.id() );
                            checkState( kvStoreContextManager.releaseDefaultKVStoreContext( regionId, operatorDef.id() ),
                                        "Cannot release default kv store context of Operator %s in Pipeline %s",
                                        operatorDef.id(),
                                        pipelineReplica.id() );
                        }
                        else if ( operatorDef.operatorType() == PARTITIONED_STATEFUL )
                        {
                            LOGGER.info( "Releasing partitioned kv store context of Operator {} in Pipeline {}",
                                         operatorDef.id(),
                                         pipelineReplica.id() );
                            checkState( kvStoreContextManager.releasePartitionedKVStoreContext( regionId, operatorDef.id() ),
                                        "Cannot release partitioned kv store context of Operator %s in Pipeline %s",
                                        operatorDef.id(),
                                        pipelineReplica.id() );
                        }
                    }
                }
            }
        }
    }

    private void checkPipelineStartIndices ( final RegionConfig regionConfig )
    {
        final int operatorCount = regionConfig.getRegionDef().getOperatorCount();
        int j = -1;
        final List<Integer> pipelineStartIndices = regionConfig.getPipelineStartIndices();
        for ( int i : pipelineStartIndices )
        {
            checkArgument( i > j, "invalid pipeline indices: %s for region %s", pipelineStartIndices, regionConfig.getRegionId() );
            j = i;
            checkArgument( i < operatorCount,
                           "invalid pipeline indices: %s for region %s",
                           pipelineStartIndices,
                           regionConfig.getRegionId() );
        }
    }

    private PipelineReplicaId[] createPipelineReplicaIds ( final int regionId, final int replicaCount, final int pipelineId )
    {
        final PipelineReplicaId[] pipelineReplicaIds = new PipelineReplicaId[ replicaCount ];
        for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
        {
            pipelineReplicaIds[ replicaIndex ] = new PipelineReplicaId( new PipelineId( regionId, pipelineId ), replicaIndex );
        }
        return pipelineReplicaIds;
    }

    private TupleQueueContext[] createTupleQueueContexts ( final FlowDef flow,
                                                           final int regionId,
                                                           final int replicaCount,
                                                           final boolean isFirstOperator,
                                                           final OperatorDef operatorDef )
    {
        final String operatorId = operatorDef.id();
        final TupleQueueContext[] tupleQueueContexts;

        if ( flow.getUpstreamConnections( operatorId ).isEmpty() )
        {
            LOGGER.info( "Creating {} for regionId={} operatorId={}", EmptyTupleQueueContext.class.getSimpleName(), regionId, operatorId );
            tupleQueueContexts = new TupleQueueContext[ replicaCount ];
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                tupleQueueContexts[ replicaIndex ] = new EmptyTupleQueueContext( operatorId, operatorDef.inputPortCount() );
            }
        }
        else if ( operatorDef.operatorType() == PARTITIONED_STATEFUL )
        {
            LOGGER.info( "Creating {} for regionId={} operatorId={}",
                         PartitionedTupleQueueContext.class.getSimpleName(),
                         regionId,
                         operatorId );
            tupleQueueContexts = tupleQueueContextManager.createPartitionedTupleQueueContext( regionId, replicaCount, operatorDef );
        }
        else
        {
            final ThreadingPreference threadingPreference = isFirstOperator ? MULTI_THREADED : SINGLE_THREADED;
            LOGGER.info( "Creating {} {} for regionId={} operatorId={}",
                         threadingPreference,
                         DefaultTupleQueueContext.class.getSimpleName(),
                         regionId,
                         operatorId );
            tupleQueueContexts = new TupleQueueContext[ replicaCount ];
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                tupleQueueContexts[ replicaIndex ] = tupleQueueContextManager.createDefaultTupleQueueContext( regionId,
                                                                                                              replicaIndex,
                                                                                                              operatorDef,
                                                                                                              threadingPreference );
            }
        }

        return tupleQueueContexts;
    }

    private TupleQueueDrainerPool[] createTupleQueueDrainerPools ( final int regionId,
                                                                   final int replicaCount,
                                                                   final boolean isFirstOperator,
                                                                   final OperatorDef operatorDef )
    {
        final String operatorId = operatorDef.id();
        final TupleQueueDrainerPool[] drainerPools = new TupleQueueDrainerPool[ replicaCount ];
        if ( isFirstOperator )
        {
            LOGGER.info( "Creating {} for regionId={} operatorId={}",
                         BlockingTupleQueueDrainerPool.class.getSimpleName(),
                         regionId,
                         operatorId );
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                drainerPools[ replicaIndex ] = new BlockingTupleQueueDrainerPool( config, operatorDef );
            }
        }
        else
        {
            LOGGER.info( "Creating {} for regionId={} operatorId={}",
                         NonBlockingTupleQueueDrainerPool.class.getSimpleName(),
                         regionId,
                         operatorId );
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                drainerPools[ replicaIndex ] = new NonBlockingTupleQueueDrainerPool( config, operatorDef );
            }
        }
        return drainerPools;
    }

    private KVStoreContext[] createKvStoreContexts ( final int regionId, final int replicaCount, final OperatorDef operatorDef )
    {
        final String operatorId = operatorDef.id();
        final KVStoreContext[] kvStoreContexts;
        if ( operatorDef.operatorType() == STATELESS )
        {
            LOGGER.info( "Creating {} for regionId={} operatorId={}", EmptyKVStoreContext.class.getSimpleName(), regionId, operatorId );
            kvStoreContexts = new KVStoreContext[ replicaCount ];
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {

                kvStoreContexts[ replicaIndex ] = new EmptyKVStoreContext( operatorId );
            }
        }
        else if ( operatorDef.operatorType() == PARTITIONED_STATEFUL )
        {
            LOGGER.info( "Creating {} for regionId={} operatorId={}",
                         PartitionedKVStoreContext.class.getSimpleName(),
                         regionId,
                         operatorId );
            kvStoreContexts = kvStoreContextManager.createPartitionedKVStoreContexts( regionId, replicaCount, operatorId );
        }
        else
        {
            LOGGER.info( "Creating {} for regionId={} operatorId={}", DefaultKVStoreContext.class.getSimpleName(), regionId, operatorId );
            checkArgument( replicaCount == 1, "invalid replica count for operator %s in region %s", operatorDef.id(), regionId );
            kvStoreContexts = new KVStoreContext[ 1 ];
            kvStoreContexts[ 0 ] = kvStoreContextManager.createDefaultKVStoreContext( regionId, operatorId );
        }
        return kvStoreContexts;
    }

    private Supplier<TuplesImpl>[] createOutputSuppliers ( final int regionId,
                                                           final int replicaCount,
                                                           final boolean isLastOperator,
                                                           final OperatorDef operatorDef )
    {
        final String operatorId = operatorDef.id();
        final Supplier<TuplesImpl>[] outputSuppliers = new Supplier[ replicaCount ];
        if ( isLastOperator )
        {
            LOGGER.info( "Creating {} for last operator of regionId={} operatorId={}",
                         NonCachedTuplesImplSupplier.class.getSimpleName(),
                         regionId,
                         operatorId );
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                outputSuppliers[ replicaIndex ] = new NonCachedTuplesImplSupplier( operatorDef.outputPortCount() );
            }
        }
        else
        {
            LOGGER.info( "Creating {} for regionId={} operatorId={}",
                         CachedTuplesImplSupplier.class.getSimpleName(),
                         regionId,
                         operatorId );
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                outputSuppliers[ replicaIndex ] = new CachedTuplesImplSupplier( operatorDef.outputPortCount() );
            }
        }
        return outputSuppliers;
    }

    private TupleQueueContext createPipelineTupleQueueContext ( final FlowDef flow,
                                                                final int regionId,
                                                                final int replicaIndex,
                                                                final OperatorType regionType,
                                                                final OperatorReplica[] pipelineOperatorReplicas )
    {
        final OperatorDef firstOperatorDef = pipelineOperatorReplicas[ 0 ].getOperatorDef();
        if ( flow.getUpstreamConnections( firstOperatorDef.id() ).isEmpty() )
        {
            LOGGER.info( "Creating {} for pipeline tuple queue context of regionId={} as pipeline has no input port",
                         EmptyTupleQueueContext.class.getSimpleName(),
                         regionId );
            return new EmptyTupleQueueContext( firstOperatorDef.id(), firstOperatorDef.inputPortCount() );
        }
        else if ( regionType == PARTITIONED_STATEFUL )
        {
            if ( firstOperatorDef.operatorType() == PARTITIONED_STATEFUL )
            {
                LOGGER.info( "Creating {} for pipeline tuple queue context of regionId={} for pipeline operator={}",
                             DefaultTupleQueueContext.class.getSimpleName(),
                             regionId,
                             firstOperatorDef.id() );
                return tupleQueueContextManager.createDefaultTupleQueueContext( regionId, replicaIndex, firstOperatorDef, MULTI_THREADED );
            }
            else
            {
                LOGGER.info( "Creating {} for pipeline tuple queue context of regionId={} as first operator is {}",
                             EmptyTupleQueueContext.class.getSimpleName(),
                             regionId,
                             firstOperatorDef.operatorType() );
                return new EmptyTupleQueueContext( firstOperatorDef.id(), firstOperatorDef.inputPortCount() );
            }
        }
        else
        {
            LOGGER.info( "Creating {} for pipeline tuple queue context of regionId={}",
                         EmptyTupleQueueContext.class.getSimpleName(),
                         regionId );
            return new EmptyTupleQueueContext( firstOperatorDef.id(), firstOperatorDef.inputPortCount() );
        }
    }

}
