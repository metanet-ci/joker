package cs.bilkent.zanza.engine.region.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.zanza.engine.config.ThreadingPreference;
import static cs.bilkent.zanza.engine.config.ThreadingPreference.MULTI_THREADED;
import static cs.bilkent.zanza.engine.config.ThreadingPreference.SINGLE_THREADED;
import cs.bilkent.zanza.engine.config.ZanzaConfig;
import cs.bilkent.zanza.engine.kvstore.KVStoreContext;
import cs.bilkent.zanza.engine.kvstore.KVStoreContextManager;
import cs.bilkent.zanza.engine.kvstore.impl.DefaultKVStoreContext;
import cs.bilkent.zanza.engine.kvstore.impl.EmptyKVStoreContext;
import cs.bilkent.zanza.engine.kvstore.impl.PartitionedKVStoreContext;
import cs.bilkent.zanza.engine.pipeline.OperatorInstance;
import cs.bilkent.zanza.engine.pipeline.PipelineId;
import cs.bilkent.zanza.engine.pipeline.PipelineInstance;
import cs.bilkent.zanza.engine.pipeline.PipelineInstanceId;
import cs.bilkent.zanza.engine.pipeline.impl.tuplesupplier.CachedTuplesImplSupplier;
import cs.bilkent.zanza.engine.pipeline.impl.tuplesupplier.NonCachedTuplesImplSupplier;
import cs.bilkent.zanza.engine.region.RegionInstance;
import cs.bilkent.zanza.engine.region.RegionManager;
import cs.bilkent.zanza.engine.region.RegionRuntimeConfig;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContextManager;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainerPool;
import cs.bilkent.zanza.engine.tuplequeue.impl.context.DefaultTupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.impl.context.EmptyTupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.impl.context.PartitionedTupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.impl.drainer.pool.BlockingTupleQueueDrainerPool;
import cs.bilkent.zanza.engine.tuplequeue.impl.drainer.pool.NonBlockingTupleQueueDrainerPool;
import cs.bilkent.zanza.flow.FlowDefinition;
import cs.bilkent.zanza.flow.OperatorDefinition;
import cs.bilkent.zanza.operator.impl.TuplesImpl;
import static cs.bilkent.zanza.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.zanza.operator.spec.OperatorType.STATELESS;

@Singleton
@NotThreadSafe
public class RegionManagerImpl implements RegionManager
{

    private static final Logger LOGGER = LoggerFactory.getLogger( RegionManagerImpl.class );

    private final ZanzaConfig config;

    private final KVStoreContextManager kvStoreContextManager;

    private final TupleQueueContextManager tupleQueueContextManager;

    private final Map<Integer, RegionInstance> regions = new HashMap<>();

    @Inject
    public RegionManagerImpl ( final ZanzaConfig config,
                               final KVStoreContextManager kvStoreContextManager,
                               final TupleQueueContextManager tupleQueueContextManager )
    {
        this.config = config;
        this.kvStoreContextManager = kvStoreContextManager;
        this.tupleQueueContextManager = tupleQueueContextManager;
    }

    @Override
    public RegionInstance createRegion ( final FlowDefinition flow, final RegionRuntimeConfig regionConfig )
    {
        checkState( !regions.containsKey( regionConfig.getRegionId() ) );
        checkPipelineStartIndices( regionConfig );

        final int regionId = regionConfig.getRegionId();
        final int replicaCount = regionConfig.getReplicaCount();
        final int pipelineCount = regionConfig.getPipelineStartIndices().size();

        LOGGER.info( "Creating components for regionId={} pipelineCount={} replicaCount={}", regionId, pipelineCount, replicaCount );

        final PipelineInstance[][] pipelineInstances = new PipelineInstance[ pipelineCount ][ replicaCount ];

        for ( int pipelineId = 0; pipelineId < pipelineCount; pipelineId++ )
        {

            final OperatorDefinition[] operatorDefinitions = regionConfig.getOperatorDefinitions( pipelineId );
            final int operatorCount = operatorDefinitions.length;
            final OperatorInstance[][] operatorInstances = new OperatorInstance[ replicaCount ][ operatorCount ];
            LOGGER.info( "Initializing pipeline instance for regionId={} pipelineId={} with {} operators",
                         regionId,
                         pipelineId,
                         operatorCount );

            final PipelineInstanceId[] pipelineInstanceIds = createPipelineInstanceIds( regionId, replicaCount, pipelineId );

            for ( int operatorIndex = 0; operatorIndex < operatorCount; operatorIndex++ )
            {
                final OperatorDefinition operatorDefinition = operatorDefinitions[ operatorIndex ];

                final boolean isFirstOperator = operatorIndex == 0;
                final TupleQueueContext[] tupleQueueContexts = createTupleQueueContexts( flow,
                                                                                         regionId,
                                                                                         replicaCount,
                                                                                         isFirstOperator,
                                                                                         operatorDefinition );

                final TupleQueueDrainerPool[] drainerPools = createTupleQueueDrainerPools( regionId,
                                                                                           replicaCount,
                                                                                           isFirstOperator,
                                                                                           operatorDefinition );

                final KVStoreContext[] kvStoreContexts = createKvStoreContexts( regionId, replicaCount, operatorDefinition );

                final boolean isLastOperator = operatorIndex == ( operatorCount - 1 );
                final Supplier<TuplesImpl>[] outputSuppliers = createOutputSuppliers( regionId,
                                                                                      replicaCount,
                                                                                      isLastOperator,
                                                                                      operatorDefinition );

                for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
                {
                    operatorInstances[ replicaIndex ][ operatorIndex ] = new OperatorInstance( pipelineInstanceIds[ replicaIndex ],
                                                                                               operatorDefinition,
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
                final OperatorInstance[] pipelineOperatorInstances = operatorInstances[ replicaIndex ];
                final TupleQueueContext pipelineTupleQueueContext = createPipelineTupleQueueContext( flow,
                                                                                                     regionId,
                                                                                                     regionConfig,
                                                                                                     pipelineOperatorInstances );

                pipelineInstances[ pipelineId ][ replicaIndex ] = new PipelineInstance( config, pipelineInstanceIds[ replicaIndex ],
                                                                                        pipelineOperatorInstances,
                                                                                        pipelineTupleQueueContext );
            }
        }

        final RegionInstance region = new RegionInstance( regionConfig, pipelineInstances );
        regions.put( regionId, region );
        return region;
    }

    private void checkPipelineStartIndices ( final RegionRuntimeConfig regionConfig )
    {
        final int operatorCount = regionConfig.getRegion().getOperators().size();
        int j = -1;
        for ( int i : regionConfig.getPipelineStartIndices() )
        {
            checkArgument( i > j );
            j = i;
            checkArgument( i < operatorCount );
        }
    }

    private PipelineInstanceId[] createPipelineInstanceIds ( final int regionId, final int replicaCount, final int pipelineId )
    {
        final PipelineInstanceId[] pipelineInstanceIds = new PipelineInstanceId[ replicaCount ];
        for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
        {
            pipelineInstanceIds[ replicaIndex ] = new PipelineInstanceId( new PipelineId( regionId, pipelineId ), replicaIndex );
        }
        return pipelineInstanceIds;
    }

    private TupleQueueContext[] createTupleQueueContexts ( final FlowDefinition flow,
                                                           final int regionId,
                                                           final int replicaCount,
                                                           final boolean isFirstOperator,
                                                           final OperatorDefinition operatorDefinition )
    {
        final String operatorId = operatorDefinition.id();
        final TupleQueueContext[] tupleQueueContexts;

        if ( flow.getUpstreamConnections( operatorId ).isEmpty() )
        {
            LOGGER.info( "Creating {} for regionId={} operatorId={}", EmptyTupleQueueContext.class.getSimpleName(), regionId, operatorId );
            tupleQueueContexts = new TupleQueueContext[ replicaCount ];
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                tupleQueueContexts[ replicaIndex ] = new EmptyTupleQueueContext( operatorId, operatorDefinition.inputPortCount() );
            }
        }
        else if ( operatorDefinition.operatorType() == PARTITIONED_STATEFUL )
        {
            LOGGER.info( "Creating {} for regionId={} operatorId={}",
                         PartitionedTupleQueueContext.class.getSimpleName(),
                         regionId,
                         operatorId );
            tupleQueueContexts = tupleQueueContextManager.createPartitionedTupleQueueContext( regionId, replicaCount, operatorDefinition );
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
                                                                                                              operatorDefinition,
                                                                                                              threadingPreference );
            }
        }

        return tupleQueueContexts;
    }

    private TupleQueueDrainerPool[] createTupleQueueDrainerPools ( final int regionId,
                                                                   final int replicaCount,
                                                                   final boolean isFirstOperator,
                                                                   final OperatorDefinition operatorDefinition )
    {
        final String operatorId = operatorDefinition.id();
        final TupleQueueDrainerPool[] drainerPools = new TupleQueueDrainerPool[ replicaCount ];
        if ( isFirstOperator )
        {
            LOGGER.info( "Creating {} for regionId={} operatorId={}",
                         BlockingTupleQueueDrainerPool.class.getSimpleName(),
                         regionId,
                         operatorId );
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                drainerPools[ replicaIndex ] = new BlockingTupleQueueDrainerPool( config, operatorDefinition );
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
                drainerPools[ replicaIndex ] = new NonBlockingTupleQueueDrainerPool( config, operatorDefinition );
            }
        }
        return drainerPools;
    }

    private KVStoreContext[] createKvStoreContexts ( final int regionId,
                                                     final int replicaCount,
                                                     final OperatorDefinition operatorDefinition )
    {
        final String operatorId = operatorDefinition.id();
        final KVStoreContext[] kvStoreContexts;
        if ( operatorDefinition.operatorType() == STATELESS )
        {
            LOGGER.info( "Creating {} for regionId={} operatorId={}", EmptyKVStoreContext.class.getSimpleName(), regionId, operatorId );
            kvStoreContexts = new KVStoreContext[ replicaCount ];
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {

                kvStoreContexts[ replicaIndex ] = new EmptyKVStoreContext( operatorId );
            }
        }
        else if ( operatorDefinition.operatorType() == PARTITIONED_STATEFUL )
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
            checkArgument( replicaCount == 1 );
            kvStoreContexts = new KVStoreContext[ 1 ];
            kvStoreContexts[ 0 ] = kvStoreContextManager.createDefaultKVStoreContext( regionId, operatorId );
        }
        return kvStoreContexts;
    }

    private Supplier<TuplesImpl>[] createOutputSuppliers ( final int regionId,
                                                           final int replicaCount,
                                                           final boolean isLastOperator,
                                                           final OperatorDefinition operatorDefinition )
    {
        final String operatorId = operatorDefinition.id();
        final Supplier<TuplesImpl>[] outputSuppliers = new Supplier[ replicaCount ];
        if ( isLastOperator )
        {
            LOGGER.info( "Creating {} for last operator of regionId={} operatorId={}",
                         NonCachedTuplesImplSupplier.class.getSimpleName(),
                         regionId,
                         operatorId );
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                outputSuppliers[ replicaIndex ] = new NonCachedTuplesImplSupplier( operatorDefinition.outputPortCount() );
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
                outputSuppliers[ replicaIndex ] = new CachedTuplesImplSupplier( operatorDefinition.outputPortCount() );
            }
        }
        return outputSuppliers;
    }

    private TupleQueueContext createPipelineTupleQueueContext ( final FlowDefinition flow,
                                                                final int regionId,
                                                                final RegionRuntimeConfig regionConfig,
                                                                final OperatorInstance[] pipelineOperatorInstances )
    {
        final OperatorDefinition firstOperatorDefinition = pipelineOperatorInstances[ 0 ].getOperatorDefinition();
        if ( flow.getUpstreamConnections( firstOperatorDefinition.id() ).isEmpty() )
        {
            LOGGER.info( "Creating {} for pipeline tuple queue context of regionId={} as pipeline has no input port",
                         EmptyTupleQueueContext.class.getSimpleName(),
                         regionId );
            return new EmptyTupleQueueContext( firstOperatorDefinition.id(), firstOperatorDefinition.inputPortCount() );
        }
        else if ( regionConfig.getRegion().getRegionType() == PARTITIONED_STATEFUL )
        {
            if ( firstOperatorDefinition.operatorType() == PARTITIONED_STATEFUL )
            {
                LOGGER.info( "Creating {} for pipeline tuple queue context of regionId={} for pipeline operator={}",
                             DefaultTupleQueueContext.class.getSimpleName(),
                             regionId,
                             firstOperatorDefinition.id() );
                return tupleQueueContextManager.createDefaultTupleQueueContext( regionId, 0, firstOperatorDefinition, MULTI_THREADED );
            }
            else
            {
                LOGGER.info( "Creating {} for pipeline tuple queue context of regionId={} as pipeline has no input port",
                             EmptyTupleQueueContext.class.getSimpleName(),
                             regionId );
                return new EmptyTupleQueueContext( firstOperatorDefinition.id(), firstOperatorDefinition.inputPortCount() );
            }
        }
        else
        {
            LOGGER.info( "Creating {} for pipeline tuple queue context of regionId={}",
                         EmptyTupleQueueContext.class.getSimpleName(),
                         regionId );
            return new EmptyTupleQueueContext( firstOperatorDefinition.id(), firstOperatorDefinition.inputPortCount() );
        }
    }

}
