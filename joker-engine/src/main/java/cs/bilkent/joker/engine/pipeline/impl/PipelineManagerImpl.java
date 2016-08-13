package cs.bilkent.joker.engine.pipeline.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.partition.PartitionKeyFunction;
import cs.bilkent.joker.engine.partition.PartitionKeyFunctionFactory;
import cs.bilkent.joker.engine.partition.PartitionService;
import cs.bilkent.joker.engine.pipeline.DownstreamTupleSender;
import cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.COMPLETING;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.INITIAL;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.RUNNING;
import cs.bilkent.joker.engine.pipeline.Pipeline;
import cs.bilkent.joker.engine.pipeline.PipelineId;
import cs.bilkent.joker.engine.pipeline.PipelineManager;
import cs.bilkent.joker.engine.pipeline.PipelineReplica;
import cs.bilkent.joker.engine.pipeline.PipelineReplicaRunner;
import cs.bilkent.joker.engine.pipeline.SupervisorNotifier;
import cs.bilkent.joker.engine.pipeline.UpstreamConnectionStatus;
import static cs.bilkent.joker.engine.pipeline.UpstreamConnectionStatus.ACTIVE;
import static cs.bilkent.joker.engine.pipeline.UpstreamConnectionStatus.CLOSED;
import static cs.bilkent.joker.engine.pipeline.UpstreamConnectionStatus.NO_CONNECTION;
import cs.bilkent.joker.engine.pipeline.UpstreamContext;
import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.CompositeDownstreamTupleSender;
import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.DownstreamTupleSender1;
import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.DownstreamTupleSender2;
import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.DownstreamTupleSender3;
import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.DownstreamTupleSender4;
import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.DownstreamTupleSenderN;
import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.PartitionedDownstreamTupleSender1;
import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.PartitionedDownstreamTupleSender2;
import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.PartitionedDownstreamTupleSender3;
import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.PartitionedDownstreamTupleSender4;
import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.PartitionedDownstreamTupleSenderN;
import cs.bilkent.joker.engine.region.Region;
import cs.bilkent.joker.engine.region.RegionConfig;
import cs.bilkent.joker.engine.region.RegionDef;
import cs.bilkent.joker.engine.region.RegionManager;
import cs.bilkent.joker.engine.supervisor.Supervisor;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.Port;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;
import cs.bilkent.joker.utils.Pair;
import static java.lang.Math.min;

public class PipelineManagerImpl implements PipelineManager
{

    private static final Logger LOGGER = LoggerFactory.getLogger( PipelineManagerImpl.class );

    private static final int DOWNSTREAM_TUPLE_SENDER_CONSTRUCTOR_COUNT = 5;


    private final JokerConfig jokerConfig;

    private final RegionManager regionManager;

    private final PartitionService partitionService;

    private final PartitionKeyFunctionFactory partitionKeyFunctionFactory;

    private final Map<PipelineId, Pipeline> pipelines = new LinkedHashMap<>();

    private final BiFunction<List<Pair<Integer, Integer>>, TupleQueueContext, DownstreamTupleSender>[]
            defaultDownstreamTupleSenderConstructors = new BiFunction[ 6 ];

    private final Function6<List<Pair<Integer, Integer>>, Integer, int[], TupleQueueContext[], PartitionKeyFunction,
                                   DownstreamTupleSender>[] partitionedDownstreamTupleSenderConstructors = new Function6[ 6 ];

    @Inject
    public PipelineManagerImpl ( final JokerConfig jokerConfig,
                                 final RegionManager regionManager,
                                 final PartitionService partitionService,
                                 final PartitionKeyFunctionFactory partitionKeyFunctionFactory )
    {
        this.jokerConfig = jokerConfig;
        this.regionManager = regionManager;
        this.partitionService = partitionService;
        this.partitionKeyFunctionFactory = partitionKeyFunctionFactory;
        createDownstreamTupleSenderFactories( partitionService );
    }

    public Map<PipelineId, Pipeline> getPipelines ()
    {
        return new LinkedHashMap<>( pipelines );
    }

    private void createDownstreamTupleSenderFactories ( final PartitionService partitionService )
    {
        defaultDownstreamTupleSenderConstructors[ 1 ] = ( pairs, tupleQueueContext ) ->
        {
            final Pair<Integer, Integer> pair1 = pairs.get( 0 );
            return new DownstreamTupleSender1( pair1._1, pair1._2, tupleQueueContext );
        };
        defaultDownstreamTupleSenderConstructors[ 2 ] = ( pairs, tupleQueueContext ) ->
        {
            final Pair<Integer, Integer> pair1 = pairs.get( 0 );
            final Pair<Integer, Integer> pair2 = pairs.get( 1 );
            return new DownstreamTupleSender2( pair1._1, pair1._2, pair2._1, pair2._2, tupleQueueContext );
        };
        defaultDownstreamTupleSenderConstructors[ 3 ] = ( pairs, tupleQueueContext ) ->
        {
            final Pair<Integer, Integer> pair1 = pairs.get( 0 );
            final Pair<Integer, Integer> pair2 = pairs.get( 1 );
            final Pair<Integer, Integer> pair3 = pairs.get( 2 );
            return new DownstreamTupleSender3( pair1._1, pair1._2, pair2._1, pair2._2, pair3._1, pair3._2, tupleQueueContext );
        };
        defaultDownstreamTupleSenderConstructors[ 4 ] = ( pairs, tupleQueueContext ) ->
        {
            final Pair<Integer, Integer> pair1 = pairs.get( 0 );
            final Pair<Integer, Integer> pair2 = pairs.get( 1 );
            final Pair<Integer, Integer> pair3 = pairs.get( 2 );
            final Pair<Integer, Integer> pair4 = pairs.get( 3 );
            return new DownstreamTupleSender4( pair1._1,
                                               pair1._2,
                                               pair2._1,
                                               pair2._2,
                                               pair3._1,
                                               pair3._2,
                                               pair4._1,
                                               pair4._2,
                                               tupleQueueContext );
        };
        defaultDownstreamTupleSenderConstructors[ 5 ] = ( pairs, tupleQueueContext ) ->
        {
            final int[] sourcePorts = new int[ pairs.size() ];
            final int[] destinationPorts = new int[ pairs.size() ];
            copyPorts( pairs, sourcePorts, destinationPorts );
            return new DownstreamTupleSenderN( sourcePorts, destinationPorts, tupleQueueContext );
        };
        partitionedDownstreamTupleSenderConstructors[ 1 ] = ( pairs, partitionCount, partitionDistribution, tupleQueueContexts,
                                                              partitionKeyFunction ) ->
        {
            final Pair<Integer, Integer> pair1 = pairs.get( 0 );
            return new PartitionedDownstreamTupleSender1( pair1._1,
                                                          pair1._2,
                                                          partitionService.getPartitionCount(),
                                                          partitionDistribution,
                                                          tupleQueueContexts,
                                                          partitionKeyFunction );
        };
        partitionedDownstreamTupleSenderConstructors[ 2 ] = ( pairs, partitionCount, partitionDistribution, tupleQueueContexts,
                                                              partitionKeyFunction ) ->
        {
            final Pair<Integer, Integer> pair1 = pairs.get( 0 );
            final Pair<Integer, Integer> pair2 = pairs.get( 1 );
            return new PartitionedDownstreamTupleSender2( pair1._1,
                                                          pair1._2,
                                                          pair2._1,
                                                          pair2._2,
                                                          partitionService.getPartitionCount(),
                                                          partitionDistribution,
                                                          tupleQueueContexts,
                                                          partitionKeyFunction );
        };
        partitionedDownstreamTupleSenderConstructors[ 3 ] = ( pairs, partitionCount, partitionDistribution, tupleQueueContexts,
                                                              partitionKeyFunction ) ->
        {
            final Pair<Integer, Integer> pair1 = pairs.get( 0 );
            final Pair<Integer, Integer> pair2 = pairs.get( 1 );
            final Pair<Integer, Integer> pair3 = pairs.get( 2 );
            return new PartitionedDownstreamTupleSender3( pair1._1,
                                                          pair1._2,
                                                          pair2._1,
                                                          pair2._2,
                                                          pair3._1,
                                                          pair3._2,
                                                          partitionService.getPartitionCount(),
                                                          partitionDistribution,
                                                          tupleQueueContexts,
                                                          partitionKeyFunction );
        };
        partitionedDownstreamTupleSenderConstructors[ 4 ] = ( pairs, partitionCount, partitionDistribution, tupleQueueContexts,
                                                              partitionKeyFunction ) ->
        {
            final Pair<Integer, Integer> pair1 = pairs.get( 0 );
            final Pair<Integer, Integer> pair2 = pairs.get( 1 );
            final Pair<Integer, Integer> pair3 = pairs.get( 2 );
            final Pair<Integer, Integer> pair4 = pairs.get( 3 );
            return new PartitionedDownstreamTupleSender4( pair1._1,
                                                          pair1._2,
                                                          pair2._1,
                                                          pair2._2,
                                                          pair3._1,
                                                          pair3._2,
                                                          pair4._1,
                                                          pair4._2,
                                                          partitionService.getPartitionCount(),
                                                          partitionDistribution,
                                                          tupleQueueContexts,
                                                          partitionKeyFunction );
        };
        partitionedDownstreamTupleSenderConstructors[ 5 ] = ( pairs, partitionCount, partitionDistribution, tupleQueueContexts,
                                                              partitionKeyFunction ) ->
        {
            final int[] sourcePorts = new int[ pairs.size() ];
            final int[] destinationPorts = new int[ pairs.size() ];
            copyPorts( pairs, sourcePorts, destinationPorts );
            return new PartitionedDownstreamTupleSenderN( sourcePorts,
                                                          destinationPorts,
                                                          partitionService.getPartitionCount(),
                                                          partitionDistribution,
                                                          tupleQueueContexts,
                                                          partitionKeyFunction );
        };
    }

    @Override
    public List<Pipeline> createPipelines ( final Supervisor supervisor, final FlowDef flow, final List<RegionConfig> regionConfigs )
    {
        createPipelines( regionConfigs );
        createPipelineReplicas( flow, regionConfigs );
        createDownstreamTupleSenders( flow );
        createUpstreamContexts( flow );
        createPipelineReplicaRunners( supervisor );
        return new ArrayList<>( this.pipelines.values() );
    }

    @Override
    public void shutdown ()
    {
        final Multimap<Integer, PipelineId> regionIds = HashMultimap.create();
        for ( PipelineId id : pipelines.keySet() )
        {
            regionIds.put( id.regionId, id );
        }

        for ( Integer regionId : new ArrayList<>( regionIds.keySet() ) )
        {
            for ( PipelineId pipelineId : regionIds.removeAll( regionId ) )
            {
                LOGGER.info( "Removing Pipeline {}", pipelineId );
                pipelines.remove( pipelineId );
            }

            regionManager.releaseRegion( regionId );
        }
    }

    private void createPipelines ( final Collection<RegionConfig> regionConfigs )
    {
        final List<RegionConfig> l = new ArrayList<>( regionConfigs );
        Collections.sort( l, ( r1, r2 ) -> Integer.compare( r1.getRegionId(), r2.getRegionId() ) );

        for ( RegionConfig regionConfig : l )
        {
            LOGGER.info( "Initializing regionId={} with {} pipelines ( {} ) and {} replicas",
                         regionConfig.getRegionId(),
                         regionConfig.getPipelineCount(),
                         regionConfig.getPipelineStartIndices(),
                         regionConfig.getReplicaCount() );
            for ( int pipelineId = 0; pipelineId < regionConfig.getPipelineCount(); pipelineId++ )
            {
                final PipelineId id = new PipelineId( regionConfig.getRegionId(), pipelineId );
                final Pipeline prev = pipelines.put( id, new Pipeline( id, regionConfig ) );
                checkState( prev == null, "there are multiple pipelines with same id: %s", id );
            }
        }
    }

    private void createPipelineReplicas ( final FlowDef flow, final Collection<RegionConfig> regionConfigs )
    {
        for ( RegionConfig regionConfig : regionConfigs )
        {
            final Region region = regionManager.createRegion( flow, regionConfig );
            for ( int pipelineId = 0; pipelineId < regionConfig.getPipelineCount(); pipelineId++ )
            {
                final PipelineReplica[] pipelineReplicas = region.getPipelineReplicas( pipelineId );
                final PipelineId id = new PipelineId( regionConfig.getRegionId(), pipelineId );
                final Pipeline pipeline = pipelines.get( id );
                for ( int replicaIndex = 0; replicaIndex < pipeline.getReplicaCount(); replicaIndex++ )
                {
                    pipeline.setPipelineReplica( replicaIndex, pipelineReplicas[ replicaIndex ] );
                }
            }

            LOGGER.info( "regionId={} is created", regionConfig.getRegionId() );
        }
    }

    private void createDownstreamTupleSenders ( final FlowDef flow )
    {
        for ( Pipeline pipeline : pipelines.values() )
        {
            createDownstreamTupleSenders( flow, pipeline );
        }
    }

    private void createDownstreamTupleSenders ( final FlowDef flow, final Pipeline pipeline )
    {
        final OperatorDef lastOperator = pipeline.getLastOperatorDef();
        final Map<String, List<Pair<Integer, Integer>>> connectionsByOperatorId = getDownstreamConnectionsByOperatorId( flow,
                                                                                                                        lastOperator );
        LOGGER.info( "Pipeline {} with last operator {} has following downstream connectionsByOperatorId: {}",
                     pipeline.getId(),
                     lastOperator.id(),
                     connectionsByOperatorId );

        for ( int replicaIndex = 0; replicaIndex < pipeline.getReplicaCount(); replicaIndex++ )
        {
            final DownstreamTupleSender[] senders = new DownstreamTupleSender[ connectionsByOperatorId.size() ];
            int i = 0;
            for ( Entry<String, List<Pair<Integer, Integer>>> e : connectionsByOperatorId.entrySet() )
            {
                final String downstreamOperatorId = e.getKey();
                final List<Pair<Integer, Integer>> pairs = e.getValue();
                final OperatorDef downstreamOperator = flow.getOperator( downstreamOperatorId );
                final Pipeline downstreamPipeline = getPipeline( downstreamOperator, 0 );
                final RegionDef downstreamRegionDef = downstreamPipeline.getRegionDef();
                final TupleQueueContext[] pipelineTupleQueueContexts = getPipelineTupleQueueContexts( downstreamOperator );
                final int j = min( pairs.size(), DOWNSTREAM_TUPLE_SENDER_CONSTRUCTOR_COUNT );

                if ( pipeline.getId().regionId == downstreamPipeline.getId().regionId )
                {
                    final TupleQueueContext pipelineTupleQueueContext = pipelineTupleQueueContexts[ replicaIndex ];
                    senders[ i ] = defaultDownstreamTupleSenderConstructors[ j ].apply( pairs, pipelineTupleQueueContext );
                }
                else if ( downstreamRegionDef.getRegionType() == PARTITIONED_STATEFUL )
                {
                    final int[] partitionDistribution = getPartitionDistribution( downstreamOperator );
                    final PartitionKeyFunction partitionKeyFunction = partitionKeyFunctionFactory.createPartitionKeyFunction(
                            downstreamRegionDef.getPartitionFieldNames() );
                    senders[ i ] = partitionedDownstreamTupleSenderConstructors[ j ].apply( pairs,
                                                                                            partitionService.getPartitionCount(),
                                                                                            partitionDistribution,
                                                                                            pipelineTupleQueueContexts,
                                                                                            partitionKeyFunction );
                }
                else if ( downstreamRegionDef.getRegionType() == STATELESS )
                {
                    TupleQueueContext pipelineTupleQueueContext = null;
                    if ( pipeline.getReplicaCount() == downstreamPipeline.getReplicaCount() )
                    {
                        pipelineTupleQueueContext = pipelineTupleQueueContexts[ replicaIndex ];
                    }
                    else if ( downstreamPipeline.getReplicaCount() == 1 )
                    {
                        pipelineTupleQueueContext = pipelineTupleQueueContexts[ 0 ];

                    }

                    if ( pipelineTupleQueueContext != null )
                    {
                        senders[ i ] = defaultDownstreamTupleSenderConstructors[ j ].apply( pairs, pipelineTupleQueueContext );
                    }
                    else
                    {
                        throw new IllegalStateException( "incompatible replica counts! pipeline: " + pipeline.getId() + " replica count: "
                                                         + pipeline.getReplicaCount() + " downstream pipeline: "
                                                         + downstreamPipeline.getId() + " downstream pipeline replica count: "
                                                         + downstreamPipeline.getReplicaCount() );
                    }
                }
                else if ( downstreamRegionDef.getRegionType() == STATEFUL )
                {
                    final int l = pipelineTupleQueueContexts.length;
                    checkState( l == 1, "Operator %s can not have %s replicas", downstreamOperatorId, l );
                    final TupleQueueContext pipelineTupleQueueContext = pipelineTupleQueueContexts[ 0 ];
                    senders[ i ] = defaultDownstreamTupleSenderConstructors[ j ].apply( pairs, pipelineTupleQueueContext );
                }
                else
                {
                    throw new IllegalStateException( "invalid region type: " + downstreamRegionDef.getRegionType() );
                }

                i++;
            }

            DownstreamTupleSender sender;
            if ( i == 0 )
            {
                sender = new NopDownstreamTupleSender();
            }
            else if ( i == 1 )
            {
                sender = senders[ 0 ];
            }
            else
            {
                sender = new CompositeDownstreamTupleSender( senders );
            }

            LOGGER.info( "Created {} for Pipeline {} replica index {}", sender.getClass().getSimpleName(), pipeline.getId(), replicaIndex );

            pipeline.setDownstreamTupleSender( replicaIndex, sender );
        }
    }

    private Map<String, List<Pair<Integer, Integer>>> getDownstreamConnectionsByOperatorId ( final FlowDef flow,
                                                                                             final OperatorDef operator )
    {
        final Map<String, List<Pair<Integer, Integer>>> result = new LinkedHashMap<>();
        final Map<Port, Collection<Port>> connections = flow.getDownstreamConnections( operator.id() );
        final Set<String> downstreamOperatorIds = new HashSet<>();
        for ( Collection<Port> c : connections.values() )
        {
            downstreamOperatorIds.addAll( c.stream().map( p -> p.operatorId ).collect( Collectors.toSet() ) );
        }

        final List<String> downstreamOperatorIdsSorted = new ArrayList<>( downstreamOperatorIds );
        Collections.sort( downstreamOperatorIdsSorted );

        for ( String downstreamOperatorId : downstreamOperatorIdsSorted )
        {
            for ( Entry<Port, Collection<Port>> e : connections.entrySet() )
            {
                final Port upstreamPort = e.getKey();
                e.getValue()
                 .stream()
                 .filter( downstreamPort -> downstreamPort.operatorId.equals( downstreamOperatorId ) )
                 .forEach( downstreamPort -> result.computeIfAbsent( downstreamOperatorId, s -> new ArrayList<>() )
                                                   .add( Pair.of( upstreamPort.portIndex, downstreamPort.portIndex ) ) );
            }
        }

        for ( List<Pair<Integer, Integer>> pairs : result.values() )
        {
            Collections.sort( pairs, ( p1, p2 ) ->
            {
                final int r = Integer.compare( p1._1, p2._1 );
                return r == 0 ? Integer.compare( p1._2, p2._2 ) : r;
            } );
        }

        return result;
    }

    private int[] getPartitionDistribution ( final OperatorDef operator )
    {
        final Pipeline pipeline = getPipeline( operator, 0 );
        return partitionService.getOrCreatePartitionDistribution( pipeline.getId().regionId, pipeline.getReplicaCount() );
    }

    private TupleQueueContext[] getPipelineTupleQueueContexts ( final OperatorDef operator )
    {
        final Pipeline pipeline = getPipeline( operator, 0 );
        final int replicaCount = pipeline.getReplicaCount();
        final TupleQueueContext[] tupleQueueContexts = new TupleQueueContext[ replicaCount ];
        for ( int i = 0; i < replicaCount; i++ )
        {
            tupleQueueContexts[ i ] = pipeline.getPipelineReplica( i ).getUpstreamTupleQueueContext();
        }

        return tupleQueueContexts;
    }

    private void copyPorts ( final List<Pair<Integer, Integer>> pairs, final int[] sourcePorts, final int[] destinationPorts )
    {
        for ( int i = 0; i < pairs.size(); i++ )
        {
            sourcePorts[ i ] = pairs.get( i )._1;
            destinationPorts[ i ] = pairs.get( i )._2;
        }
    }

    private void createUpstreamContexts ( final FlowDef flow )
    {
        for ( Pipeline pipeline : pipelines.values() )
        {
            UpstreamContext upstreamContext;
            final OperatorDef firstOperator = pipeline.getFirstOperatorDef();
            if ( firstOperator.inputPortCount() == 0 )
            {
                upstreamContext = new UpstreamContext( 0, new UpstreamConnectionStatus[] {} );
            }
            else
            {
                final UpstreamConnectionStatus[] statuses = new UpstreamConnectionStatus[ firstOperator.inputPortCount() ];
                final Map<Port, Collection<Port>> upstreamConnections = flow.getUpstreamConnections( firstOperator.id() );
                for ( int portIndex = 0; portIndex < firstOperator.inputPortCount(); portIndex++ )
                {
                    UpstreamConnectionStatus status = CLOSED;
                    final Collection<Port> upstreamPorts = upstreamConnections.get( new Port( firstOperator.id(), portIndex ) );
                    if ( upstreamPorts == null || upstreamPorts.isEmpty() )
                    {
                        status = NO_CONNECTION;
                    }
                    else
                    {
                        for ( Port upstreamPort : upstreamPorts )
                        {
                            final OperatorDef upstreamOperator = flow.getOperator( upstreamPort.operatorId );
                            final Pipeline upstreamPipeline = getPipeline( upstreamOperator, -1 );
                            final int operatorIndex = upstreamPipeline.getOperatorIndex( upstreamOperator );
                            checkState( ( upstreamPipeline.getOperatorCount() - 1 ) == operatorIndex,
                                        "Operator %s is supposed to be last operator of pipeline %s but it is on index %s",
                                        upstreamOperator.id(),
                                        upstreamPipeline.getId(),
                                        operatorIndex );
                            final OperatorReplicaStatus pipelineStatus = upstreamPipeline.getPipelineStatus();
                            if ( pipelineStatus == INITIAL || pipelineStatus == RUNNING || pipelineStatus == COMPLETING )
                            {
                                status = ACTIVE;
                                break;
                            }
                        }
                    }
                    statuses[ portIndex ] = status;
                }
                upstreamContext = new UpstreamContext( 0, statuses );
            }

            LOGGER.info( "Created {} for pipeline {}", upstreamContext, pipeline.getId() );
            pipeline.setUpstreamContext( upstreamContext );
        }
    }

    private void createPipelineReplicaRunners ( final Supervisor supervisor )
    {
        for ( Pipeline pipeline : pipelines.values() )
        {
            for ( int replicaIndex = 0; replicaIndex < pipeline.getReplicaCount(); replicaIndex++ )
            {
                final PipelineReplica pipelineReplica = pipeline.getPipelineReplica( replicaIndex );
                final SupervisorNotifier supervisorNotifier = new SupervisorNotifier( supervisor, pipelineReplica );
                final PipelineReplicaRunner runner = new PipelineReplicaRunner( jokerConfig,
                                                                                pipelineReplica,
                                                                                supervisor,
                                                                                supervisorNotifier,
                                                                                pipeline.getDownstreamTupleSender( replicaIndex ) );
                pipeline.setPipelineReplicaRunner( replicaIndex, runner );
                LOGGER.info( "Created runner for pipeline replica: {}", pipelineReplica.id() );
            }
        }
    }

    private Pipeline getPipeline ( final OperatorDef operator, final int operatorIndex )
    {
        for ( Pipeline pipeline : pipelines.values() )
        {
            final int i = pipeline.getOperatorIndex( operator );
            if ( i == -1 )
            {
                continue;
            }
            else if ( operatorIndex == -1 )
            {
                return pipeline;
            }

            checkArgument( operatorIndex == i,
                           "Operator {} is expected to be at %s'th index of pipeline %s but it is at %s'th index",
                           operator.id(),
                           pipeline.getId(),
                           i );
            return pipeline;
        }

        throw new IllegalArgumentException( "Operator " + operator.id() + " is not found in the pipelines" );
    }

    @FunctionalInterface
    private interface Function6<T1, T2, T3, T4, T5, T6>
    {
        T6 apply ( T1 t1, T2 t2, T3 t3, T4 t4, T5 t5 );
    }


    static class NopDownstreamTupleSender implements DownstreamTupleSender
    {

        @Override
        public Future<Void> send ( final TuplesImpl tuples )
        {
            return null;
        }

    }

}
