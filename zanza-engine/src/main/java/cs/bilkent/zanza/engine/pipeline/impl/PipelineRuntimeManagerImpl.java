package cs.bilkent.zanza.engine.pipeline.impl;

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
import javax.inject.Named;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.zanza.engine.config.ZanzaConfig;
import cs.bilkent.zanza.engine.partition.PartitionKeyFunction;
import cs.bilkent.zanza.engine.partition.PartitionKeyFunctionFactory;
import cs.bilkent.zanza.engine.partition.PartitionService;
import cs.bilkent.zanza.engine.pipeline.DownstreamTupleSender;
import cs.bilkent.zanza.engine.pipeline.OperatorReplicaStatus;
import static cs.bilkent.zanza.engine.pipeline.OperatorReplicaStatus.COMPLETING;
import static cs.bilkent.zanza.engine.pipeline.OperatorReplicaStatus.INITIAL;
import static cs.bilkent.zanza.engine.pipeline.OperatorReplicaStatus.RUNNING;
import cs.bilkent.zanza.engine.pipeline.PipelineId;
import cs.bilkent.zanza.engine.pipeline.PipelineReplica;
import cs.bilkent.zanza.engine.pipeline.PipelineReplicaRunner;
import cs.bilkent.zanza.engine.pipeline.PipelineRuntimeManager;
import cs.bilkent.zanza.engine.pipeline.PipelineRuntimeState;
import cs.bilkent.zanza.engine.pipeline.SupervisorNotifier;
import cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus;
import static cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus.ACTIVE;
import static cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus.CLOSED;
import static cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus.NO_CONNECTION;
import cs.bilkent.zanza.engine.pipeline.UpstreamContext;
import cs.bilkent.zanza.engine.pipeline.impl.downstreamtuplesender.CompositeDownstreamTupleSender;
import cs.bilkent.zanza.engine.pipeline.impl.downstreamtuplesender.DownstreamTupleSender1;
import cs.bilkent.zanza.engine.pipeline.impl.downstreamtuplesender.DownstreamTupleSender2;
import cs.bilkent.zanza.engine.pipeline.impl.downstreamtuplesender.DownstreamTupleSender3;
import cs.bilkent.zanza.engine.pipeline.impl.downstreamtuplesender.DownstreamTupleSender4;
import cs.bilkent.zanza.engine.pipeline.impl.downstreamtuplesender.DownstreamTupleSenderN;
import cs.bilkent.zanza.engine.pipeline.impl.downstreamtuplesender.PartitionedDownstreamTupleSender1;
import cs.bilkent.zanza.engine.pipeline.impl.downstreamtuplesender.PartitionedDownstreamTupleSender2;
import cs.bilkent.zanza.engine.pipeline.impl.downstreamtuplesender.PartitionedDownstreamTupleSender3;
import cs.bilkent.zanza.engine.pipeline.impl.downstreamtuplesender.PartitionedDownstreamTupleSender4;
import cs.bilkent.zanza.engine.pipeline.impl.downstreamtuplesender.PartitionedDownstreamTupleSenderN;
import cs.bilkent.zanza.engine.region.Region;
import cs.bilkent.zanza.engine.region.RegionManager;
import cs.bilkent.zanza.engine.region.RegionRuntimeConfig;
import cs.bilkent.zanza.engine.supervisor.Supervisor;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.flow.FlowDef;
import cs.bilkent.zanza.flow.OperatorDef;
import cs.bilkent.zanza.flow.Port;
import cs.bilkent.zanza.operator.impl.TuplesImpl;
import static cs.bilkent.zanza.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import cs.bilkent.zanza.utils.Pair;
import static java.lang.Math.min;

public class PipelineRuntimeManagerImpl implements PipelineRuntimeManager
{

    private static final Logger LOGGER = LoggerFactory.getLogger( PipelineRuntimeManagerImpl.class );

    private static final int DOWNSTREAM_TUPLE_SENDER_CONSTRUCTOR_COUNT = 5;


    private final ZanzaConfig zanzaConfig;

    private final RegionManager regionManager;

    private final PartitionService partitionService;

    private final PartitionKeyFunctionFactory partitionKeyFunctionFactory;

    private final Map<PipelineId, PipelineRuntimeState> pipelineRuntimeStates = new LinkedHashMap<>();

    private final BiFunction<List<Pair<Port, Port>>, TupleQueueContext, DownstreamTupleSender>[] defaultDownstreamTupleSenderConstructors
            = new BiFunction[ 6 ];

    private final Function6<List<Pair<Port, Port>>, Integer, int[], TupleQueueContext[], PartitionKeyFunction, DownstreamTupleSender>[]
            partitionedDownstreamTupleSenderConstructors = new Function6[ 6 ];

    private final ThreadGroup zanzaThreadGroup;

    @Inject
    public PipelineRuntimeManagerImpl ( final ZanzaConfig zanzaConfig,
                                        final RegionManager regionManager,
                                        final PartitionService partitionService,
                                        final PartitionKeyFunctionFactory partitionKeyFunctionFactory,
                                        @Named( "ZanzaThreadGroup" ) final ThreadGroup zanzaThreadGroup )
    {
        this.zanzaConfig = zanzaConfig;
        this.regionManager = regionManager;
        this.partitionService = partitionService;
        this.partitionKeyFunctionFactory = partitionKeyFunctionFactory;
        this.zanzaThreadGroup = zanzaThreadGroup;
        createDownstreamTupleSenderFactories( partitionService );
    }

    private void createDownstreamTupleSenderFactories ( final PartitionService partitionService )
    {
        defaultDownstreamTupleSenderConstructors[ 1 ] = ( pairs, tupleQueueContext ) -> {
            final Pair<Port, Port> pair1 = pairs.get( 0 );
            return new DownstreamTupleSender1( pair1._1.portIndex, pair1._2.portIndex, tupleQueueContext );
        };
        defaultDownstreamTupleSenderConstructors[ 2 ] = ( pairs, tupleQueueContext ) -> {
            final Pair<Port, Port> pair1 = pairs.get( 0 );
            final Pair<Port, Port> pair2 = pairs.get( 1 );
            return new DownstreamTupleSender2( pair1._1.portIndex,
                                               pair1._2.portIndex,
                                               pair2._1.portIndex,
                                               pair2._2.portIndex,
                                               tupleQueueContext );
        };
        defaultDownstreamTupleSenderConstructors[ 3 ] = ( pairs, tupleQueueContext ) -> {
            final Pair<Port, Port> pair1 = pairs.get( 0 );
            final Pair<Port, Port> pair2 = pairs.get( 1 );
            final Pair<Port, Port> pair3 = pairs.get( 2 );
            return new DownstreamTupleSender3( pair1._1.portIndex,
                                               pair1._2.portIndex,
                                               pair2._1.portIndex,
                                               pair2._2.portIndex,
                                               pair3._1.portIndex,
                                               pair3._2.portIndex,
                                               tupleQueueContext );
        };
        defaultDownstreamTupleSenderConstructors[ 4 ] = ( pairs, tupleQueueContext ) -> {
            final Pair<Port, Port> pair1 = pairs.get( 0 );
            final Pair<Port, Port> pair2 = pairs.get( 1 );
            final Pair<Port, Port> pair3 = pairs.get( 2 );
            final Pair<Port, Port> pair4 = pairs.get( 3 );
            return new DownstreamTupleSender4( pair1._1.portIndex,
                                               pair1._2.portIndex,
                                               pair2._1.portIndex,
                                               pair2._2.portIndex,
                                               pair3._1.portIndex,
                                               pair3._2.portIndex,
                                               pair4._1.portIndex,
                                               pair4._2.portIndex,
                                               tupleQueueContext );
        };
        defaultDownstreamTupleSenderConstructors[ 5 ] = ( pairs, tupleQueueContext ) -> {
            final int[] sourcePorts = new int[ pairs.size() ];
            final int[] destinationPorts = new int[ pairs.size() ];
            copyPorts( pairs, sourcePorts, destinationPorts );
            return new DownstreamTupleSenderN( sourcePorts, destinationPorts, tupleQueueContext );
        };
        partitionedDownstreamTupleSenderConstructors[ 1 ] = ( pairs, partitionCount, partitionDistribution, tupleQueueContexts,
                                                              partitionKeyFunction ) -> {
            final Pair<Port, Port> pair1 = pairs.get( 0 );
            return new PartitionedDownstreamTupleSender1( pair1._1.portIndex,
                                                          pair1._2.portIndex,
                                                          partitionService.getPartitionCount(),
                                                          partitionDistribution,
                                                          tupleQueueContexts,
                                                          partitionKeyFunction );
        };
        partitionedDownstreamTupleSenderConstructors[ 2 ] = ( pairs, partitionCount, partitionDistribution, tupleQueueContexts,
                                                              partitionKeyFunction ) -> {
            final Pair<Port, Port> pair1 = pairs.get( 0 );
            final Pair<Port, Port> pair2 = pairs.get( 1 );
            return new PartitionedDownstreamTupleSender2( pair1._1.portIndex,
                                                          pair1._2.portIndex,
                                                          pair2._1.portIndex,
                                                          pair2._2.portIndex,
                                                          partitionService.getPartitionCount(),
                                                          partitionDistribution,
                                                          tupleQueueContexts,
                                                          partitionKeyFunction );
        };
        partitionedDownstreamTupleSenderConstructors[ 3 ] = ( pairs, partitionCount, partitionDistribution, tupleQueueContexts,
                                                              partitionKeyFunction ) -> {
            final Pair<Port, Port> pair1 = pairs.get( 0 );
            final Pair<Port, Port> pair2 = pairs.get( 1 );
            final Pair<Port, Port> pair3 = pairs.get( 2 );
            return new PartitionedDownstreamTupleSender3( pair1._1.portIndex,
                                                          pair1._2.portIndex,
                                                          pair2._1.portIndex,
                                                          pair2._2.portIndex,
                                                          pair3._1.portIndex,
                                                          pair3._2.portIndex,
                                                          partitionService.getPartitionCount(),
                                                          partitionDistribution,
                                                          tupleQueueContexts,
                                                          partitionKeyFunction );
        };
        partitionedDownstreamTupleSenderConstructors[ 4 ] = ( pairs, partitionCount, partitionDistribution, tupleQueueContexts,
                                                              partitionKeyFunction ) -> {
            final Pair<Port, Port> pair1 = pairs.get( 0 );
            final Pair<Port, Port> pair2 = pairs.get( 1 );
            final Pair<Port, Port> pair3 = pairs.get( 2 );
            final Pair<Port, Port> pair4 = pairs.get( 3 );
            return new PartitionedDownstreamTupleSender4( pair1._1.portIndex,
                                                          pair1._2.portIndex,
                                                          pair2._1.portIndex,
                                                          pair2._2.portIndex,
                                                          pair3._1.portIndex,
                                                          pair3._2.portIndex,
                                                          pair4._1.portIndex,
                                                          pair4._2.portIndex,
                                                          partitionService.getPartitionCount(),
                                                          partitionDistribution,
                                                          tupleQueueContexts,
                                                          partitionKeyFunction );
        };
        partitionedDownstreamTupleSenderConstructors[ 5 ] = ( pairs, partitionCount, partitionDistribution, tupleQueueContexts,
                                                              partitionKeyFunction ) -> {
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
    public List<PipelineRuntimeState> createPipelineRuntimeStates ( final Supervisor supervisor, final FlowDef flow,
                                                                    final List<RegionRuntimeConfig> regionRuntimeConfigs )
    {
        createPipelineRuntimeStates( regionRuntimeConfigs );
        createPipelineReplicas( flow, regionRuntimeConfigs );
        createDownstreamTupleSenders( flow );
        createUpstreamContexts( flow );
        createPipelineReplicaRunners( supervisor );
        return new ArrayList<>( this.pipelineRuntimeStates.values() );
    }

    private void createPipelineRuntimeStates ( final Collection<RegionRuntimeConfig> regionRuntimeConfigs )
    {
        final List<RegionRuntimeConfig> l = new ArrayList<>( regionRuntimeConfigs );
        Collections.sort( l, ( r1, r2 ) -> Integer.compare( r1.getRegionId(), r2.getRegionId() ) );

        for ( RegionRuntimeConfig regionRuntimeConfig : l )
        {
            LOGGER.info( "Initializing regionId={} with {} pipelines ( {} ) and {} replicas",
                         regionRuntimeConfig.getRegionId(),
                         regionRuntimeConfig.getPipelineCount(),
                         regionRuntimeConfig.getPipelineStartIndices(),
                         regionRuntimeConfig.getReplicaCount() );
            for ( int pipelineId = 0; pipelineId < regionRuntimeConfig.getPipelineCount(); pipelineId++ )
            {
                final PipelineId id = new PipelineId( regionRuntimeConfig.getRegionId(), pipelineId );
                final PipelineRuntimeState prev = pipelineRuntimeStates.put( id, new PipelineRuntimeState( id, regionRuntimeConfig ) );
                checkState( prev == null, "there are multiple pipelines with same id: %s", id );
            }
        }
    }

    private void createPipelineReplicas ( final FlowDef flow, final Collection<RegionRuntimeConfig> regionRuntimeConfigs )
    {
        for ( RegionRuntimeConfig regionRuntimeConfig : regionRuntimeConfigs )
        {
            final Region region = regionManager.createRegion( flow, regionRuntimeConfig );
            for ( int pipelineId = 0; pipelineId < regionRuntimeConfig.getPipelineCount(); pipelineId++ )
            {
                final PipelineReplica[] pipelineReplicas = region.getPipelineReplicas( pipelineId );
                final PipelineId id = new PipelineId( regionRuntimeConfig.getRegionId(), pipelineId );
                final PipelineRuntimeState pipelineRuntimeState = pipelineRuntimeStates.get( id );
                for ( int replicaIndex = 0; replicaIndex < pipelineRuntimeState.getReplicaCount(); replicaIndex++ )
                {
                    pipelineRuntimeState.setPipelineReplica( replicaIndex, pipelineReplicas[ replicaIndex ] );
                }
            }

            LOGGER.info( "regionId={} is created", regionRuntimeConfig.getRegionId() );
        }
    }

    private void createDownstreamTupleSenders ( final FlowDef flow )
    {
        for ( PipelineRuntimeState pipelineRuntimeState : pipelineRuntimeStates.values() )
        {
            createDownstreamTupleSenders( flow, pipelineRuntimeState );
        }
    }

    private void createDownstreamTupleSenders ( final FlowDef flow, final PipelineRuntimeState pipelineRuntimeState )
    {
        final OperatorDef[] operators = pipelineRuntimeState.getOperatorDefs();
        final OperatorDef lastOperator = operators[ operators.length - 1 ];
        final Map<String, List<Pair<Port, Port>>> connections = getDownstreamConnections( flow, lastOperator );
        LOGGER.info( "Pipeline {} with last operator {} has following downstream connections: {}",
                     pipelineRuntimeState.getId(),
                     lastOperator.id(),
                     connections );

        for ( int replicaIndex = 0; replicaIndex < pipelineRuntimeState.getReplicaCount(); replicaIndex++ )
        {
            final DownstreamTupleSender[] senders = new DownstreamTupleSender[ connections.size() ];
            int i = 0;
            for ( Entry<String, List<Pair<Port, Port>>> e : connections.entrySet() )
            {
                final String downstreamOperatorId = e.getKey();
                final List<Pair<Port, Port>> pairs = e.getValue();
                final OperatorDef downstreamOperator = flow.getOperator( downstreamOperatorId );
                final TupleQueueContext[] pipelineTupleQueueContexts = getPipelineTupleQueueContexts( downstreamOperator );
                final int j = min( pairs.size(), DOWNSTREAM_TUPLE_SENDER_CONSTRUCTOR_COUNT );
                if ( downstreamOperator.operatorType() == PARTITIONED_STATEFUL )
                {
                    final int[] partitionDistribution = getPartitionDistribution( downstreamOperator );
                    final PartitionKeyFunction partitionKeyFunction = partitionKeyFunctionFactory.createPartitionKeyFunction(
                            downstreamOperator.partitionFieldNames() );
                    senders[ i ] = partitionedDownstreamTupleSenderConstructors[ j ].apply( pairs,
                                                                                            partitionService.getPartitionCount(),
                                                                                            partitionDistribution,
                                                                                            pipelineTupleQueueContexts,
                                                                                            partitionKeyFunction );
                }
                else
                {
                    final TupleQueueContext pipelineTupleQueueContext = pipelineTupleQueueContexts[ replicaIndex ];
                    senders[ i ] = defaultDownstreamTupleSenderConstructors[ j ].apply( pairs, pipelineTupleQueueContext );
                }

                i++;
            }

            if ( i == 0 )
            {
                pipelineRuntimeState.setDownstreamTupleSender( replicaIndex, new NopDownstreamTupleSender() );
            }
            else if ( i == 1 )
            {
                pipelineRuntimeState.setDownstreamTupleSender( replicaIndex, senders[ 0 ] );
            }
            else
            {
                pipelineRuntimeState.setDownstreamTupleSender( replicaIndex, new CompositeDownstreamTupleSender( senders ) );
            }

            LOGGER.info( "Created {} for pipeline {} replicaIndex={}",
                         pipelineRuntimeState.getDownstreamTupleSender( replicaIndex ).getClass().getSimpleName(),
                         pipelineRuntimeState.getId() );
        }
    }

    private Map<String, List<Pair<Port, Port>>> getDownstreamConnections ( final FlowDef flow, final OperatorDef operator )
    {
        final Map<String, List<Pair<Port, Port>>> result = new LinkedHashMap<>();
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
                                                   .add( Pair.of( upstreamPort, downstreamPort ) ) );
            }
        }

        for ( List<Pair<Port, Port>> pairs : result.values() )
        {
            Collections.sort( pairs, ( p1, p2 ) -> {
                final int r1 = p1._1.operatorId.compareTo( p2._1.operatorId );
                final int r2 = Integer.compare( p1._1.portIndex, p2._1.portIndex );
                final int r3 = Integer.compare( p1._2.portIndex, p2._2.portIndex );
                return r1 == 0 ? ( r2 == 0 ? r3 : r2 ) : r1;
            } );
        }

        return result;
    }

    private int[] getPartitionDistribution ( final OperatorDef operator )
    {
        final PipelineRuntimeState pipelineRuntimeState = getPipelineRuntimeState( operator, 0 );
        return partitionService.getOrCreatePartitionDistribution( pipelineRuntimeState.getId().regionId,
                                                                  pipelineRuntimeState.getReplicaCount() );
    }

    private TupleQueueContext[] getPipelineTupleQueueContexts ( final OperatorDef operator )
    {
        final PipelineRuntimeState pipelineRuntimeState = getPipelineRuntimeState( operator, 0 );
        final int replicaCount = pipelineRuntimeState.getReplicaCount();
        final TupleQueueContext[] tupleQueueContexts = new TupleQueueContext[ replicaCount ];
        for ( int i = 0; i < replicaCount; i++ )
        {
            tupleQueueContexts[ i ] = pipelineRuntimeState.getPipelineReplica( i ).getUpstreamTupleQueueContext();
        }

        return tupleQueueContexts;
    }

    private void copyPorts ( final List<Pair<Port, Port>> pairs, final int[] sourcePorts, final int[] destinationPorts )
    {
        for ( int i = 0; i < pairs.size(); i++ )
        {
            sourcePorts[ i ] = pairs.get( i )._1.portIndex;
            destinationPorts[ i ] = pairs.get( i )._2.portIndex;
        }
    }

    private void createUpstreamContexts ( final FlowDef flow )
    {
        for ( PipelineRuntimeState pipelineRuntimeState : pipelineRuntimeStates.values() )
        {
            UpstreamContext upstreamContext;
            final OperatorDef firstOperator = pipelineRuntimeState.getOperatorDefs()[ 0 ];
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
                            final PipelineRuntimeState upstreamPipeline = getPipelineRuntimeState( upstreamOperator, -1 );
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

            LOGGER.info( "Created {} for pipeline {}", upstreamContext, pipelineRuntimeState.getId() );
            pipelineRuntimeState.setUpstreamContext( upstreamContext );
        }
    }

    private PipelineRuntimeState getPipelineRuntimeState ( final OperatorDef operator, final int operatorIndex )
    {
        for ( PipelineRuntimeState pipelineRuntimeState : pipelineRuntimeStates.values() )
        {
            final int i = pipelineRuntimeState.getOperatorIndex( operator );
            if ( i == -1 )
            {
                continue;
            }
            else if ( operatorIndex == -1 )
            {
                return pipelineRuntimeState;
            }

            checkArgument( operatorIndex == i,
                           "Operator {} is expected to be at %s'th index of pipeline %s but it is at %s'th index",
                           operator.id(),
                           pipelineRuntimeState.getId(),
                           i );
            return pipelineRuntimeState;
        }

        throw new IllegalArgumentException( "Operator " + operator.id() + " is not found in the pipelines" );
    }

    private void createPipelineReplicaRunners ( final Supervisor supervisor )
    {
        for ( PipelineRuntimeState pipelineRuntimeState : pipelineRuntimeStates.values() )
        {
            for ( int replicaIndex = 0; replicaIndex < pipelineRuntimeState.getReplicaCount(); replicaIndex++ )
            {
                final PipelineReplica pipelineReplica = pipelineRuntimeState.getPipelineReplica( replicaIndex );
                final SupervisorNotifier supervisorNotifier = new SupervisorNotifier( supervisor, pipelineReplica );
                final PipelineReplicaRunner runner = new PipelineReplicaRunner( zanzaConfig,
                                                                                pipelineReplica,
                                                                                supervisor,
                                                                                supervisorNotifier,
                                                                                pipelineRuntimeState.getDownstreamTupleSender(
                                                                                        replicaIndex ) );
                final Thread thread = new Thread( zanzaThreadGroup, runner, "Thread-" + pipelineReplica.id() );
                pipelineRuntimeState.setPipelineReplicaRunner( replicaIndex, runner, thread );
                LOGGER.info( "Created runner thread for pipeline instance: {}", pipelineReplica.id() );
            }
        }
    }

    @FunctionalInterface
    private interface Function6<T1, T2, T3, T4, T5, T6>
    {
        T6 apply ( T1 t1, T2 t2, T3 t3, T4 t4, T5 t5 );
    }


    private static class NopDownstreamTupleSender implements DownstreamTupleSender
    {

        @Override
        public Future<Void> send ( final TuplesImpl tuples )
        {
            return null;
        }

    }

}
