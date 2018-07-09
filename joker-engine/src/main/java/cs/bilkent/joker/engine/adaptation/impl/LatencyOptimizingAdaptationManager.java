package cs.bilkent.joker.engine.adaptation.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cs.bilkent.joker.engine.adaptation.AdaptationAction;
import cs.bilkent.joker.engine.adaptation.AdaptationManager;
import cs.bilkent.joker.engine.adaptation.BottleneckResolver;
import cs.bilkent.joker.engine.adaptation.impl.bottleneckresolver.PipelineSplitter;
import cs.bilkent.joker.engine.adaptation.impl.bottleneckresolver.RegionExpander;
import cs.bilkent.joker.engine.config.AdaptationConfig;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import cs.bilkent.joker.engine.metric.FlowMetrics;
import cs.bilkent.joker.engine.metric.LatencyMetrics;
import cs.bilkent.joker.engine.metric.LatencyMetrics.LatencyRecord;
import cs.bilkent.joker.engine.metric.LatencyMetricsHistory;
import cs.bilkent.joker.engine.metric.LatencyMetricsHistorySummarizer;
import cs.bilkent.joker.engine.metric.PipelineMetrics;
import cs.bilkent.joker.engine.metric.PipelineMetricsHistorySummarizer;
import cs.bilkent.joker.flow.FlowDef;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkArgument;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.utils.Pair;
import cs.bilkent.joker.operator.utils.Triple;
import static java.util.Collections.emptyList;
import static java.util.Collections.reverse;
import static java.util.Collections.reverseOrder;
import static java.util.Collections.unmodifiableList;
import static java.util.Comparator.comparing;
import static java.util.Comparator.comparingLong;
import static java.util.stream.Collectors.toList;

@NotThreadSafe
@Singleton
public class LatencyOptimizingAdaptationManager implements AdaptationManager
{

    private static final Logger LOGGER = LoggerFactory.getLogger( LatencyOptimizingAdaptationManager.class );


    private final PipelineMetricsHistorySummarizer pipelineMetricsHistorySummarizer;

    private final LatencyMetricsHistorySummarizer latencyMetricsHistorySummarizer;

    private final List<BottleneckResolver> bottleneckResolvers;

    private final BiPredicate<PipelineMetrics, PipelineMetrics> loadChangePredicate;

    private final Function<RegionExecPlan, RegionAdaptationContext> regionAdaptationContextFactory;

    private final long maxTupleLatencyNanos;

    private boolean adaptationEnabled;

    private List<RegionAdaptationContext> regions;

    private String sinkOperatorId;

    private Triple<RegionAdaptationContext, PipelineId, LatencyMetrics> currentAdaptationCtx;

    @Inject
    public LatencyOptimizingAdaptationManager ( final JokerConfig config )
    {
        this( config, RegionAdaptationContext::new );
    }

    LatencyOptimizingAdaptationManager ( final JokerConfig config,
                                         final Function<RegionExecPlan, RegionAdaptationContext> regionAdaptationContextFactory )
    {
        final AdaptationConfig adaptationConfig = config.getAdaptationConfig();
        this.pipelineMetricsHistorySummarizer = adaptationConfig.getPipelineMetricsHistorySummarizer();
        this.latencyMetricsHistorySummarizer = adaptationConfig.getLatencyMetricsHistorySummarizer();
        final BottleneckResolver pipelineSplitter = new PipelineSplitter( adaptationConfig.getPipelineSplitIndexExtractor() );
        final BottleneckResolver regionExpander = new RegionExpander( config.getPartitionServiceConfig().getMaxReplicaCount() );
        final List<BottleneckResolver> bottleneckResolvers = new ArrayList<>();
        if ( adaptationConfig.isAdaptationEnabled() )
        {
            if ( adaptationConfig.isPipelineSplitEnabled() )
            {
                bottleneckResolvers.add( pipelineSplitter );
            }

            if ( adaptationConfig.isRegionRebalanceEnabled() )
            {
                bottleneckResolvers.add( regionExpander );
            }

            checkState( !bottleneckResolvers.isEmpty(), "there should be at least one bottleneck resolver when adaptation is enabled!" );

            if ( !adaptationConfig.isPipelineSplitFirst() )
            {
                reverse( bottleneckResolvers );
            }
        }

        this.bottleneckResolvers = unmodifiableList( bottleneckResolvers );
        this.loadChangePredicate = adaptationConfig.getLoadChangePredicate();
        this.regionAdaptationContextFactory = regionAdaptationContextFactory;
        this.adaptationEnabled = adaptationConfig.isAdaptationEnabled();
        this.maxTupleLatencyNanos = adaptationConfig.getLatencyThresholdNanos();
        if ( this.maxTupleLatencyNanos <= 0 )
        {
            disableAdaptation();
        }
    }

    @Override
    public void initialize ( final FlowDef flowDef, final List<RegionExecPlan> execPlans )
    {
        checkArgument( flowDef != null );
        checkArgument( execPlans != null && execPlans.size() > 0 );
        checkState( regions == null );

        this.regions = execPlans.stream().map( regionAdaptationContextFactory ).collect( toList() );
        this.regions.sort( comparing( RegionAdaptationContext::getRegionId ) );
        final List<String> sinkOperatorIds = execPlans.stream().map( execPlan -> {
            final RegionDef regionDef = execPlan.getRegionDef();
            return regionDef.getOperator( regionDef.getOperatorCount() - 1 ).getId();
        } ).filter( operatorId -> flowDef.getOutboundConnections( operatorId ).isEmpty() ).collect( toList() );
        checkArgument( sinkOperatorIds.size() == 1, "currently only 1 sink operator is supported for latency optimization" );
        this.sinkOperatorId = sinkOperatorIds.get( 0 );
        LOGGER.info( "Latency optimization will be executed for sink operator: {}", this.sinkOperatorId );
    }

    @Override
    public void disableAdaptation ()
    {
        adaptationEnabled = false;
    }

    @Override
    public List<AdaptationAction> adapt ( final List<RegionExecPlan> execPlans, final FlowMetrics metrics )
    {
        return currentAdaptationCtx == null
               ? increaseParallelismIfLatencyThresholdExceeded( execPlans, metrics )
               : evaluateAdaptations( execPlans, metrics );
    }

    private List<AdaptationAction> increaseParallelismIfLatencyThresholdExceeded ( final List<RegionExecPlan> execPlans,
                                                                                   final FlowMetrics metrics )
    {
        if ( !adaptationEnabled )
        {
            return emptyList();
        }

        regions.forEach( region -> {
            final List<PipelineMetrics> regionMetrics = metrics.getRegionMetrics( region.getRegionId(), pipelineMetricsHistorySummarizer );
            region.updateRegionMetrics( regionMetrics, loadChangePredicate );
        } );

        final LatencyMetrics latencyMetrics = getSummarizedLatencyMetrics( execPlans, metrics );

        if ( latencyMetrics.getTupleLatency().getMean() <= maxTupleLatencyNanos )
        {
            LOGGER.info( "Latency optimization succeeded. Threshold (ns): {} current latency (ns): {}",
                         maxTupleLatencyNanos,
                         latencyMetrics.getTupleLatency().getMean() );

            return emptyList();
        }

        final Triple<RegionAdaptationContext, PipelineId, List<AdaptationAction>> ctx;
        ctx = regions.stream()
                     // for each pipeline of each region
                     .flatMap( region -> region.getCurrentExecPlan()
                                               .getPipelineIds()
                                               .stream()
                                               .map( pipelineId -> Pair.of( region, pipelineId ) ) )
                     // get the overall latency of the pipeline
                     .map( p -> {
                         final RegionAdaptationContext region = p._1;
                         final RegionExecPlan execPlan = region.getCurrentExecPlan();
                         final PipelineId pipelineId = p._2;
                         final long lt = getPipelineMeanLatency( latencyMetrics, execPlan, pipelineId );
                         LOGGER.debug( "{} latency: {} ns", pipelineId, lt );
                         return Triple.of( region, pipelineId, lt );
                     } )
                     // sort the pipelines in descending order of pipeline latency
                     .sorted( reverseOrder( comparingLong( t -> t._3 ) ) )
                     // get possible parallelism changes
                     .map( t -> {
                         final RegionAdaptationContext region = t._1;
                         final PipelineId pipelineId = t._2;
                         final Predicate<PipelineMetrics> predicate = m -> m.getPipelineId().equals( pipelineId );
                         final List<AdaptationAction> actions = region.resolveIfBottleneck( predicate, bottleneckResolvers );
                         LOGGER.debug( "{} actions: {}", pipelineId, actions );
                         return Triple.of( region, pipelineId, actions );
                     } )
                     // get the first possible parallelism change
                     .filter( p -> p._3.size() > 0 ).findFirst().orElse( null );

        if ( ctx == null )
        {
            LOGGER.warn( "Although latency threshold is exceeded, there is no possible parallelism change" );

            return emptyList();
        }

        final RegionAdaptationContext adaptingRegion = ctx._1;
        final PipelineId adaptingPipelineId = ctx._2;
        final List<AdaptationAction> actions = ctx._3;

        currentAdaptationCtx = Triple.of( adaptingRegion, adaptingPipelineId, latencyMetrics );

        LOGGER.info( "Optimizing latency of {} with {}", adaptingPipelineId, actions );

        return unmodifiableList( actions );
    }

    private List<AdaptationAction> evaluateAdaptations ( final List<RegionExecPlan> execPlans, final FlowMetrics metrics )
    {
        final LatencyMetrics latencyMetrics = getSummarizedLatencyMetrics( execPlans, metrics );
        final long afterAdaptationTupleLatency = latencyMetrics.getTupleLatency().getMean();
        final long beforeAdaptationTupleLatency = currentAdaptationCtx._3.getTupleLatency().getMean();
        final PipelineId adaptingPipelineId = currentAdaptationCtx._2;
        if ( afterAdaptationTupleLatency < beforeAdaptationTupleLatency )
        {
            // tuple latency is decreased...
            LOGGER.info( "Latency optimization of {} is successful. Tuple latency goes down to {} ns from {} ns",
                         adaptingPipelineId,
                         afterAdaptationTupleLatency,
                         beforeAdaptationTupleLatency );

            finalizeAdaptation( metrics );

            return emptyList();
        }

        LOGGER.info( "Latency optimization of {} failed. New tuple latency {}, previous: {} ns",
                     adaptingPipelineId,
                     afterAdaptationTupleLatency,
                     beforeAdaptationTupleLatency );

        return retryAdaptation();
    }

    private List<AdaptationAction> retryAdaptation ()
    {
        final RegionAdaptationContext adaptingRegion = currentAdaptationCtx._1;
        final PipelineId pipelineId = currentAdaptationCtx._2;
        final Predicate<PipelineMetrics> predicate = metrics -> metrics.getPipelineId().equals( pipelineId );
        final List<AdaptationAction> reverts = adaptingRegion.revertAdaptation();
        final List<AdaptationAction> newActions = adaptingRegion.resolveIfBottleneck( predicate, bottleneckResolvers );

        // TODO if total latency of the non-resolved pipelines is bigger than the latency threshold, we cannot continue anymore.
        // for now, just complete the latency optimization process once there is no other candidate parallelism change

        final List<AdaptationAction> actions = new ArrayList<>( reverts );
        if ( newActions.isEmpty() )
        {
            LOGGER.warn( "Cannot find another parallelism change to optimize {}. Completing the latency optimization phase with failure...",
                         pipelineId );
            currentAdaptationCtx = null;
            disableAdaptation();
        }
        else
        {
            LOGGER.info( "Continuing to optimize {} with {}", pipelineId, newActions );
            actions.addAll( newActions );
        }

        return unmodifiableList( actions );
    }


    private void finalizeAdaptation ( final FlowMetrics metrics )
    {
        final RegionAdaptationContext adaptingRegion = currentAdaptationCtx._1;
        final List<PipelineMetrics> regionMetrics = metrics.getRegionMetrics( adaptingRegion.getRegionId(),
                                                                              pipelineMetricsHistorySummarizer );
        adaptingRegion.finalizeAdaptation( regionMetrics );

        LOGGER.debug( "Adaptation of {} is finalized", currentAdaptationCtx._2 );

        currentAdaptationCtx = null;
    }

    private long getPipelineMeanLatency ( final LatencyMetrics latencyMetrics, final RegionExecPlan execPlan, final PipelineId pipelineId )
    {
        final OperatorDef[] operatorDefs = execPlan.getOperatorDefsByPipelineStartIndex( pipelineId.getPipelineStartIndex() );
        final List<LatencyRecord> records = new ArrayList<>();
        final LatencyRecord queueLatency = latencyMetrics.getQueueLatency( operatorDefs[ 0 ].getId() );
        if ( queueLatency != null )
        {
            // not a source operator
            records.add( queueLatency );
        }
        Arrays.stream( operatorDefs ).map( OperatorDef::getId ).map( latencyMetrics::getInvocationLatency ).forEach( records::add );

        return records.stream().mapToLong( LatencyRecord::getMean ).sum();
    }

    private LatencyMetrics getSummarizedLatencyMetrics ( final List<RegionExecPlan> execPlans, final FlowMetrics metrics )
    {
        final int replicaCount = execPlans.stream()
                                          .filter( execPlan -> execPlan.getRegionDef()
                                                                       .getOperators()
                                                                       .stream()
                                                                       .map( OperatorDef::getId )
                                                                       .anyMatch( operatorId -> operatorId.equals( sinkOperatorId ) ) )
                                          .findFirst()
                                          .orElseThrow( (Supplier<RuntimeException>) IllegalStateException::new )
                                          .getReplicaCount();

        final List<LatencyMetricsHistory> histories = IntStream.range( 0, replicaCount )
                                                               .mapToObj( i -> Pair.of( sinkOperatorId, i ) )
                                                               .map( metrics::getLatencyMetricsHistory )
                                                               .collect( toList() );

        return latencyMetricsHistorySummarizer.summarize( histories );
    }

    RegionAdaptationContext getAdaptingRegion ()
    {
        return currentAdaptationCtx != null ? currentAdaptationCtx._1 : null;
    }

    PipelineId getAdaptingPipelineId ()
    {
        return currentAdaptationCtx != null ? currentAdaptationCtx._2 : null;
    }

    boolean isAdaptationEnabled ()
    {
        return adaptationEnabled;
    }
}
