package cs.bilkent.joker.engine.adaptation.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import javax.inject.Singleton;

import cs.bilkent.joker.engine.adaptation.AdaptationAction;
import cs.bilkent.joker.engine.adaptation.AdaptationManager;
import cs.bilkent.joker.engine.adaptation.BottleneckResolver;
import cs.bilkent.joker.engine.adaptation.impl.bottleneckresolver.PipelineSplitter;
import cs.bilkent.joker.engine.adaptation.impl.bottleneckresolver.RegionExtender;
import cs.bilkent.joker.engine.config.AdaptationConfig;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import cs.bilkent.joker.engine.metric.FlowMetrics;
import cs.bilkent.joker.engine.metric.PipelineMetrics;
import cs.bilkent.joker.engine.metric.PipelineMetricsHistorySummarizer;
import static cs.bilkent.joker.engine.util.RegionUtil.getLeftMostRegions;
import cs.bilkent.joker.flow.FlowDef;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkArgument;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkState;
import static java.util.Collections.emptyList;
import static java.util.Collections.reverse;
import static java.util.Collections.unmodifiableList;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

@NotThreadSafe
@Singleton
public class OrganicAdaptationManager implements AdaptationManager
{

    private final PipelineMetricsHistorySummarizer pipelineMetricsHistorySummarizer;

    private final List<BottleneckResolver> bottleneckResolvers;

    private final BiPredicate<PipelineMetrics, PipelineMetrics> loadChangePredicate;

    private final Predicate<PipelineMetrics> bottleneckPredicate;

    private final BiPredicate<PipelineMetrics, PipelineMetrics> adaptationEvaluationPredicate;

    private final Function<RegionExecPlan, RegionAdaptationContext> regionAdaptationContextFactory;

    private boolean adaptationEnabled;

    private FlowDef flowDef;

    private List<RegionAdaptationContext> regions;

    private List<RegionAdaptationContext> adaptingRegions = emptyList();

    @Inject
    public OrganicAdaptationManager ( final JokerConfig config )
    {
        this( config, RegionAdaptationContext::new );
    }

    OrganicAdaptationManager ( final JokerConfig config,
                               final Function<RegionExecPlan, RegionAdaptationContext> regionAdaptationContextFactory )
    {
        final AdaptationConfig adaptationConfig = config.getAdaptationConfig();
        this.pipelineMetricsHistorySummarizer = adaptationConfig.getPipelineMetricsHistorySummarizer();
        final BiFunction<RegionExecPlan, PipelineMetrics, Integer> ext = adaptationConfig.getPipelineSplitIndexExtractor();
        final BottleneckResolver pipelineSplitter = new PipelineSplitter( ext );
        final BottleneckResolver regionExtender = new RegionExtender( config.getPartitionServiceConfig().getMaxReplicaCount() );
        final List<BottleneckResolver> bottleneckResolvers = new ArrayList<>();
        if ( adaptationConfig.isAdaptationEnabled() )
        {
            if ( adaptationConfig.isPipelineSplitEnabled() )
            {
                bottleneckResolvers.add( pipelineSplitter );
            }
            if ( adaptationConfig.isRegionRebalanceEnabled() )
            {
                bottleneckResolvers.add( regionExtender );
            }

            checkState( !bottleneckResolvers.isEmpty(), "there should be at least one bottleneck resolver when adaptation is enabled!" );

            if ( !adaptationConfig.isPipelineSplitFirst() )
            {
                reverse( bottleneckResolvers );
            }
        }

        this.bottleneckResolvers = unmodifiableList( bottleneckResolvers );
        this.loadChangePredicate = adaptationConfig.getLoadChangePredicate();
        this.bottleneckPredicate = adaptationConfig.getBottleneckPredicate();
        this.adaptationEvaluationPredicate = adaptationConfig.getAdaptationEvaluationPredicate();
        this.regionAdaptationContextFactory = regionAdaptationContextFactory;
        this.adaptationEnabled = adaptationConfig.isAdaptationEnabled();
    }

    RegionAdaptationContext getRegion ( final int regionId )
    {
        return regions.stream().filter( region -> region.getRegionId() == regionId ).findFirst().orElse( null );
    }

    List<RegionAdaptationContext> getAdaptingRegions ()
    {
        return unmodifiableList( adaptingRegions );
    }

    @Override
    public void initialize ( final FlowDef flowDef, final List<RegionExecPlan> execPlans )
    {
        checkArgument( flowDef != null );
        checkArgument( execPlans != null && execPlans.size() > 0 );
        checkState( regions == null );

        this.flowDef = flowDef;
        this.regions = execPlans.stream().map( regionAdaptationContextFactory ).collect( toList() );
        this.regions.sort( comparing( RegionAdaptationContext::getRegionId ) );
    }

    @Override
    public void disableAdaptation ()
    {
        adaptationEnabled = false;
    }

    @Override
    public List<AdaptationAction> adapt ( final List<RegionExecPlan> execPlans, final FlowMetrics metrics )
    {
        return adaptingRegions.isEmpty() ? resolveBottlenecksIfPresent( metrics ) : evaluateAdaptations( metrics );
    }

    private List<AdaptationAction> resolveBottlenecksIfPresent ( final FlowMetrics metrics )
    {
        if ( !adaptationEnabled )
        {
            return emptyList();
        }

        for ( RegionAdaptationContext region : regions )
        {
            final List<PipelineMetrics> regionMetrics = metrics.getRegionMetrics( region.getRegionId(), pipelineMetricsHistorySummarizer );
            region.updateRegionMetrics( regionMetrics, loadChangePredicate );
        }

        final List<RegionAdaptationContext> adaptingRegions = new ArrayList<>();
        final List<AdaptationAction> adaptationActions = new ArrayList<>();

        for ( RegionAdaptationContext region : regions )
        {
            final List<AdaptationAction> regionActions = region.resolveIfBottleneck( bottleneckPredicate, bottleneckResolvers );
            if ( !regionActions.isEmpty() )
            {
                adaptingRegions.add( region );
                adaptationActions.addAll( regionActions );
            }
        }

        if ( adaptingRegions.isEmpty() )
        {
            return emptyList();
        }

        this.adaptingRegions = adaptingRegions;

        return unmodifiableList( adaptationActions );
    }

    private List<AdaptationAction> evaluateAdaptations ( final FlowMetrics metrics )
    {
        final RegionAdaptationContext nonResolvedRegion = getNonResolvedBottleneck( metrics );

        return ( nonResolvedRegion == null ) ? finalizeAdaptations( metrics ) : retryOrRevertAdaptations( nonResolvedRegion );
    }

    private RegionAdaptationContext getNonResolvedBottleneck ( final FlowMetrics metrics )
    {
        final List<RegionDef> leftMostRegions = getLeftMostRegions( flowDef, getRegionDefs( regions ), getRegionDefs( adaptingRegions ) );

        final Predicate<RegionAdaptationContext> isLeftMostRegion = r -> leftMostRegions.contains( r.getRegionDef() );
        final Predicate<RegionAdaptationContext> isAdaptationFailed = r -> {
            final int regionId = r.getRegionId();
            final List<PipelineMetrics> regionMetrics = metrics.getRegionMetrics( regionId, pipelineMetricsHistorySummarizer );

            return !r.isAdaptationSuccessful( regionMetrics, adaptationEvaluationPredicate );
        };

        return adaptingRegions.stream().filter( isLeftMostRegion ).filter( isAdaptationFailed ).findFirst().orElse( null );
    }

    private List<RegionDef> getRegionDefs ( final List<RegionAdaptationContext> regions )
    {
        return regions.stream().map( RegionAdaptationContext::getRegionDef ).collect( toList() );
    }

    private List<AdaptationAction> finalizeAdaptations ( final FlowMetrics metrics )
    {
        for ( RegionAdaptationContext adaptingRegion : adaptingRegions )
        {
            final int regionId = adaptingRegion.getRegionId();
            final List<PipelineMetrics> regionMetrics = metrics.getRegionMetrics( regionId, pipelineMetricsHistorySummarizer );
            adaptingRegion.finalizeAdaptation( regionMetrics );
        }

        adaptingRegions = emptyList();

        return emptyList();
    }

    private List<AdaptationAction> retryOrRevertAdaptations ( final RegionAdaptationContext nonResolvedRegion )
    {
        final List<AdaptationAction> reverts = nonResolvedRegion.revertAdaptation();
        final List<AdaptationAction> newActions = nonResolvedRegion.resolveIfBottleneck( bottleneckPredicate, bottleneckResolvers );

        final List<AdaptationAction> actions = new ArrayList<>();
        if ( newActions.isEmpty() )
        {
            reverse( adaptingRegions );
            for ( RegionAdaptationContext adaptingRegion : adaptingRegions )
            {
                if ( adaptingRegion.equals( nonResolvedRegion ) )
                {
                    actions.addAll( reverts );
                }
                else
                {
                    actions.addAll( adaptingRegion.revertAdaptation() );
                }
            }

            adaptingRegions = emptyList();
        }
        else
        {
            actions.addAll( reverts );
            actions.addAll( newActions );
        }

        return unmodifiableList( actions );
    }

}
