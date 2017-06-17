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
import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.engine.metric.FlowMetrics;
import cs.bilkent.joker.engine.metric.PipelineMetrics;
import cs.bilkent.joker.engine.metric.PipelineMetricsHistorySummarizer;
import static cs.bilkent.joker.engine.util.RegionUtil.getUpmostRegions;
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

    private final Function<RegionExecutionPlan, RegionAdaptationContext> regionAdaptationContextFactory;

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
                               final Function<RegionExecutionPlan, RegionAdaptationContext> regionAdaptationContextFactory )
    {
        final AdaptationConfig adaptationConfig = config.getAdaptationConfig();
        this.pipelineMetricsHistorySummarizer = adaptationConfig.getPipelineMetricsHistorySummarizer();
        final BiFunction<RegionExecutionPlan, PipelineMetrics, Integer> ext = adaptationConfig.getPipelineSplitIndexExtractor();
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
    public void initialize ( final FlowDef flowDef, final List<RegionExecutionPlan> regionExecutionPlans )
    {
        checkArgument( flowDef != null );
        checkArgument( regionExecutionPlans != null && regionExecutionPlans.size() > 0 );
        checkState( regions == null );

        this.flowDef = flowDef;
        this.regions = regionExecutionPlans.stream().map( regionAdaptationContextFactory ).collect( toList() );
        this.regions.sort( comparing( RegionAdaptationContext::getRegionId ) );
    }

    @Override
    public void disableAdaptation ()
    {
        adaptationEnabled = false;
    }

    @Override
    public List<AdaptationAction> adapt ( final List<RegionExecutionPlan> regionExecutionPlans, final FlowMetrics flowMetrics )
    {
        return adaptingRegions.isEmpty() ? resolveBottleneckRegionsIfPresent( flowMetrics ) : evaluateAdaptations( flowMetrics );
    }

    private List<AdaptationAction> resolveBottleneckRegionsIfPresent ( final FlowMetrics flowMetrics )
    {
        if ( !adaptationEnabled )
        {
            return emptyList();
        }

        for ( RegionAdaptationContext region : regions )
        {
            final List<PipelineMetrics> regionMetrics = flowMetrics.getRegionMetrics( region.getRegionId(),
                                                                                      pipelineMetricsHistorySummarizer );
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

    private List<AdaptationAction> evaluateAdaptations ( final FlowMetrics flowMetrics )
    {
        final RegionAdaptationContext nonResolvedRegion = getNonResolvedBottleneckRegion( flowMetrics );

        return ( nonResolvedRegion == null ) ? finalizeAdaptations( flowMetrics ) : retryOrRollbackAdaptations( nonResolvedRegion );
    }

    private RegionAdaptationContext getNonResolvedBottleneckRegion ( final FlowMetrics flowMetrics )
    {
        final List<RegionDef> upmostRegions = getUpmostRegions( flowDef, getRegionDefs( regions ), getRegionDefs( adaptingRegions ) );

        final Predicate<RegionAdaptationContext> isUpmostRegion = r -> upmostRegions.contains( r.getRegionDef() );
        final Predicate<RegionAdaptationContext> isAdaptationFailed = r ->
        {
            final int regionId = r.getRegionId();
            final List<PipelineMetrics> regionMetrics = flowMetrics.getRegionMetrics( regionId, pipelineMetricsHistorySummarizer );

            return !r.isAdaptationSuccessful( regionMetrics, adaptationEvaluationPredicate );
        };

        return adaptingRegions.stream().filter( isUpmostRegion ).filter( isAdaptationFailed ).findFirst().orElse( null );
    }

    private List<RegionDef> getRegionDefs ( final List<RegionAdaptationContext> regions )
    {
        return regions.stream().map( RegionAdaptationContext::getRegionDef ).collect( toList() );
    }

    private List<AdaptationAction> finalizeAdaptations ( final FlowMetrics flowMetrics )
    {
        for ( RegionAdaptationContext adaptingRegion : adaptingRegions )
        {
            final int regionId = adaptingRegion.getRegionId();
            final List<PipelineMetrics> regionMetrics = flowMetrics.getRegionMetrics( regionId, pipelineMetricsHistorySummarizer );
            adaptingRegion.finalizeAdaptation( regionMetrics );
        }

        adaptingRegions = emptyList();

        return emptyList();
    }

    private List<AdaptationAction> retryOrRollbackAdaptations ( final RegionAdaptationContext nonResolvedRegion )
    {
        final List<AdaptationAction> rollbacks = nonResolvedRegion.rollbackAdaptation();
        final List<AdaptationAction> newActions = nonResolvedRegion.resolveIfBottleneck( bottleneckPredicate, bottleneckResolvers );

        final List<AdaptationAction> actions = new ArrayList<>();
        if ( newActions.isEmpty() )
        {
            reverse( adaptingRegions );
            for ( RegionAdaptationContext adaptingRegion : adaptingRegions )
            {
                if ( adaptingRegion.equals( nonResolvedRegion ) )
                {
                    actions.addAll( rollbacks );
                }
                else
                {
                    actions.addAll( adaptingRegion.rollbackAdaptation() );
                }
            }

            adaptingRegions = emptyList();
        }
        else
        {
            actions.addAll( rollbacks );
            actions.addAll( newActions );
        }

        return unmodifiableList( actions );
    }

}
