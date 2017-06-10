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
import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.engine.metric.FlowMetrics;
import cs.bilkent.joker.engine.metric.PipelineMetrics;
import cs.bilkent.joker.engine.metric.PipelineMetricsHistorySummarizer;
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
    public void disableAdaptation ()
    {
        adaptationEnabled = false;
    }

    @Override
    public void initialize ( final List<RegionExecutionPlan> regionExecutionPlans )
    {
        checkState( regions == null );
        regions = regionExecutionPlans.stream().map( regionAdaptationContextFactory ).collect( toList() );
        regions.sort( comparing( RegionAdaptationContext::getRegionId ) );
    }

    @Override
    public List<AdaptationAction> apply ( final List<RegionExecutionPlan> regionExecutionPlans, final FlowMetrics flowMetrics )
    {
        if ( adaptingRegions.isEmpty() )
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

            final List<RegionAdaptationContext> newAdaptingRegions = new ArrayList<>();
            final List<AdaptationAction> newAdaptationActions = new ArrayList<>();

            for ( RegionAdaptationContext region : regions )
            {
                final List<AdaptationAction> adaptationActions = region.resolveIfBottleneck( bottleneckPredicate, bottleneckResolvers );
                if ( !adaptationActions.isEmpty() )
                {
                    newAdaptingRegions.add( region );
                    newAdaptationActions.addAll( adaptationActions );
                }
            }

            if ( newAdaptingRegions.isEmpty() )
            {
                return emptyList();
            }

            adaptingRegions = newAdaptingRegions;
            return unmodifiableList( newAdaptationActions );
        }
        else
        {
            int nonResolvedIdx = -1;
            for ( int i = 0; i < adaptingRegions.size(); i++ )
            {
                final RegionAdaptationContext adaptingRegion = adaptingRegions.get( i );
                final List<PipelineMetrics> regionMetrics = flowMetrics.getRegionMetrics( adaptingRegion.getRegionId(),
                                                                                          pipelineMetricsHistorySummarizer );
                final boolean success = adaptingRegion.isAdaptationSuccessful( regionMetrics, adaptationEvaluationPredicate );
                if ( !success )
                {
                    nonResolvedIdx = i;
                    break;
                }
            }

            if ( nonResolvedIdx == -1 )
            {
                for ( RegionAdaptationContext adaptingRegion : adaptingRegions )
                {
                    final List<PipelineMetrics> regionMetrics = flowMetrics.getRegionMetrics( adaptingRegion.getRegionId(),
                                                                                              pipelineMetricsHistorySummarizer );
                    adaptingRegion.finalizeAdaptation( regionMetrics );
                }

                adaptingRegions = emptyList();
                return emptyList();
            }
            else
            {
                final RegionAdaptationContext nonResolvedRegion = adaptingRegions.get( nonResolvedIdx );
                final List<AdaptationAction> rollbacks = nonResolvedRegion.rollbackAdaptation( true );
                final List<AdaptationAction> newAdaptationActions = nonResolvedRegion.resolveIfBottleneck( bottleneckPredicate,
                                                                                                           bottleneckResolvers );

                final List<AdaptationAction> actions = new ArrayList<>();
                if ( newAdaptationActions.isEmpty() )
                {
                    // we are collecting rollbacks in the reverse order...
                    for ( int i = adaptingRegions.size() - 1; i > nonResolvedIdx; i-- )
                    {
                        final RegionAdaptationContext adaptingRegion = adaptingRegions.get( i );
                        actions.addAll( adaptingRegion.rollbackAdaptation( true ) );
                    }

                    actions.addAll( rollbacks );

                    for ( int i = nonResolvedIdx - 1; i >= 0; i-- )
                    {
                        final RegionAdaptationContext adaptingRegion = adaptingRegions.get( i );
                        actions.addAll( adaptingRegion.rollbackAdaptation( true ) );
                    }

                    adaptingRegions = emptyList();
                }
                else
                {
                    actions.addAll( rollbacks );
                    actions.addAll( newAdaptationActions );
                }

                return unmodifiableList( actions );
            }
        }
    }

}
