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
import static java.util.Collections.unmodifiableList;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

@NotThreadSafe
@Singleton
public class OrganicAdaptationManager implements AdaptationManager
{

    private final AdaptationConfig adaptationConfig;

    private final PipelineMetricsHistorySummarizer pipelineMetricsHistorySummarizer;

    private final List<BottleneckResolver> bottleneckResolvers;

    private final BiPredicate<PipelineMetrics, PipelineMetrics> loadChangePredicate;

    private final Predicate<PipelineMetrics> bottleneckPredicate;

    private final BiPredicate<PipelineMetrics, PipelineMetrics> adaptationEvaluationPredicate;

    private final Function<RegionExecutionPlan, RegionAdaptationContext> regionAdaptationContextFactory;

    private List<RegionAdaptationContext> regions;

    private RegionAdaptationContext adaptingRegion;

    @Inject
    public OrganicAdaptationManager ( final JokerConfig config )
    {
        this( config, RegionAdaptationContext::new );
    }

    OrganicAdaptationManager ( final JokerConfig config,
                               final Function<RegionExecutionPlan, RegionAdaptationContext> regionAdaptationContextFactory )
    {
        this.adaptationConfig = config.getAdaptationConfig();
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
        }

        this.bottleneckResolvers = unmodifiableList( bottleneckResolvers );
        this.loadChangePredicate = adaptationConfig.getLoadChangePredicate();
        this.bottleneckPredicate = adaptationConfig.getBottleneckPredicate();
        this.adaptationEvaluationPredicate = adaptationConfig.getAdaptationEvaluationPredicate();
        this.regionAdaptationContextFactory = regionAdaptationContextFactory;
    }

    RegionAdaptationContext getRegion ( final int regionId )
    {
        return regions.stream().filter( region -> region.getRegionId() == regionId ).findFirst().orElse( null );
    }

    RegionAdaptationContext getAdaptingRegion ()
    {
        return adaptingRegion;
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
        if ( adaptingRegion == null )
        {
            for ( RegionAdaptationContext region : regions )
            {
                final List<PipelineMetrics> regionMetrics = flowMetrics.getRegionMetrics( region.getRegionId(),
                                                                                          pipelineMetricsHistorySummarizer );
                region.updateRegionMetrics( regionMetrics, loadChangePredicate );
            }

            for ( RegionAdaptationContext region : regions )
            {
                final List<AdaptationAction> adaptationActions = region.resolveIfBottleneck( bottleneckPredicate, bottleneckResolvers );
                if ( !adaptationActions.isEmpty() )
                {
                    this.adaptingRegion = region;

                    return adaptationActions;
                }
            }
        }
        else
        {
            final List<PipelineMetrics> regionMetrics = flowMetrics.getRegionMetrics( adaptingRegion.getRegionId(),
                                                                                      pipelineMetricsHistorySummarizer );
            final List<AdaptationAction> rollbacks = adaptingRegion.evaluateAdaptation( regionMetrics, adaptationEvaluationPredicate );

            if ( !rollbacks.isEmpty() )
            {
                final List<AdaptationAction> adaptationActions = adaptingRegion.resolveIfBottleneck( bottleneckPredicate,
                                                                                                     bottleneckResolvers );

                if ( !adaptationActions.isEmpty() )
                {
                    final List<AdaptationAction> actions = new ArrayList<>();
                    actions.addAll( rollbacks );
                    actions.addAll( adaptationActions );

                    return actions;
                }

                adaptingRegion = null;

                return rollbacks;
            }

            adaptingRegion = null;
        }

        return emptyList();
    }

}
