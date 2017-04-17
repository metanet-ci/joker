package cs.bilkent.joker.engine.adaptation.impl;

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
import cs.bilkent.joker.engine.adaptation.PipelineMetricsHistorySummarizer;
import cs.bilkent.joker.engine.adaptation.impl.bottleneckresolver.PipelineSplitter;
import cs.bilkent.joker.engine.adaptation.impl.bottleneckresolver.RegionExtender;
import cs.bilkent.joker.engine.config.AdaptationConfig;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.engine.metric.FlowMetrics;
import cs.bilkent.joker.engine.metric.PipelineMetrics;
import cs.bilkent.joker.engine.metric.PipelineMetricsHistory;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkArgument;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkState;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
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
        this.bottleneckResolvers = asList( pipelineSplitter, regionExtender );
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
                final RegionExecutionPlan regionExecutionPlan = getRegionExecutionPlan( regionExecutionPlans, region );
                final List<PipelineMetrics> regionMetrics = flowMetrics.getRegionMetrics( region.getRegionId() )
                                                                       .stream()
                                                                       .map( pipelineMetricsHistorySummarizer::summarize )
                                                                       .collect( toList() );
                region.updateRegionMetrics( regionExecutionPlan, regionMetrics, loadChangePredicate );
            }

            for ( RegionAdaptationContext region : regions )
            {
                final AdaptationAction adaptationAction = region.resolveIfBottleneck( bottleneckPredicate, bottleneckResolvers );
                if ( adaptationAction != null )
                {
                    this.adaptingRegion = region;

                    return singletonList( adaptationAction );
                }
            }
        }
        else
        {
            final PipelineId adaptingPipelineId = adaptingRegion.getAdaptingPipelineId();
            final PipelineMetricsHistory pipelineMetricsHistory = flowMetrics.getPipelineMetricsHistory( adaptingPipelineId );
            checkArgument( pipelineMetricsHistory != null, "no pipeline metrics history for adapting pipeline: %s", adaptingPipelineId );
            final PipelineMetrics pipelineMetrics = pipelineMetricsHistorySummarizer.summarize( pipelineMetricsHistory );
            final AdaptationAction rollback = adaptingRegion.evaluateAdaptation( pipelineMetrics, adaptationEvaluationPredicate );

            if ( rollback != null )
            {
                final AdaptationAction adaptationAction = adaptingRegion.resolveIfBottleneck( bottleneckPredicate, bottleneckResolvers );

                if ( adaptationAction != null )
                {
                    return asList( rollback, adaptationAction );
                }

                adaptingRegion = null;

                return singletonList( rollback );
            }

            adaptingRegion = null;
        }

        return emptyList();
    }

    private RegionExecutionPlan getRegionExecutionPlan ( final List<RegionExecutionPlan> regionExecutionPlans,
                                                         final RegionAdaptationContext region )
    {
        return regionExecutionPlans.stream()
                                   .filter( plan -> plan.getRegionId() == region.getRegionId() )
                                   .findFirst()
                                   .orElseThrow( () -> new IllegalStateException( "no region execution plan for region: "
                                                                                  + region.getRegionId() ) );
    }

}
