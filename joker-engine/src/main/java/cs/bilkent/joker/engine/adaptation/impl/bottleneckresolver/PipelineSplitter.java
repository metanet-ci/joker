package cs.bilkent.joker.engine.adaptation.impl.bottleneckresolver;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cs.bilkent.joker.engine.adaptation.AdaptationAction;
import cs.bilkent.joker.engine.adaptation.BottleneckResolver;
import cs.bilkent.joker.engine.adaptation.impl.adaptationaction.SplitPipelineAction;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.engine.metric.PipelineMetrics;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.utils.Pair;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class PipelineSplitter implements BottleneckResolver
{

    private static final Logger LOGGER = LoggerFactory.getLogger( PipelineSplitter.class );


    private final BiFunction<RegionExecutionPlan, PipelineMetrics, Integer> pipelineSplitIndexExtractor;

    public PipelineSplitter ( final BiFunction<RegionExecutionPlan, PipelineMetrics, Integer> pipelineSplitIndexExtractor )
    {
        this.pipelineSplitIndexExtractor = pipelineSplitIndexExtractor;
    }

    SplitPipelineAction resolve ( final RegionExecutionPlan regionExecutionPlan, final PipelineMetrics bottleneckPipelineMetrics )
    {
        checkArgument( regionExecutionPlan != null );
        checkArgument( bottleneckPipelineMetrics != null );

        final PipelineId pipelineId = bottleneckPipelineMetrics.getPipelineId();

        final int operatorCount = regionExecutionPlan.getOperatorCountByPipelineStartIndex( pipelineId.getPipelineStartIndex() );
        if ( operatorCount < 2 )
        {
            return null;
        }

        final int splitIndex = pipelineSplitIndexExtractor.apply( regionExecutionPlan, bottleneckPipelineMetrics );

        return splitIndex > 0 ? new SplitPipelineAction( regionExecutionPlan, pipelineId, splitIndex ) : null;
    }

    @Override
    public List<Pair<AdaptationAction, List<PipelineId>>> resolve ( final RegionExecutionPlan regionExecutionPlan,
                                                                    final List<PipelineMetrics> bottleneckPipelinesMetrics )
    {
        RegionExecutionPlan currentRegionExecutionPlan = regionExecutionPlan;
        final List<Pair<AdaptationAction, List<PipelineId>>> adaptationActions = new ArrayList<>();
        for ( PipelineMetrics pipelineMetrics : bottleneckPipelinesMetrics )
        {
            final AdaptationAction adaptationAction = resolve( currentRegionExecutionPlan, pipelineMetrics );
            if ( adaptationAction == null )
            {
                return emptyList();
            }

            adaptationActions.add( Pair.of( adaptationAction, singletonList( pipelineMetrics.getPipelineId() ) ) );
            currentRegionExecutionPlan = adaptationAction.getNewRegionExecutionPlan();
        }

        LOGGER.info( "Region: {} bottlenecks can be resolved with splits: {}", regionExecutionPlan.getRegionId(), adaptationActions );

        return adaptationActions;
    }

}
