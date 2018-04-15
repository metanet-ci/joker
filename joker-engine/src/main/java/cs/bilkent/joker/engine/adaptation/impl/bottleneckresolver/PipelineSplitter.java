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
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import cs.bilkent.joker.engine.metric.PipelineMetrics;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.operator.utils.Pair;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class PipelineSplitter implements BottleneckResolver
{

    private static final Logger LOGGER = LoggerFactory.getLogger( PipelineSplitter.class );


    private final BiFunction<RegionExecPlan, PipelineMetrics, Integer> pipelineSplitIndexExtractor;

    public PipelineSplitter ( final BiFunction<RegionExecPlan, PipelineMetrics, Integer> pipelineSplitIndexExtractor )
    {
        this.pipelineSplitIndexExtractor = pipelineSplitIndexExtractor;
    }

    SplitPipelineAction resolve ( final RegionExecPlan execPlan, final PipelineMetrics metrics )
    {
        checkArgument( execPlan != null );
        checkArgument( metrics != null );

        final PipelineId pipelineId = metrics.getPipelineId();

        final int operatorCount = execPlan.getOperatorCountByPipelineStartIndex( pipelineId.getPipelineStartIndex() );
        if ( operatorCount < 2 )
        {
            return null;
        }

        final int splitIndex = pipelineSplitIndexExtractor.apply( execPlan, metrics );

        return splitIndex > 0 ? new SplitPipelineAction( execPlan, pipelineId, splitIndex ) : null;
    }

    @Override
    public List<Pair<AdaptationAction, List<PipelineId>>> resolve ( final RegionExecPlan execPlan, final List<PipelineMetrics> metrics )
    {
        RegionExecPlan currentExecPlan = execPlan;
        final List<Pair<AdaptationAction, List<PipelineId>>> actions = new ArrayList<>();
        for ( PipelineMetrics pipelineMetrics : metrics )
        {
            final AdaptationAction action = resolve( currentExecPlan, pipelineMetrics );
            if ( action == null )
            {
                return emptyList();
            }

            actions.add( Pair.of( action, singletonList( pipelineMetrics.getPipelineId() ) ) );
            currentExecPlan = action.getNewExecPlan();
        }

        LOGGER.info( "Region: {} bottlenecks can be resolved with splits: {}", execPlan.getRegionId(), actions );

        return actions;
    }

}
