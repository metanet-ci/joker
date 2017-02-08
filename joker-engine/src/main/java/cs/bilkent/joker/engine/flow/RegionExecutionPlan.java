package cs.bilkent.joker.engine.flow;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.pipeline.PipelineId;
import cs.bilkent.joker.engine.region.RegionDef;
import cs.bilkent.joker.operator.OperatorDef;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toList;

public class RegionExecutionPlan
{

    private final RegionDef regionDef;

    private final int replicaCount;

    private final List<Integer> pipelineStartIndices;

    public RegionExecutionPlan ( final RegionDef regionDef, final List<Integer> pipelineStartIndices, final int replicaCount )
    {
        checkArgument( ( regionDef.getRegionType() == STATEFUL || regionDef.getRegionType() == STATELESS )
                       ? replicaCount == 1
                       : replicaCount > 0 );
        validatePipelineStartIndices( regionDef.getOperators(), pipelineStartIndices );
        this.regionDef = regionDef;
        this.replicaCount = replicaCount;
        this.pipelineStartIndices = unmodifiableList( new ArrayList<>( pipelineStartIndices ) );
    }

    private void validatePipelineStartIndices ( final List<OperatorDef> operators, final List<Integer> pipelineStartIndices )
    {
        if ( pipelineStartIndices.get( 0 ) != 0 )
        {
            pipelineStartIndices.add( 0, 0 );
        }
        checkArgument( pipelineStartIndices.size() <= operators.size() );
        int i = -1;
        for ( int startIndex : pipelineStartIndices )
        {
            checkArgument( startIndex > i, "invalid pipeline start indices: ", pipelineStartIndices );
            i = startIndex;
        }
        checkArgument( i < operators.size() );
    }

    public int getRegionId ()
    {
        return regionDef.getRegionId();
    }

    public RegionDef getRegionDef ()
    {
        return regionDef;
    }

    public int getReplicaCount ()
    {
        return replicaCount;
    }

    public int getPipelineCount ()
    {
        return pipelineStartIndices.size();
    }

    public List<PipelineId> getPipelineIds ()
    {
        return pipelineStartIndices.stream().map( i -> new PipelineId( regionDef.getRegionId(), i ) ).collect( toList() );
    }

    public List<Integer> getPipelineStartIndices ()
    {
        return pipelineStartIndices;
    }

    public int getPipelineStartIndex ( final int pipelineIndex )
    {
        return pipelineStartIndices.get( pipelineIndex );
    }

    public int getPipelineIndex ( final int pipelineStartIndex )
    {
        final int index = pipelineStartIndices.indexOf( pipelineStartIndex );
        checkArgument( index != -1, "invalid pipeline start index: " + pipelineStartIndex );

        return index;
    }

    public int getOperatorCountByPipelineStartIndex ( final int pipelineStartIndex )
    {
        return getOperatorDefsByPipelineStartIndex( pipelineStartIndex ).length;
    }

    public int getOperatorCountByPipelineIndex ( final int pipelineIndex )
    {
        return getOperatorDefsByPipelineIndex( pipelineIndex ).length;
    }

    public OperatorDef[] getOperatorDefsByPipelineStartIndex ( final int pipelineStartIndex )
    {
        final int index = getPipelineIndex( pipelineStartIndex );
        return getOperatorDefsByPipelineIndex( index );
    }

    public OperatorDef[] getOperatorDefsByPipelineIndex ( final int pipelineIndex )
    {
        final List<OperatorDef> operators = regionDef.getOperators();
        final int startIndex = pipelineStartIndices.get( pipelineIndex );
        final int endIndex =
                pipelineIndex + 1 < pipelineStartIndices.size() ? pipelineStartIndices.get( pipelineIndex + 1 ) : operators.size();
        final List<OperatorDef> operatorDefs = operators.subList( startIndex, endIndex );
        final OperatorDef[] operatorDefsArr = new OperatorDef[ operatorDefs.size() ];
        operatorDefs.toArray( operatorDefsArr );
        return operatorDefsArr;
    }

    @Override
    public String toString ()
    {
        return "RegionExecutionPlan{" + "regionDef=" + regionDef + ", replicaCount=" + replicaCount + ", pipelineStartIndices="
               + pipelineStartIndices + '}';
    }

}
