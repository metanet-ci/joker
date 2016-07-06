package cs.bilkent.zanza.engine.region;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.flow.OperatorDefinition;
import static cs.bilkent.zanza.operator.spec.OperatorType.PARTITIONED_STATEFUL;

public class RegionRuntimeConfig
{

    private final int regionId;

    private final RegionDefinition region;

    private final int replicaCount;

    private final List<Integer> pipelineStartIndices;

    public RegionRuntimeConfig ( final int regionId,
                                 final RegionDefinition region,
                                 final int replicaCount,
                                 final List<Integer> pipelineStartIndices )
    {
        checkArgument( ( region.getRegionType() == PARTITIONED_STATEFUL && replicaCount > 0 ) || replicaCount == 1 );
        this.regionId = regionId;
        this.region = region;
        this.replicaCount = replicaCount;
        this.pipelineStartIndices = pipelineStartIndices;
    }

    public int getRegionId ()
    {
        return regionId;
    }

    public RegionDefinition getRegion ()
    {
        return region;
    }

    public int getReplicaCount ()
    {
        return replicaCount;
    }

    public int getPipelineCount ()
    {
        return pipelineStartIndices.size();
    }

    public List<Integer> getPipelineStartIndices ()
    {
        return pipelineStartIndices;
    }

    public OperatorDefinition[] getOperatorDefinitions ( final int pipelineId )
    {
        final List<OperatorDefinition> operators = region.getOperators();
        final int startIndex = pipelineStartIndices.get( pipelineId );
        final int endIndex = startIndex + 1 < pipelineStartIndices.size() ? pipelineStartIndices.get( startIndex + 1 ) : operators.size();
        final List<OperatorDefinition> operatorDefinitions = operators.subList( startIndex, endIndex );
        final OperatorDefinition[] operatorDefinitionsArr = new OperatorDefinition[ operatorDefinitions.size() ];
        operatorDefinitions.toArray( operatorDefinitionsArr );
        return operatorDefinitionsArr;
    }

    @Override
    public String toString ()
    {
        return "RegionRuntimeConfig{" +
               "regionId=" + regionId +
               ", region=" + region +
               ", replicaCount=" + replicaCount +
               ", pipelineStartIndices=" + pipelineStartIndices +
               '}';
    }

}
