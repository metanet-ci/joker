package cs.bilkent.zanza.engine.region;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.flow.OperatorDef;
import static cs.bilkent.zanza.operator.spec.OperatorType.PARTITIONED_STATEFUL;

public class RegionConfig
{

    private final RegionDef regionDef;

    private final int replicaCount;

    private final List<Integer> pipelineStartIndices;

    public RegionConfig ( final RegionDef regionDef, final List<Integer> pipelineStartIndices, final int replicaCount )
    {
        checkArgument( ( regionDef.getRegionType() == PARTITIONED_STATEFUL && replicaCount > 0 ) || replicaCount == 1 );
        this.regionDef = regionDef;
        this.replicaCount = replicaCount;
        this.pipelineStartIndices = pipelineStartIndices;
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

    public List<Integer> getPipelineStartIndices ()
    {
        return pipelineStartIndices;
    }

    public OperatorDef[] getOperatorDefs ( final int pipelineId )
    {
        final List<OperatorDef> operators = regionDef.getOperators();
        final int startIndex = pipelineStartIndices.get( pipelineId );
        final int endIndex = startIndex + 1 < pipelineStartIndices.size() ? pipelineStartIndices.get( startIndex + 1 ) : operators.size();
        final List<OperatorDef> operatorDefs = operators.subList( startIndex, endIndex );
        final OperatorDef[] operatorDefsArr = new OperatorDef[ operatorDefs.size() ];
        operatorDefs.toArray( operatorDefsArr );
        return operatorDefsArr;
    }

    @Override
    public String toString ()
    {
        return "RegionConfig{" + "regionDef=" + regionDef + ", replicaCount=" + replicaCount + ", pipelineStartIndices="
               + pipelineStartIndices + '}';
    }

}
