package cs.bilkent.zanza.engine.region;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static cs.bilkent.zanza.operator.spec.OperatorType.STATEFUL;

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
        checkArgument( region.getRegionType() != STATEFUL || replicaCount == 1 );
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

    public List<Integer> getPipelineStartIndices ()
    {
        return pipelineStartIndices;
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
