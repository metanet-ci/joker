package cs.bilkent.joker.engine.pipeline;

import static java.lang.Integer.compare;

public class PipelineId implements Comparable<PipelineId>
{

    private final int regionId;

    private final int pipelineStartIndex;

    private final String str;

    private final int hashCode;

    public PipelineId ( final int regionId, final int pipelineStartIndex )
    {
        this.regionId = regionId;
        this.pipelineStartIndex = pipelineStartIndex;
        this.str = "P[" + regionId + "][" + pipelineStartIndex + "]";
        this.hashCode = computeHashCode();
    }

    public int getRegionId ()
    {
        return regionId;
    }

    public int getPipelineStartIndex ()
    {
        return pipelineStartIndex;
    }

    @Override
    public boolean equals ( final Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        final PipelineId that = (PipelineId) o;

        return regionId == that.regionId && pipelineStartIndex == that.pipelineStartIndex;
    }

    @Override
    public int hashCode ()
    {
        return hashCode;
    }

    private int computeHashCode ()
    {
        int result = regionId;
        result = 31 * result + pipelineStartIndex;
        return result;
    }

    @Override
    public String toString ()
    {
        return str;
    }

    @Override
    public int compareTo ( final PipelineId other )
    {
        int r = compare( this.regionId, other.regionId );
        return r != 0 ? r : compare( this.pipelineStartIndex, other.pipelineStartIndex );
    }

}
