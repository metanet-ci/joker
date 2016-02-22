package cs.bilkent.zanza.engine.pipeline;

public class PipelineInstanceId
{

    public final int regionId;

    public final int pipelineId;

    public final int replicaId;

    private final int hashCode;

    private final String str;

    public PipelineInstanceId ( final int regionId, final int pipelineId, final int replicaId )
    {
        this.regionId = regionId;
        this.pipelineId = pipelineId;
        this.replicaId = replicaId;
        this.hashCode = computeHashCode();
        this.str = computeToString();
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

        final PipelineInstanceId that = (PipelineInstanceId) o;

        if ( regionId != that.regionId )
        {
            return false;
        }
        if ( pipelineId != that.pipelineId )
        {
            return false;
        }
        return replicaId == that.replicaId;

    }

    @Override
    public int hashCode ()
    {
        return hashCode;
    }

    @Override
    public String toString ()
    {
        return str;
    }

    private int computeHashCode ()
    {
        int result = regionId;
        result = 31 * result + pipelineId;
        return 31 * result + replicaId;
    }

    private String computeToString ()
    {
        return "PipelineInstanceId{" +
               "regionId=" + regionId +
               ", pipelineId=" + pipelineId +
               ", replicaId=" + replicaId +
               '}';
    }

}
