package cs.bilkent.zanza.engine.pipeline;

public class PipelineInstanceId
{

    private final int regionId;

    private final int replicaIndex;

    private final int pipelineId;

    private final String str;

    private final int hashCode;

    public PipelineInstanceId ( final int regionId, final int replicaIndex, final int pipelineId )
    {
        this.regionId = regionId;
        this.replicaIndex = replicaIndex;
        this.pipelineId = pipelineId;
        this.str = "PP[" + regionId + "_" + replicaIndex + "_" + pipelineId + "]";
        ;
        this.hashCode = str.hashCode();
    }

    public int regionId ()
    {
        return regionId;
    }

    public int getReplicaIndex ()
    {
        return replicaIndex;
    }

    public int pipelineId ()
    {
        return pipelineId;
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

        return str.equals( that.str );
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

}
