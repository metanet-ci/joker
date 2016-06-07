package cs.bilkent.zanza.engine.pipeline;

public class PipelineInstanceId
{

    private final int pipelineId;

    private final int regionId;

    private final int replicaId;

    private final String str;

    private final int hashCode;

    public PipelineInstanceId ( final int regionId, final int pipelineId, final int replicaId )
    {
        this.pipelineId = pipelineId;
        this.replicaId = replicaId;
        this.regionId = regionId;
        this.str = "PP[" + regionId + "_" + pipelineId + "_" + replicaId + "]";
        ;
        this.hashCode = str.hashCode();
    }

    public int regionId ()
    {
        return regionId;
    }

    public int pipelineId ()
    {
        return pipelineId;
    }

    public int replicaId ()
    {
        return replicaId;
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
