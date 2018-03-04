package cs.bilkent.joker.engine.pipeline;

import cs.bilkent.joker.engine.flow.PipelineId;

public class PipelineReplicaId
{

    public final PipelineId pipelineId;

    public final int replicaIndex;

    private final String str;

    private final int hashCode;

    public PipelineReplicaId ( final int regionId, final int pipelineStartIndex, final int replicaIndex )
    {
        this( new PipelineId( regionId, pipelineStartIndex ), replicaIndex );
    }

    public PipelineReplicaId ( PipelineId pipelineId, final int replicaIndex )
    {
        this.pipelineId = pipelineId;
        this.replicaIndex = replicaIndex;
        this.str = pipelineId.toString() + "[" + replicaIndex + "]";
        ;
        this.hashCode = str.hashCode();
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

        final PipelineReplicaId that = (PipelineReplicaId) o;

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
