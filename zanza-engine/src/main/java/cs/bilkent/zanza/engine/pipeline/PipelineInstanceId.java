package cs.bilkent.zanza.engine.pipeline;

public class PipelineInstanceId
{

    public static PipelineInstanceId of ( final int regionId, final int pipelineId, final int replicaId )
    {
        final String str = "PP<" + regionId + "_" + "_" + pipelineId + "_" + replicaId + ">";
        return new PipelineInstanceId( str );
    }

    private final String id;

    private final int hashCode;

    private PipelineInstanceId ( final String id )
    {
        this.id = id;
        this.hashCode = id.hashCode();
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

        return id.equals( that.id );

    }
    
    @Override
    public int hashCode ()
    {
        return hashCode;
    }

    @Override
    public String toString ()
    {
        return id;
    }

}
