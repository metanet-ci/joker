package cs.bilkent.zanza.engine.pipeline;

public class PipelineId
{

    public final int regionId;

    public final int pipelineId;

    private final String str;

    private final int hashCode;

    public PipelineId ( final int regionId, final int pipelineId )
    {
        this.regionId = regionId;
        this.pipelineId = pipelineId;
        this.str = "P[" + regionId + "][" + pipelineId + "]";
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

        final PipelineId that = (PipelineId) o;

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
