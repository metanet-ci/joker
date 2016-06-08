package cs.bilkent.zanza.engine.pipeline;

import java.util.Arrays;

import static cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus.ACTIVE;

public class UpstreamContext
{

    private final int version;

    private final UpstreamConnectionStatus[] statuses;

    public UpstreamContext ( final int version, final UpstreamConnectionStatus[] statuses )
    {
        this.version = version;
        this.statuses = statuses;
    }

    public int getVersion ()
    {
        return version;
    }

    public UpstreamConnectionStatus getUpstreamConnectionStatus ( int index )
    {
        return statuses[ index ];
    }

    public boolean isActiveConnectionPresent ()
    {
        for ( UpstreamConnectionStatus status : statuses )
        {
            if ( status == ACTIVE )
            {
                return true;
            }
        }

        return false;
    }

    public boolean isActiveConnectionAbsent ()
    {
        return !isActiveConnectionPresent();
    }

    public boolean[] getUpstreamConnectionStatuses ()
    {
        final boolean[] b = new boolean[ statuses.length ];
        for ( int portIndex = 0; portIndex < statuses.length; portIndex++ )
        {
            b[ portIndex ] = statuses[ portIndex ] == ACTIVE;
        }

        return b;
    }

    public int getActiveConnectionCount ()
    {
        int count = 0;
        for ( UpstreamConnectionStatus status : statuses )
        {
            if ( status == ACTIVE )
            {
                count++;
            }
        }

        return count;
    }

    public UpstreamContext withUpstreamConnectionStatus ( final int portIndex, final UpstreamConnectionStatus newStatus )
    {
        final UpstreamConnectionStatus[] statuses = Arrays.copyOf( this.statuses, this.statuses.length );
        statuses[ portIndex ] = newStatus;
        return new UpstreamContext( this.version + 1, statuses );
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

        final UpstreamContext that = (UpstreamContext) o;

        if ( version != that.version )
        {
            return false;
        }

        return Arrays.equals( statuses, that.statuses );
    }

    @Override
    public int hashCode ()
    {
        int result = version;
        result = 31 * result + Arrays.hashCode( statuses );
        return result;
    }

    @Override
    public String toString ()
    {
        return "UpstreamContext{" +
               "version=" + version +
               ", statuses=" + Arrays.toString( statuses ) +
               '}';
    }

}
