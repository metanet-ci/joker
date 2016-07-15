package cs.bilkent.zanza.flow;


import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;


public final class Port
{
    public static final int DYNAMIC_PORT_COUNT = -1;

    public static final int DEFAULT_PORT_INDEX = 0;

    public final String operatorId;

    public final int portIndex;

    public Port ( String operatorId, int portIndex )
    {
        checkNotNull( operatorId, "operator id can't be null" );
        checkArgument( portIndex >= 0, "port must be non-negative" );
        this.operatorId = operatorId;
        this.portIndex = portIndex;
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

        final Port port = (Port) o;

        if ( portIndex != port.portIndex )
        {
            return false;
        }
        return operatorId.equals( port.operatorId );

    }

    @Override
    public int hashCode ()
    {
        int result = operatorId.hashCode();
        result = 31 * result + portIndex;
        return result;
    }

    @Override
    public String toString ()
    {
        return "Port{" +
               "operatorId='" + operatorId + '\'' +
               ", portIndex=" + portIndex +
               '}';
    }

}
