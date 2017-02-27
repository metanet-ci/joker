package cs.bilkent.joker.flow;


import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkArgument;

/**
 * Endpoint of a connection which connects two operators
 */
public final class Port
{
    public static final int DYNAMIC_PORT_COUNT = -1;

    public static final int DEFAULT_PORT_INDEX = 0;

    private final String operatorId;

    private final int portIndex;

    public Port ( String operatorId, int portIndex )
    {
        checkArgument( operatorId != null, "operator id can't be null" );
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

        return portIndex == port.portIndex && operatorId.equals( port.operatorId );
    }

    public String getOperatorId ()
    {
        return operatorId;
    }

    public int getPortIndex ()
    {
        return portIndex;
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
        return "Port{" + "operatorId='" + operatorId + '\'' + ", portIndex=" + portIndex + '}';
    }

}
