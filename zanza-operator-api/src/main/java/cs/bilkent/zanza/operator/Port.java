package cs.bilkent.zanza.operator;

public class Port
{
    public static final int DEFAULT_PORT_INDEX = 0;

    public final String operatorName;

    public final int portIndex;

    public Port(String operatorName, int portIndex)
    {
        this.operatorName = operatorName;
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
        return operatorName.equals( port.operatorName );

    }

    @Override
    public int hashCode ()
    {
        int result = operatorName.hashCode();
        result = 31 * result + portIndex;
        return result;
    }
}
