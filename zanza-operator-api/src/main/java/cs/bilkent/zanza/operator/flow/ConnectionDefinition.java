package cs.bilkent.zanza.operator.flow;


import cs.bilkent.zanza.operator.Port;


public class ConnectionDefinition
{
    public final Port source;

    public final Port target;

    public ConnectionDefinition ( final Port source, final Port target )
    {
        this.source = source;
        this.target = target;
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

        final ConnectionDefinition that = (ConnectionDefinition) o;

        if ( !source.equals( that.source ) )
        {
            return false;
        }
        return target.equals( that.target );

    }

    @Override
    public int hashCode ()
    {
        int result = source.hashCode();
        result = 31 * result + target.hashCode();
        return result;
    }
}
