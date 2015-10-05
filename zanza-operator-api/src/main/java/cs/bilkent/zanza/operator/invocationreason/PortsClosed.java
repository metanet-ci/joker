package cs.bilkent.zanza.operator.invocationreason;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import cs.bilkent.zanza.operator.InvocationReason;

public class PortsClosed implements InvocationReason
{
    private final int[] ports;

    public PortsClosed(final int[] ports)
    {
        checkNotNull( ports, "ports can't be null" );
        checkArgument( ports.length > 0, "ports can't be empty" );
        this.ports = ports;
    }

    @Override
    public boolean isSuccessful()
    {
        return false;
    }

    public int[] getPorts()
    {
        return ports;
    }
}
