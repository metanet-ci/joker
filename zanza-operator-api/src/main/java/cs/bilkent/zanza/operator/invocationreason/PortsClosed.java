package cs.bilkent.zanza.operator.invocationreason;

import cs.bilkent.zanza.operator.InvocationReason;

public class PortsClosed implements InvocationReason
{
    private final int[] ports;

    public PortsClosed(final int[] ports)
    {
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
