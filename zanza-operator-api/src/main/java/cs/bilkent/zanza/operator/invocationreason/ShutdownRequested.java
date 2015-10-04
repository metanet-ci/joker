package cs.bilkent.zanza.operator.invocationreason;

import cs.bilkent.zanza.operator.InvocationReason;

public class ShutdownRequested implements InvocationReason
{
    public static final InvocationReason INSTANCE = new ShutdownRequested();

    private ShutdownRequested()
    {

    }

    @Override
    public boolean isSuccessful()
    {
        return false;
    }
}
