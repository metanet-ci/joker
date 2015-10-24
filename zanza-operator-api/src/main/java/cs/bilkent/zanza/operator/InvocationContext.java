package cs.bilkent.zanza.operator;

public interface InvocationContext
{

    enum InvocationReason
    {

        SUCCESS
                {
                    public boolean isSuccessful ()
                    {
                        return true;
                    }
                },
        SHUTDOWN
                {
                    public boolean isSuccessful ()
                    {
                        return false;
                    }
                },
        CLOSED_PORT
                {
                    public boolean isSuccessful ()
                    {
                        return false;
                    }
                };

        boolean isSuccessful ()
        {
            return isSuccessful();
        }
    }

    PortsToTuples getTuples ();

    InvocationReason getReason ();

    KVStore getKVStore ();

    default boolean isSuccessfulInvocation ()
    {
        return getReason().isSuccessful();
    }
}
