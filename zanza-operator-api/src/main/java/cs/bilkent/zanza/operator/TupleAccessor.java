package cs.bilkent.zanza.operator;

public final class TupleAccessor
{

    private TupleAccessor ()
    {
    }

    public static void setPartition ( final Tuple tuple, final Object partitionKey, final int partitionHash )
    {
        tuple.setPartition( partitionKey, partitionHash );
    }

    public static void clearPartition ( final Tuple tuple )
    {
        tuple.clearPartition();
    }

}
