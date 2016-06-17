package cs.bilkent.zanza.engine.partition;

public final class PartitionUtil
{

    public static int getPartitionId ( final Object key, final int partitionCount )
    {
        final int partitionId = key.hashCode() % partitionCount;
        return partitionId >= 0 ? partitionId : ( partitionId + partitionCount );
    }

    private PartitionUtil ()
    {

    }

}
