package cs.bilkent.joker.engine.partition;

public final class PartitionUtil
{

    public static int getPartitionId ( final Object key, final int partitionCount )
    {
        return getPartitionId( key.hashCode(), partitionCount );
    }

    public static int getPartitionId ( final int hashCode, final int partitionCount )
    {
        final int partitionId = hashCode % partitionCount;
        return partitionId >= 0 ? partitionId : ( partitionId + partitionCount );
    }

    private PartitionUtil ()
    {

    }

}
