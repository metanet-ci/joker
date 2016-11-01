package cs.bilkent.joker.engine.partition;

public interface PartitionService
{

    int getPartitionCount ();

    PartitionDistribution getPartitionDistribution ( int regionId );

    default PartitionDistribution getPartitionDistributionOrFail ( int regionId )
    {
        final PartitionDistribution distribution = getPartitionDistribution( regionId );
        if ( distribution == null )
        {
            throw new IllegalStateException( "regionId=" + regionId + " has no partition distribution!" );
        }

        return distribution;
    }

    PartitionDistribution createPartitionDistribution ( int regionId, int replicaCount );

    PartitionDistribution rebalancePartitionDistribution ( int regionId, int newReplicaCount );

}
