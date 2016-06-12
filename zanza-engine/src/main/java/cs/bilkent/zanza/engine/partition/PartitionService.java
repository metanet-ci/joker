package cs.bilkent.zanza.engine.partition;

import cs.bilkent.zanza.engine.config.ZanzaConfig;

public interface PartitionService
{

    void init ( ZanzaConfig config );

    int getPartitionCount ();

    int[] getOrCreatePartitionDistribution ( int regionId, int replicaCount );

}
