package cs.bilkent.joker.engine.tuplequeue;

import cs.bilkent.joker.engine.config.ThreadingPref;
import cs.bilkent.joker.engine.partition.PartitionDistribution;
import cs.bilkent.joker.operator.OperatorDef;


public interface OperatorQueueManager
{

    OperatorQueue createDefaultQueue ( int regionId, OperatorDef operatorDef, int replicaIndex, ThreadingPref threadingPref );

    OperatorQueue getDefaultQueue ( int regionId, String operatorId, int replicaIndex );

    default OperatorQueue getDefaultQueueOrFail ( int regionId, String operatorId, int replicaIndex )
    {
        final OperatorQueue queue = getDefaultQueue( regionId, operatorId, replicaIndex );
        if ( queue == null )
        {
            throw new IllegalStateException(
                    "default operator queues not found for regionId=" + regionId + " replicaIndex=" + replicaIndex + " operatorId="
                    + operatorId );
        }

        return queue;
    }

    default OperatorQueue[] createPartitionedQueues ( int regionId, OperatorDef operatorDef, PartitionDistribution partitionDistribution )
    {
        return createPartitionedQueues( regionId, operatorDef, partitionDistribution, operatorDef.getPartitionFieldNames().size() );
    }

    OperatorQueue[] createPartitionedQueues ( int regionId,
                                              OperatorDef operatorDef,
                                              PartitionDistribution partitionDistribution,
                                              int forwardedKeySize );

    OperatorQueue[] rebalancePartitionedQueues ( int regionId,
                                                 OperatorDef operatorDef,
                                                 PartitionDistribution currentPartitionDistribution,
                                                 PartitionDistribution newPartitionDistribution );

    OperatorQueue[] getPartitionedQueues ( int regionId, String operatorId );

    default OperatorQueue getPartitionedQueueOrFail ( int regionId, String operatorId, int replicaIndex )
    {
        return getPartitionedQueuesOrFail( regionId, operatorId )[ replicaIndex ];
    }

    default OperatorQueue[] getPartitionedQueuesOrFail ( int regionId, String operatorId )
    {
        final OperatorQueue[] operatorQueues = getPartitionedQueues( regionId, operatorId );
        if ( operatorQueues == null )
        {
            throw new IllegalStateException(
                    "partitioned operator queues not found for regionId=" + regionId + " operatorId=" + operatorId );
        }

        return operatorQueues;
    }

    void releaseDefaultQueue ( int regionId, String operatorId, int replicaIndex );

    void releasePartitionedQueues ( int regionId, String operatorId );

    OperatorQueue switchThreadingPref ( int regionId, String operatorId, int replicaIndex );
}
