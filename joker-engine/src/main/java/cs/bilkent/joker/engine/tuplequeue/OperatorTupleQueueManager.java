package cs.bilkent.joker.engine.tuplequeue;

import cs.bilkent.joker.engine.config.ThreadingPreference;
import cs.bilkent.joker.engine.partition.PartitionDistribution;
import cs.bilkent.joker.operator.OperatorDef;


public interface OperatorTupleQueueManager
{

    OperatorTupleQueue createDefaultOperatorTupleQueue ( int regionId,
                                                         int replicaIndex,
                                                         OperatorDef operatorDef,
                                                         ThreadingPreference threadingPreference );

    OperatorTupleQueue getDefaultOperatorTupleQueue ( int regionId, int replicaIndex, String operatorId );

    default OperatorTupleQueue getDefaultOperatorTupleQueueOrFail ( int regionId, int replicaIndex, String operatorId )
    {
        final OperatorTupleQueue operatorTupleQueue = getDefaultOperatorTupleQueue( regionId, replicaIndex, operatorId );
        if ( operatorTupleQueue == null )
        {
            throw new IllegalStateException( "default operator tuple queues not found for regionId=" + regionId + " replicaIndex="
                                             + replicaIndex + " operatorId=" + operatorId );
        }

        return operatorTupleQueue;
    }

    default OperatorTupleQueue[] createPartitionedOperatorTupleQueues ( int regionId,
                                                                        OperatorDef operatorDef,
                                                                        PartitionDistribution partitionDistribution )
    {
        return createPartitionedOperatorTupleQueues( regionId,
                                                     operatorDef,
                                                     partitionDistribution,
                                                     operatorDef.partitionFieldNames().size() );
    }

    OperatorTupleQueue[] createPartitionedOperatorTupleQueues ( int regionId,
                                                                OperatorDef operatorDef,
                                                                PartitionDistribution partitionDistribution,
                                                                int forwardKeyLimit );

    OperatorTupleQueue[] rebalancePartitionedOperatorTupleQueues ( int regionId,
                                                                   OperatorDef operatorDef,
                                                                   PartitionDistribution currentPartitionDistribution,
                                                                   PartitionDistribution newPartitionDistribution );

    OperatorTupleQueue[] getPartitionedOperatorTupleQueues ( int regionId, String operatorId );

    default OperatorTupleQueue[] getPartitionedOperatorTupleQueuesOrFail ( int regionId, String operatorId )
    {
        final OperatorTupleQueue[] operatorTupleQueues = getPartitionedOperatorTupleQueues( regionId, operatorId );
        if ( operatorTupleQueues == null )
        {
            throw new IllegalStateException( "partitioned operator tuple queues not found for regionId=" + regionId + " operatorId="
                                             + operatorId );
        }

        return operatorTupleQueues;
    }

    void releaseDefaultOperatorTupleQueue ( int regionId, int replicaIndex, String operatorId );

    void releasePartitionedOperatorTupleQueues ( int regionId, String operatorId );

    OperatorTupleQueue switchThreadingPreference ( int regionId, int replicaIndex, String operatorId );
}
