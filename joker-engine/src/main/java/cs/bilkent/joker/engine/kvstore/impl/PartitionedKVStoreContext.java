package cs.bilkent.joker.engine.kvstore.impl;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.kvstore.KVStoreContext;
import cs.bilkent.joker.engine.partition.PartitionKey;
import static cs.bilkent.joker.engine.partition.PartitionUtil.getPartitionId;
import cs.bilkent.joker.operator.kvstore.KVStore;
import cs.bilkent.joker.operator.kvstore.impl.KeyDecoratedKVStore;

public class PartitionedKVStoreContext implements KVStoreContext
{

    private final String operatorId;

    private final int replicaIndex;

    private final KVStore[] kvStores;

    private final int partitionCount;

    PartitionedKVStoreContext ( final String operatorId, final int replicaIndex, final KVStore[] kvStores, final int[] partitions )
    {
        this.operatorId = operatorId;
        this.replicaIndex = replicaIndex;
        this.kvStores = Arrays.copyOf( kvStores, kvStores.length );
        this.partitionCount = partitions.length;
        for ( int partitionId = 0; partitionId < partitionCount; partitionId++ )
        {
            if ( partitions[ partitionId ] != replicaIndex )
            {
                this.kvStores[ partitionId ] = null;
            }
        }
    }

    @Override
    public String getOperatorId ()
    {
        return operatorId;
    }

    @Override
    public KVStore getKVStore ( final PartitionKey key )
    {
        if ( key == null )
        {
            return null;
        }

        final int partitionHash = key.partitionHashCode();
        final int partitionId = getPartitionId( partitionHash, partitionCount );
        final KVStore kvStore = kvStores[ partitionId ];
        checkArgument( kvStore != null, "partitionId=% is not in replicaIndex=%", partitionId, replicaIndex );
        return new KeyDecoratedKVStore( key, kvStore );
    }

}
