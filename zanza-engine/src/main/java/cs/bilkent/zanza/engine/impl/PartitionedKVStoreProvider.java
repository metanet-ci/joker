package cs.bilkent.zanza.engine.impl;

import java.util.List;
import java.util.function.Function;

import cs.bilkent.zanza.engine.kvstore.KVStoreContext;
import cs.bilkent.zanza.kvstore.KVStore;
import cs.bilkent.zanza.operator.PortsToTuples;


public class PartitionedKVStoreProvider implements Function<PortsToTuples, KVStore>
{

    private final KVStoreContext kvStoreContext;

    private final List<String> partitionFieldNames;

    public PartitionedKVStoreProvider ( final KVStoreContext kvStoreContext, final List<String> partitionFieldNames )
    {
        this.kvStoreContext = kvStoreContext;
        this.partitionFieldNames = partitionFieldNames;
    }

    @Override
    public KVStore apply ( final PortsToTuples portsToTuples )
    {
        final int randomPort = portsToTuples.getPortCount() - 1;
        final Object partitionKey = portsToTuples.getTuple( randomPort, 0 ).getValues( partitionFieldNames );
        return kvStoreContext.getKVStore( partitionKey );
    }

}
