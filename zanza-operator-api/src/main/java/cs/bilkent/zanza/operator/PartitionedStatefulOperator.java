package cs.bilkent.zanza.operator;

import cs.bilkent.zanza.operator.store.KVStore;

public abstract class PartitionedStatefulOperator extends AbstractOperator
{

	protected <K, V> KVStore<K, V> getKVStore()
	{
		return null; // TODO
	}

}
