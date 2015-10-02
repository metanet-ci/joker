package cs.bilkent.zanza.operator.kvstore;

import cs.bilkent.zanza.operator.Fields;

public interface KVStore extends Fields
{

	default void clear()
	{
		throw new UnsupportedOperationException();
	}

	default int size()
	{
		throw new UnsupportedOperationException();
	}

}
