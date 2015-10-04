package cs.bilkent.zanza.operator.kvstore;

import java.util.Collection;

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

	default Collection<String> fieldNames()
	{
		throw new UnsupportedOperationException();
	}

}
