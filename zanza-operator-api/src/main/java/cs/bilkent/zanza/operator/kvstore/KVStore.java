package cs.bilkent.zanza.operator.kvstore;

import java.util.Collection;

import cs.bilkent.zanza.operator.Fields;

public interface KVStore extends Fields
{
    @Override
    default void clear()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    default int size()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    default Collection<String> fieldNames()
    {
        throw new UnsupportedOperationException();
    }

}
