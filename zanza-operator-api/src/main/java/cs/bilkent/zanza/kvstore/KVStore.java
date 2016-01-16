package cs.bilkent.zanza.kvstore;


import java.util.Collection;

import cs.bilkent.zanza.operator.Fields;


public interface KVStore extends Fields<Object>
{
    @Override
    default void clear ()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    default int size ()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    default Collection<Object> keys ()
    {
        throw new UnsupportedOperationException();
    }

}
