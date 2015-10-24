package cs.bilkent.zanza.operator;

import java.util.Collection;

public interface KVStore extends Fields<Object>
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
    default Collection<Object> keys ()
    {
        throw new UnsupportedOperationException();
    }

}
