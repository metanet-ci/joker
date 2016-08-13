package cs.bilkent.joker.operator.kvstore.impl;


import cs.bilkent.joker.operator.kvstore.KVStore;
import cs.bilkent.joker.utils.Pair;


public class KeyDecoratedKVStore implements KVStore
{

    private final Object prefix;

    private final KVStore kvStore;

    public KeyDecoratedKVStore ( final Object prefix, final KVStore kvStore )
    {
        this.prefix = prefix;
        this.kvStore = kvStore;
    }

    @Override
    public <T> T get ( final Object key )
    {
        return kvStore.get( Pair.of( prefix, key ) );
    }

    @Override
    public boolean contains ( final Object key )
    {
        return kvStore.contains( Pair.of( prefix, key ) );
    }

    @Override
    public void set ( final Object key, final Object value )
    {
        kvStore.set( Pair.of( prefix, key ), value );
    }

    @Override
    public <T> T put ( final Object key, final T value )
    {
        return kvStore.put( Pair.of( prefix, key ), value );
    }

    @Override
    public Object remove ( final Object key )
    {
        return kvStore.remove( Pair.of( prefix, key ) );
    }

    @Override
    public boolean delete ( final Object key )
    {
        return kvStore.delete( Pair.of( prefix, key ) );
    }

    public KVStore getKvStore ()
    {
        return kvStore;
    }

}
