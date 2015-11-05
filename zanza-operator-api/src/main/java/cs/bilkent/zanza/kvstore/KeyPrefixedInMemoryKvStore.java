package cs.bilkent.zanza.kvstore;

import cs.bilkent.zanza.util.Pair;

public class KeyPrefixedInMemoryKvStore implements KVStore
{

    private final Object prefix;

    private final InMemoryKVStore kvStore;

    public KeyPrefixedInMemoryKvStore ( final Object prefix, final InMemoryKVStore kvStore )
    {
        this.prefix = prefix;
        this.kvStore = kvStore;
    }

    @Override
    public <T> T get ( final Object key )
    {
        return this.kvStore.get( Pair.of( prefix, key ) );
    }

    @Override
    public boolean contains ( final Object key )
    {
        return this.kvStore.contains( Pair.of( prefix, key ) );
    }

    @Override
    public void set ( final Object key, final Object value )
    {
        this.kvStore.set( Pair.of( prefix, key ), value );
    }

    @Override
    public <T> T put ( final Object key, final T value )
    {
        return this.kvStore.put( Pair.of( prefix, key ), value );
    }

    @Override
    public Object remove ( final Object key )
    {
        return this.kvStore.remove( Pair.of( prefix, key ) );
    }

    @Override
    public boolean delete ( final Object key )
    {
        return this.kvStore.delete( Pair.of( prefix, key ) );
    }
}
