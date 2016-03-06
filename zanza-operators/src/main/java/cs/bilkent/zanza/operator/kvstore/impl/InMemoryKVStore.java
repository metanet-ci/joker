package cs.bilkent.zanza.operator.kvstore.impl;


import java.util.HashMap;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;

import static com.google.common.base.Preconditions.checkNotNull;
import cs.bilkent.zanza.operator.kvstore.KVStore;

@NotThreadSafe
public class InMemoryKVStore implements KVStore
{
    private final Map<Object, Object> values = new HashMap<>();

    public InMemoryKVStore ()
    {
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public <T> T get ( final Object key )
    {
        checkNotNull( key, "key can't be null" );
        return (T) values.get( key );
    }

    @Override
    public boolean contains ( final Object key )
    {
        checkNotNull( key, "key can't be null" );
        return values.containsKey( key );
    }

    @Override
    public void set ( final Object key, final Object value )
    {
        checkNotNull( key, "key can't be null" );
        checkNotNull( value, "value can't be null" );
        values.put( key, value );
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public <T> T put ( final Object key, final T value )
    {
        checkNotNull( key, "key can't be null" );
        checkNotNull( value, "value can't be null" );
        return (T) values.put( key, value );
    }

    @Override
    public Object remove ( final Object key )
    {
        checkNotNull( key, "key can't be null" );
        return values.remove( key );
    }

    @Override
    public boolean delete ( final Object key )
    {
        checkNotNull( key, "key can't be null" );
        return values.remove( key ) != null;
    }

    @Override
    public void clear ()
    {
        values.clear();
    }

    @Override
    public int size ()
    {
        return values.size();
    }

}
