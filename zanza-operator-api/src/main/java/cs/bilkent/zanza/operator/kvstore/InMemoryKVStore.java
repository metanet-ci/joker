package cs.bilkent.zanza.operator.kvstore;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkNotNull;

public class InMemoryKVStore implements KVStore
{
    private final Map<String, Object> values = new ConcurrentHashMap<>();

    public InMemoryKVStore()
    {
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T get ( final String key )
    {
        checkNotNull( key, "key can't be null" );
        return (T) values.get( key );
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getOrDefault ( final String key, final T defaultVal )
    {
        checkNotNull( key, "key can't be null" );
        return (T) values.getOrDefault( key, defaultVal );
    }

    @Override
    public boolean contains ( final String key )
    {
        checkNotNull( key, "key can't be null" );
        return values.containsKey( key );
    }

    @Override
    public void set ( final String key, final Object value )
    {
        checkNotNull( key, "key can't be null" );
        checkNotNull( value, "value can't be null" );
        values.put( key, value );
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T put ( final String key, final T value )
    {
        checkNotNull( key, "key can't be null" );
        checkNotNull( value, "value can't be null" );
        return (T) values.put( key, value );
    }

    @Override
    public Object remove ( final String key )
    {
        checkNotNull( key, "key can't be null" );
        return values.remove( key );
    }

    @Override
    public boolean delete ( final String key )
    {
        checkNotNull( key, "key can't be null" );
        return values.remove( key ) != null;
    }
}
