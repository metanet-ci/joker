package cs.bilkent.zanza.operator;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

// TODO Comparable, Serializable, Iterable
// supports immutable or effectively final objects
public class Tuple implements Fields
{
    private final Map<String, Object> values;

    public Tuple()
    {
        this.values = new HashMap<>();
    }

    public Tuple(final boolean withInsertionOrder)
    {
        this.values = withInsertionOrder ? new LinkedHashMap<>() : new HashMap<>();
    }

    public Tuple ( final String key, final Object value )
    {
        checkNotNull( key, "key can't be null" );
        checkNotNull( value, "value can't be null" );
        this.values = new HashMap<>();
        this.values.put( key, value );
    }

    public Tuple(final Map<String, Object> values)
    {
        checkNotNull( values, "values can't be null" );
        this.values = new HashMap<>();
        this.values.putAll(values);
    }

    public Map<String, Object> getKeysValues ()
    {
        return values;
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
        this.values.put( key, value );
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T put ( final String key, final T value )
    {
        checkNotNull( key, "key can't be null" );
        checkNotNull( value, "value can't be null" );
        return (T) this.values.put( key, value );
    }

    @Override
    public Object remove ( final String key )
    {
        checkNotNull( key, "key can't be null" );
        return this.values.remove( key );
    }

    @Override
    public boolean delete ( final String key )
    {
        checkNotNull( key, "key can't be null" );
        return this.values.remove( key ) != null;
    }

    @Override
    public void clear()
    {
        this.values.clear();
    }

    @Override
    public int size()
    {
        return this.values.size();
    }

    @Override
    public Collection<String> keys ()
    {
        return Collections.unmodifiableCollection(values.keySet());
    }
}
