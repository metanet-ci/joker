package cs.bilkent.zanza.operator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.unmodifiableMap;

/**
 * The tuple is the main data structure to manipulate data in Zanza.
 * A tuple is a mapping of keys to values where each value can be any type.
 * <p>
 * TODO Serializable, Iterable ???
 */
public final class Tuple implements Fields<String>
{
    private final Map<String, Object> values;

    public Tuple ()
    {
        this.values = new HashMap<>();
    }

    public Tuple ( final String key, final Object value )
    {
        checkNotNull( key, "key can't be null" );
        checkNotNull( value, "value can't be null" );
        this.values = new HashMap<>();
        this.values.put( key, value );
    }

    public Tuple ( final Map<String, Object> values )
    {
        checkNotNull( values, "values can't be null" );
        this.values = new HashMap<>();
        this.values.putAll( values );
    }

    public Map<String, Object> asMap ()
    {
        return unmodifiableMap( values );
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public <T> T get ( final String key )
    {
        checkNotNull( key, "key can't be null" );
        return (T) values.get( key );
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

    @SuppressWarnings( "unchecked" )
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

    public List<Object> getValues ( final List<String> keys )
    {
        checkNotNull( keys, "keys can't be null" );
        final List<Object> values = new ArrayList<>( keys.size() );
        for ( String key : keys )
        {
            values.add( get( key ) );
        }

        return values;
    }

    @Override
    public void clear ()
    {
        this.values.clear();
    }

    @Override
    public int size ()
    {
        return this.values.size();
    }

    @Override
    public Collection<String> keys ()
    {
        return Collections.unmodifiableCollection( values.keySet() );
    }

    @Override
    public boolean equals ( final Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        final Tuple tuple = (Tuple) o;

        return values.equals( tuple.values );

    }

    @Override
    public int hashCode ()
    {
        return values.hashCode();
    }

    @Override
    public String toString ()
    {
        return "Tuple{" + values + '}';
    }
}
