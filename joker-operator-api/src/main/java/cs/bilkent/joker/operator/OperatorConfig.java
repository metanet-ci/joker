package cs.bilkent.joker.operator;


import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;


/**
 * Contains all the configuration information that can be used by an operator. Users can provide operator specific
 * configuration parameters using this class.
 */
public final class OperatorConfig implements Fields<String>
{
    private final Map<String, Object> values = new HashMap<>();

    public OperatorConfig ()
    {
    }

    public OperatorConfig ( final Map<String, Object> values )
    {
        this.values.putAll( values );
    }

    public Map<String, Object> getValues ()
    {
        return values;
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public <T> T get ( final String key )
    {
        return (T) values.get( key );
    }

    @Override
    public boolean contains ( final String key )
    {
        return values.containsKey( key );
    }

    @Override
    public void set ( final String key, final Object value )
    {
        checkArgument( value != null, "value can't be null!" );
        this.values.put( key, value );
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public <T> T put ( final String key, final T value )
    {
        checkArgument( value != null, "value can't be null!" );
        return (T) this.values.put( key, value );
    }

    @Override
    public Object remove ( final String key )
    {
        return this.values.remove( key );
    }

    @Override
    public boolean delete ( final String key )
    {
        return this.values.remove( key ) != null;
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
        return Collections.unmodifiableSet( this.values.keySet() );
    }

    @Override
    public String toString ()
    {
        return "OperatorConfig{" + values + '}';
    }
}
