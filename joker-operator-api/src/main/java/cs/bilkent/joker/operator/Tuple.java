package cs.bilkent.joker.operator;


import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;

import static cs.bilkent.joker.com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.operator.schema.runtime.RuntimeSchemaField;
import cs.bilkent.joker.operator.schema.runtime.TupleSchema;
import static cs.bilkent.joker.operator.schema.runtime.TupleSchema.FIELD_NOT_FOUND;


/**
 * The tuple is the main data structure to manipulate data in Joker.
 * A tuple is a mapping of keys to values where each value can be any type.
 */
public final class Tuple implements Fields<String>
{

    private static final String INITIAL_CAPACITY_SYS_PARAM = "cs.bilkent.joker.Tuple.EMPTY_SCHEMA_INITIAL_CAPACITY";

    private static final int DEFAULT_EMPTY_SCHEMA_INITIAL_CAPACITY = 2;

    static
    {
        int sysArg = -1;
        try
        {
            String val = System.getProperty( INITIAL_CAPACITY_SYS_PARAM );
            if ( val != null )
            {
                val = val.trim();
                if ( val.length() > 0 )
                {
                    sysArg = Integer.parseInt( val );
                    System.out.println( "Static initialization: " + Tuple.class.getSimpleName() + " initial capacity is set to " + sysArg );
                }
            }
        }
        catch ( Exception e )
        {
            System.err.println( "Static initialization: " + Tuple.class.getSimpleName() + " initial capacity failed " );
            e.printStackTrace();
        }

        EMPTY_SCHEMA_INITIAL_CAPACITY = sysArg != -1 ? sysArg : DEFAULT_EMPTY_SCHEMA_INITIAL_CAPACITY;
    }

    private static final int EMPTY_SCHEMA_INITIAL_CAPACITY;


    private static final TupleSchema EMPTY_SCHEMA = new TupleSchema()
    {
        @Override
        public int getFieldCount ()
        {
            return 0;
        }

        @Override
        public List<RuntimeSchemaField> getFields ()
        {
            return Collections.emptyList();
        }

        @Override
        public int getFieldIndex ( final String fieldName )
        {
            return FIELD_NOT_FOUND;
        }

        @Override
        public String getFieldAt ( final int fieldIndex )
        {
            throw new UnsupportedOperationException();
        }
    };


    private final TupleSchema schema;

    private final ArrayList<Object> values;

    public Tuple ()
    {
        this.schema = EMPTY_SCHEMA;
        this.values = new ArrayList<>( EMPTY_SCHEMA_INITIAL_CAPACITY );
    }

    public Tuple ( TupleSchema schema )
    {
        this.schema = schema;
        this.values = new ArrayList<>( schema.getFieldCount() );
        for ( int i = 0; i < schema.getFieldCount(); i++ )
        {
            this.values.add( null );
        }
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public <T> T get ( final String key )
    {
        final int index = schema.getFieldIndex( key );
        if ( index != FIELD_NOT_FOUND )
        {
            return (T) values.get( index );
        }

        for ( int i = schema.getFieldCount(); i < values.size(); i++ )
        {
            final Entry<String, Object> entry = (Entry<String, Object>) values.get( i );
            if ( entry.getKey().equals( key ) )
            {
                return (T) entry.getValue();
            }
        }

        return null;
    }

    @Override
    public boolean contains ( final String key )
    {
        final int index = schema.getFieldIndex( key );
        if ( index != FIELD_NOT_FOUND )
        {
            return values.get( index ) != null;
        }

        for ( int i = schema.getFieldCount(); i < values.size(); i++ )
        {
            final Entry<String, Object> entry = (Entry<String, Object>) values.get( i );
            if ( entry.getKey().equals( key ) )
            {
                return true;
            }
        }

        return false;
    }

    @Override
    public void set ( final String key, final Object value )
    {
        checkArgument( value != null, "value can't be null" );

        final int index = schema.getFieldIndex( key );
        if ( index != FIELD_NOT_FOUND )
        {
            values.set( index, value );
        }
        else
        {
            for ( int i = schema.getFieldCount(); i < values.size(); i++ )
            {
                final Entry<String, Object> entry = (Entry<String, Object>) values.get( i );
                if ( entry.getKey().equals( key ) )
                {
                    entry.setValue( value );
                    return;
                }
            }

            values.add( new SimpleEntry<>( key, value ) );
        }
    }

    @Override
    public Object remove ( final String key )
    {
        final int index = schema.getFieldIndex( key );
        if ( index != FIELD_NOT_FOUND )
        {
            return values.set( index, null );
        }

        for ( int i = schema.getFieldCount(); i < values.size(); i++ )
        {
            final Entry<String, Object> entry = (Entry<String, Object>) values.get( i );
            if ( entry.getKey().equals( key ) )
            {
                if ( i < values.size() - 1 )
                {
                    values.set( i, values.get( values.size() - 1 ) );
                }

                values.remove( i );

                return entry.getValue();
            }
        }

        return null;
    }

    @Override
    public boolean delete ( final String key )
    {
        return remove( key ) != null;
    }

    public void consumeEntries ( final BiConsumer<String, Object> consumer )
    {
        for ( int i = 0; i < schema.getFieldCount(); i++ )
        {
            final Object value = values.get( i );
            if ( value != null )
            {
                final String key = schema.getFieldAt( i );
                consumer.accept( key, value );
            }
        }
        for ( int i = schema.getFieldCount(); i < values.size(); i++ )
        {
            final Entry<String, Object> entry = (Entry<String, Object>) values.get( i );
            consumer.accept( entry.getKey(), entry.getValue() );
        }
    }

    @Override
    public void clear ()
    {
        values.clear();
        for ( int i = 0; i < schema.getFieldCount(); i++ )
        {
            values.add( null );
        }
    }

    @Override
    public int size ()
    {
        int s = values.size();
        for ( int i = 0; i < schema.getFieldCount(); i++ )
        {
            if ( values.get( i ) == null )
            {
                s--;
            }
        }

        return s;
    }

    public TupleSchema getSchema ()
    {
        return schema;
    }

    private Map<String, Object> asMap ()
    {
        final Map<String, Object> map = new HashMap<>();
        for ( int i = 0; i < schema.getFieldCount(); i++ )
        {
            final Object value = values.get( i );
            if ( value != null )
            {
                final String key = schema.getFieldAt( i );
                map.put( key, value );
            }
        }
        for ( int i = schema.getFieldCount(); i < values.size(); i++ )
        {
            final Entry<String, Object> entry = (Entry<String, Object>) values.get( i );
            map.put( entry.getKey(), entry.getValue() );
        }

        return map;
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

        final Tuple that = (Tuple) o;

        return this.asMap().equals( that.asMap() );

    }

    @Override
    public int hashCode ()
    {
        return this.asMap().hashCode();
    }

    @Override
    public String toString ()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append( "Tuple{" );
        for ( int i = 0; i < schema.getFieldCount(); i++ )
        {
            final Object val = values.get( i );
            if ( val != null )
            {
                sb.append( "{" ).append( schema.getFieldAt( i ) ).append( "=" ).append( val ).append( "}," );
            }
        }

        for ( int i = schema.getFieldCount(); i < values.size(); i++ )
        {
            final Entry<String, Object> entry = (Entry<String, Object>) values.get( i );
            sb.append( "{" ).append( entry.getKey() ).append( "=" ).append( entry.getValue() ).append( "}," );
        }

        if ( sb.length() > 6 )
        {
            sb.deleteCharAt( sb.length() - 1 );
        }

        return sb.append( "}" ).toString();
    }

}
