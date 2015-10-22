package cs.bilkent.zanza.operator;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
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

    private Object partitionKey;

    private int partitionHash;

    public Tuple ()
    {
        this.values = new HashMap<>();
    }

    public Tuple ( final boolean withInsertionOrder )
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

    @SuppressWarnings( "unchecked" )
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

    /**
     * Copies partition key and partition hash to the other tuple object.
     * Can be used only within {@link OperatorType#PARTITIONED_STATEFUL} operators.
     *
     * @param that
     *         tuple object to copy the partition key and partition hash
     */
    public void copyPartitionTo ( final Tuple other )
    {
        other.setPartition( getPartitionKey(), getPartitionHash() );
    }

    /**
     * Returns the partition key assigned to the tuple by the engine
     *
     * @return the partition key assigned to the tuple by the engine
     *
     * @throws IllegalStateException
     *         if no partition key is assigned
     */
    public Object getPartitionKey ()
    {
        checkState( partitionKey != null, "partition key is not set!" );
        return partitionKey;
    }

    /**
     * Returns the partition hash assigned to the tuple by the engine
     *
     * @return the partition hash assigned to the tuple by the engine
     *
     * @throws IllegalStateException
     *         if no partition hash is assigned
     */
    public int getPartitionHash ()
    {
        checkState( partitionKey != null, "partition key is not set!" );
        return partitionHash;
    }

    /**
     * Only for use of the engine. Not exposed to the user.
     * Assigns partition key and hash for the tuple
     *
     * @param partitionKey
     *         the partition key to be assigned
     * @param partitionHash
     *         the partition has to be assigned
     */
    void setPartition ( final Object partitionKey, final int partitionHash )
    {
        checkState( this.partitionKey == null, "partition key is already assigned!" );
        this.partitionKey = partitionKey;
        this.partitionHash = partitionHash;
    }

    /**
     * Only for use of the engine. Not exposed to the user.
     * Clears partition key and partition hash of the tuple
     */
    void clearPartition ()
    {
        this.partitionKey = null;
        this.partitionHash = 0;
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
        if ( partitionKey != null )
        {
            return "Tuple[pKey=" + partitionKey + ",pHash=" + partitionHash + "]{" + values + '}';
        }
        else
        {
            return "Tuple{" + values + '}';
        }
    }
}
