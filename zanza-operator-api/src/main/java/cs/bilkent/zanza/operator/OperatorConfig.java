package cs.bilkent.zanza.operator;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import cs.bilkent.zanza.flow.Port;

/**
 * Contains all the configuration information that can be used by an operator. Users can provide operator specific
 * configuration parameters using this class.
 * <p>
 * Additionally, contains input port count and output port count if they are specified as dynamic in {@link OperatorSpec}
 * and {@link PartitionKeyExtractor} function if the operator is {@link OperatorType#PARTITIONED_STATEFUL}
 */
public class OperatorConfig implements Fields<String>
{
    private final Map<String, Object> values = new HashMap<>();

    private int inputPortCount = Port.DEFAULT_PORT_COUNT;

    private int outputPortCount = Port.DEFAULT_PORT_COUNT;

    private PartitionKeyExtractor partitionKeyExtractor;

    public OperatorConfig ()
    {
    }

    public OperatorConfig ( final Map<String, Object> values )
    {
        this.values.putAll( values );
    }

    public PartitionKeyExtractor getPartitionKeyExtractor ()
    {
        return partitionKeyExtractor;
    }

    public void setPartitionKeyExtractor ( final PartitionKeyExtractor partitionKeyExtractor )
    {
        this.partitionKeyExtractor = partitionKeyExtractor;
    }

    public Map<String, Object> getValues ()
    {
        return values;
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
        checkNotNull( key, "key can't be null!" );
        checkNotNull( value, "value can't be null!" );
        this.values.put( key, value );
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public <T> T put ( final String key, final T value )
    {
        checkNotNull( key, "key can't be null" );
        checkNotNull( value, "value can't be null!" );
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

    public int getInputPortCount ()
    {
        return inputPortCount;
    }

    public void setInputPortCount ( final int inputPortCount )
    {
        this.inputPortCount = inputPortCount;
    }

    public int getOutputPortCount ()
    {
        return outputPortCount;
    }

    public void setOutputPortCount ( final int outputPortCount )
    {
        this.outputPortCount = outputPortCount;
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
}
