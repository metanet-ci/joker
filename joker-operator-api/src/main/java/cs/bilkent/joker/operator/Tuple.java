package cs.bilkent.joker.operator;


import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;

import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkArgument;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkState;
import static cs.bilkent.joker.operator.Tuple.LatencyRecord.newQueueLatency;
import cs.bilkent.joker.operator.schema.runtime.RuntimeSchemaField;
import cs.bilkent.joker.operator.schema.runtime.TupleSchema;
import static cs.bilkent.joker.operator.schema.runtime.TupleSchema.FIELD_NOT_FOUND;


/**
 * {@code Tuple} is the main data structure to manipulate computation data. A tuple is a mapping of keys to values
 * where keys are strings and values are of any type. Tuples are semi-schemaful, which means that they can specify the fields that are
 * guaranteed to exist, using {@link TupleSchema} objects, and they can also contain additional arbitrary fields.
 * <p/>
 * If a {@code Tuple} object is created to be sent to an output port of an operator, corresponding {@link TupleSchema} object,
 * which can be accessed via {@link InitCtx#getOutputPortSchema(int)} method, should be provided to the tuple. If not
 * specified, an empty schema will be used by default, which can cause negative performance effects on the downstream. It is recommended
 * to specify schema objects properly as they will decrease memory overhead of the tuples and make field accesses in constant time.
 */
public final class Tuple implements Fields<String>
{

    private static final String INITIAL_CAPACITY_SYS_PARAM = "cs.bilkent.joker.Tuple.EMPTY_SCHEMA_INITIAL_CAPACITY";

    private static final int DEFAULT_EMPTY_SCHEMA_INITIAL_CAPACITY = 2;

    static final long INGESTION_TIME_NOT_ASSIGNED = Long.MIN_VALUE;

    static final long INGESTION_TIME_UNASSIGNABLE = INGESTION_TIME_NOT_ASSIGNED + 1;

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
            System.err.println( "Static initialization: " + Tuple.class.getSimpleName() + " initial capacity failed" );
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

    public static Tuple of ( final String key, final Object val )
    {
        return of( EMPTY_SCHEMA, key, val );
    }

    public static Tuple of ( final String key1, final Object val1, final String key2, final Object val2 )
    {
        return of( EMPTY_SCHEMA, key1, val1, key2, val2 );
    }

    public static Tuple of ( final String key1,
                             final Object val1,
                             final String key2,
                             final Object val2,
                             final String key3,
                             final Object val3 )
    {
        return of( EMPTY_SCHEMA, key1, val1, key2, val2, key3, val3 );
    }

    public static Tuple of ( final String key1,
                             final Object val1,
                             final String key2,
                             final Object val2,
                             final String key3,
                             final Object val3,
                             final String key4,
                             final Object val4 )
    {
        return of( EMPTY_SCHEMA, key1, val1, key2, val2, key3, val3, key4, val4 );
    }

    public static Tuple of ( final String key1,
                             final Object val1,
                             final String key2,
                             final Object val2,
                             final String key3,
                             final Object val3,
                             final String key4,
                             final Object val4,
                             final String key5,
                             final Object val5 )
    {
        return of( EMPTY_SCHEMA, key1, val1, key2, val2, key3, val3, key4, val4, key5, val5 );
    }


    public static Tuple of ( final TupleSchema schema, final String key, final Object val )
    {
        return new Tuple( schema ).set( key, val );
    }

    public static Tuple of ( final TupleSchema schema, final String key1, final Object val1, final String key2, final Object val2 )
    {
        return new Tuple( schema ).set( key1, val1 ).set( key2, val2 );
    }

    public static Tuple of ( final TupleSchema schema,
                             final String key1,
                             final Object val1,
                             final String key2,
                             final Object val2,
                             final String key3,
                             final Object val3 )
    {
        return new Tuple( schema ).set( key1, val1 ).set( key2, val2 ).set( key3, val3 );
    }

    public static Tuple of ( final TupleSchema schema,
                             final String key1,
                             final Object val1,
                             final String key2,
                             final Object val2,
                             final String key3,
                             final Object val3,
                             final String key4,
                             final Object val4 )
    {
        return new Tuple( schema ).set( key1, val1 ).set( key2, val2 ).set( key3, val3 ).set( key4, val4 );
    }

    public static Tuple of ( final TupleSchema schema,
                             final String key1,
                             final Object val1,
                             final String key2,
                             final Object val2,
                             final String key3,
                             final Object val3,
                             final String key4,
                             final Object val4,
                             final String key5,
                             final Object val5 )
    {
        return new Tuple( schema ).set( key1, val1 ).set( key2, val2 ).set( key3, val3 ).set( key4, val4 ).set( key5, val5 );
    }


    private final TupleSchema schema;

    private final ArrayList<Object> values;

    private long ingestionTime = INGESTION_TIME_NOT_ASSIGNED;

    private long queueOfferTime = INGESTION_TIME_NOT_ASSIGNED;

    private List<LatencyRecord> latencyRecs;

    public Tuple ()
    {
        this.schema = EMPTY_SCHEMA;
        this.values = new ArrayList<>( EMPTY_SCHEMA_INITIAL_CAPACITY );
    }

    public Tuple ( final TupleSchema schema )
    {
        this.schema = schema;
        this.values = new ArrayList<>( schema.getFieldCount() );
        for ( int i = 0; i < schema.getFieldCount(); i++ )
        {
            this.values.add( null );
        }
    }

    private Tuple ( final TupleSchema schema,
                    final ArrayList<Object> values,
                    final long ingestionTime, final List<LatencyRecord> latencyRecs )
    {
        this.schema = schema;
        this.values = values;
        this.ingestionTime = ingestionTime;
        if ( latencyRecs != null )
        {
            this.latencyRecs = new ArrayList<>( latencyRecs );
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
            final Entry<String, Object> entry = getEntry( i );
            if ( entry.getKey().equals( key ) )
            {
                return (T) entry.getValue();
            }
        }

        return null;
    }

    public <T> T getAtSchemaIndex ( final int i )
    {
        checkArgument( i >= 0 && i < schema.getFieldCount(), "invalid index" );
        return (T) values.get( i );
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
            final Entry<String, Object> entry = getEntry( i );
            if ( entry.getKey().equals( key ) )
            {
                return true;
            }
        }

        return false;
    }

    @Override
    public Tuple set ( final String key, final Object value )
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
                final Entry<String, Object> entry = getEntry( i );
                if ( entry.getKey().equals( key ) )
                {
                    entry.setValue( value );
                    return this;
                }
            }

            values.add( new SimpleEntry<>( key, value ) );
        }

        return this;
    }

    public void setAtSchemaIndex ( final int i, final Object value )
    {
        checkArgument( i >= 0 && i < schema.getFieldCount(), "invalid index" );
        values.set( i, value );
    }

    @Override
    public <T> T remove ( final String key )
    {
        final int index = schema.getFieldIndex( key );
        if ( index != FIELD_NOT_FOUND )
        {
            return (T) values.set( index, null );
        }

        for ( int i = schema.getFieldCount(); i < values.size(); i++ )
        {
            final Entry<String, Object> entry = getEntry( i );
            if ( entry.getKey().equals( key ) )
            {
                if ( i < values.size() - 1 )
                {
                    values.set( i, values.get( values.size() - 1 ) );
                }

                values.remove( i );

                return (T) entry.getValue();
            }
        }

        return null;
    }

    private Entry<String, Object> getEntry ( final int i )
    {
        return (Entry<String, Object>) values.get( i );
    }

    @Override
    public boolean delete ( final String key )
    {
        return remove( key ) != null;
    }

    public void sinkTo ( final BiConsumer<String, Object> consumer )
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
            final Entry<String, Object> entry = getEntry( i );
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

    public long getIngestionTime ()
    {
        return ingestionTime;
    }

    public boolean isIngestionTimeNA ()
    {
        return ( ingestionTime == INGESTION_TIME_NOT_ASSIGNED || ingestionTime == INGESTION_TIME_UNASSIGNABLE );
    }

    public void setIngestionTime ( final long ingestionTime, final boolean trackLatencyRecords )
    {
        checkArgument( !( ingestionTime == INGESTION_TIME_NOT_ASSIGNED || ingestionTime == INGESTION_TIME_UNASSIGNABLE ) );
        checkState( this.ingestionTime == INGESTION_TIME_NOT_ASSIGNED );
        this.ingestionTime = ingestionTime;
        if ( trackLatencyRecords )
        {
            latencyRecs = new ArrayList<>( 4 );
        }
    }

    public void attachTo ( final Tuple source )
    {
        if ( ingestionTime == INGESTION_TIME_UNASSIGNABLE )
        {
            return;
        }

        if ( source.isIngestionTimeNA() )
        {
            ingestionTime = INGESTION_TIME_UNASSIGNABLE;
            latencyRecs = null;
            return;
        }

        if ( source.ingestionTime > ingestionTime )
        {
            overwriteIngestionTime( source );
        }
    }

    private void overwriteIngestionTime ( final Tuple source )
    {
        ingestionTime = source.ingestionTime;
        if ( source.latencyRecs == null )
        {
            latencyRecs = null;
        }
        else if ( latencyRecs != null )
        {
            latencyRecs.clear();
            latencyRecs.addAll( source.latencyRecs );
        }
        else
        {
            latencyRecs = new ArrayList<>( source.latencyRecs );
        }
    }

    public Tuple shallowCopy ()
    {
        return new Tuple( schema, values, ingestionTime, latencyRecs );
    }

    public void setQueueOfferTime ( final long queueOfferTime )
    {
        if ( isNotTrackingLatencyRecords() )
        {
            return;
        }

        this.queueOfferTime = queueOfferTime;
    }

    public void recordQueueLatency ( final String operatorId, final long now )
    {
        if ( queueOfferTime == INGESTION_TIME_NOT_ASSIGNED || latencyRecs == null )
        {
            return;
        }

        latencyRecs.add( newQueueLatency( operatorId, queueOfferTime, now ) );
        queueOfferTime = INGESTION_TIME_NOT_ASSIGNED;
    }

    public void addInvocationLatencyRecord ( final LatencyRecord latencyRec )
    {
        checkArgument( latencyRec != null );
        if ( isNotTrackingLatencyRecords() )
        {
            return;
        }

        latencyRecs.add( latencyRec );
    }

    private boolean isNotTrackingLatencyRecords ()
    {
        return isIngestionTimeNA() || latencyRecs == null;
    }

    public List<LatencyRecord> getLatencyRecs ()
    {
        return latencyRecs;
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
            final Entry<String, Object> entry = getEntry( i );
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
        sb.append( "Tuple(" );
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
            final Entry<String, Object> entry = getEntry( i );
            sb.append( "{" ).append( entry.getKey() ).append( "=" ).append( entry.getValue() ).append( "}," );
        }

        if ( sb.length() > 6 )
        {
            sb.deleteCharAt( sb.length() - 1 );
        }

        return sb.append( ")" ).toString();
    }

    public static class LatencyRecord
    {

        public static LatencyRecord newQueueLatency ( final String operatorId, final long start, final long end )
        {
            final LatencyRecord rec = new LatencyRecord( operatorId, false, start );
            rec.setEnd( end );
            return rec;
        }

        public static LatencyRecord newInvocationLatency ( final String operatorId, final long start )
        {
            return new LatencyRecord( operatorId, true, start );
        }

        private final String operatorId;
        private final boolean isOperator;
        private final long start;
        private long end = INGESTION_TIME_NOT_ASSIGNED;

        private LatencyRecord ( final String operatorId, final boolean isOperator, final long start )
        {
            checkArgument( operatorId != null );
            checkArgument( start != INGESTION_TIME_NOT_ASSIGNED );
            this.operatorId = operatorId;
            this.isOperator = isOperator;
            this.start = start;
        }

        public LatencyRecord setEnd ( final long end )
        {
            checkArgument( start != INGESTION_TIME_NOT_ASSIGNED );
            this.end = end;
            return this;
        }

        public boolean isOperator ()
        {
            return isOperator;
        }

        public String getOperatorId ()
        {
            return operatorId;
        }

        public long getLatency ()
        {
            if ( end == INGESTION_TIME_NOT_ASSIGNED )
            {
                System.out.println( "" );
            }
            checkState( end != INGESTION_TIME_NOT_ASSIGNED );
            return end - start;
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

            final LatencyRecord that = (LatencyRecord) o;

            if ( isOperator != that.isOperator )
            {
                return false;
            }
            if ( start != that.start )
            {
                return false;
            }
            if ( end != that.end )
            {
                return false;
            }
            return operatorId.equals( that.operatorId );
        }

        @Override
        public int hashCode ()
        {
            int result = operatorId.hashCode();
            result = 31 * result + ( isOperator ? 1 : 0 );
            result = 31 * result + (int) ( start ^ ( start >>> 32 ) );
            result = 31 * result + (int) ( end ^ ( end >>> 32 ) );
            return result;
        }

        @Override
        public String toString ()
        {
            return "LatencyRecord{" + "operatorId='" + operatorId + '\'' + ", isOperator=" + isOperator + ", start=" + start + ", end="
                   + end + '}';
        }
    }

}
