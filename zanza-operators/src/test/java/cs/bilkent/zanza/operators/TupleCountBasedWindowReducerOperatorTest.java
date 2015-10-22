package cs.bilkent.zanza.operators;

import java.util.function.BiFunction;

import org.junit.Before;
import org.junit.Test;

import cs.bilkent.zanza.operator.Port;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.ProcessingResult;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.TupleAccessor;
import cs.bilkent.zanza.operator.invocationreason.SuccessfulInvocation;
import cs.bilkent.zanza.operator.kvstore.InMemoryKVStore;
import cs.bilkent.zanza.operator.kvstore.KVStore;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import static cs.bilkent.zanza.operators.TupleCountBasedWindowReducerOperator.INITIAL_VALUE_CONFIG_PARAMETER;
import static cs.bilkent.zanza.operators.TupleCountBasedWindowReducerOperator.REDUCER_CONFIG_PARAMETER;
import static cs.bilkent.zanza.operators.TupleCountBasedWindowReducerOperator.TUPLE_COUNT_CONFIG_PARAMETER;
import static cs.bilkent.zanza.operators.TupleCountBasedWindowReducerOperator.TUPLE_COUNT_FIELD;
import static cs.bilkent.zanza.operators.TupleCountBasedWindowReducerOperator.WINDOW_FIELD;
import static cs.bilkent.zanza.operators.TupleCountBasedWindowReducerOperator.toAccumulatorKey;
import static cs.bilkent.zanza.operators.TupleCountBasedWindowReducerOperator.toWindowKey;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TupleCountBasedWindowReducerOperatorTest
{

    private final TupleCountBasedWindowReducerOperator operator = new TupleCountBasedWindowReducerOperator();

    private final SimpleOperatorContext operatorContext = new SimpleOperatorContext();

    private final KVStore kvStore = new InMemoryKVStore();

    private final BiFunction<Tuple, Tuple, Tuple> adder = ( tuple1, tuple2 ) -> new Tuple( "count",
                                                                                           tuple1.getInteger( "count" )
                                                                                           + tuple2.getInteger( "count" ) );

    @Before
    public void before ()
    {
        operatorContext.setKvStore( kvStore );
    }

    private final int tupleCount = 2;

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailToInitWithoutReducer ()
    {
        operatorContext.getConfig().set( TUPLE_COUNT_CONFIG_PARAMETER, tupleCount );
        operator.init( operatorContext );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailToInitWithoutTupleCount ()
    {
        operatorContext.getConfig().set( REDUCER_CONFIG_PARAMETER, adder );
        operator.init( operatorContext );
    }

    @Test
    public void shouldInitSuccessfully ()
    {
        configureReducerAndTupleCount();
        final SchedulingStrategy strategy = operator.init( operatorContext );

        assertStrategy( strategy );
    }

    @Test
    public void shouldReduceWithSingleKeySingleTupleWithoutInitialValue ()
    {
        configureReducerAndTupleCount();
        operator.init( operatorContext );

        final ProcessingResult result = operator.process( new PortsToTuples( newTupleWithKey( "key1", 1 ) ),
                                                          SuccessfulInvocation.INSTANCE );
        final PortsToTuples output = result.getPortsToTuples();

        assertStrategy( result.getSchedulingStrategy() );
        assertThat( output.getTuplesByDefaultPort(), hasSize( 0 ) );

        assertWindow( "key1", 0, 1 );
        assertAccumulator( "key1", 1 );
    }

    @Test
    public void shouldReduceWithSingleKeySingleTupleWithInitialValue ()
    {
        configureReducerAndTupleCount();
        operatorContext.getConfig().set( INITIAL_VALUE_CONFIG_PARAMETER, new Tuple( "count", 2 ) );
        operator.init( operatorContext );

        final ProcessingResult result = operator.process( new PortsToTuples( newTupleWithKey( "key1", 1 ) ),
                                                          SuccessfulInvocation.INSTANCE );
        final PortsToTuples output = result.getPortsToTuples();

        assertStrategy( result.getSchedulingStrategy() );
        assertThat( output.getTuplesByDefaultPort(), hasSize( 0 ) );

        assertWindow( "key1", 0, 1 );
        assertAccumulator( "key1", 3 );
    }

    @Test
    public void shouldReduceWithSingleKeyMultipleTuplesWithoutInitialValue ()
    {
        configureReducerAndTupleCount();
        operator.init( operatorContext );

        final PortsToTuples input = new PortsToTuples();
        input.add( newTupleWithKey( "key1", 1 ) );
        input.add( newTupleWithKey( "key1", 2 ) );
        final ProcessingResult result = operator.process( input, SuccessfulInvocation.INSTANCE );
        final PortsToTuples output = result.getPortsToTuples();

        assertStrategy( result.getSchedulingStrategy() );
        assertThat( output.getTuplesByDefaultPort(), hasSize( 1 ) );

        assertOutput( output.getTuple( Port.DEFAULT_PORT_INDEX, 0 ), "key1", 0, 3 );
        assertWindow( "key1", 1, 0 );
        assertAccumulatorNotExist( "key1" );
    }

    @Test
    public void shouldReduceWithSingleKeyMultipleTuplesWithoutInitialValue2 ()
    {
        configureReducerAndTupleCount();
        operator.init( operatorContext );

        final PortsToTuples input = new PortsToTuples();
        input.add( newTupleWithKey( "key1", 1 ) );
        input.add( newTupleWithKey( "key1", 2 ) );
        input.add( newTupleWithKey( "key1", 3 ) );
        final ProcessingResult result = operator.process( input, SuccessfulInvocation.INSTANCE );
        final PortsToTuples output = result.getPortsToTuples();

        assertStrategy( result.getSchedulingStrategy() );
        assertThat( output.getTuplesByDefaultPort(), hasSize( 1 ) );

        assertOutput( output.getTuple( Port.DEFAULT_PORT_INDEX, 0 ), "key1", 0, 3 );
        assertWindow( "key1", 1, 1 );
        assertAccumulator( "key1", 3 );
    }

    @Test
    public void shouldReduceWithSingleKeyMultipleTuplesWithInitialValue ()
    {
        configureReducerAndTupleCount();
        operatorContext.getConfig().set( INITIAL_VALUE_CONFIG_PARAMETER, new Tuple( "count", 3 ) );
        operator.init( operatorContext );

        final PortsToTuples input = new PortsToTuples();
        input.add( newTupleWithKey( "key1", 1 ) );
        input.add( newTupleWithKey( "key1", 2 ) );
        final ProcessingResult result = operator.process( input, SuccessfulInvocation.INSTANCE );
        final PortsToTuples output = result.getPortsToTuples();

        assertStrategy( result.getSchedulingStrategy() );
        assertThat( output.getTuplesByDefaultPort(), hasSize( 1 ) );

        assertOutput( output.getTuple( Port.DEFAULT_PORT_INDEX, 0 ), "key1", 0, 6 );
        assertWindow( "key1", 1, 0 );
        assertAccumulatorNotExist( "key1" );
    }

    @Test
    public void shouldReduceWithSingleKeyMultipleTuplesWithInitialValue2 ()
    {
        configureReducerAndTupleCount();
        operatorContext.getConfig().set( INITIAL_VALUE_CONFIG_PARAMETER, new Tuple( "count", 4 ) );
        operator.init( operatorContext );

        final PortsToTuples input = new PortsToTuples();
        input.add( newTupleWithKey( "key1", 1 ) );
        input.add( newTupleWithKey( "key1", 2 ) );
        input.add( newTupleWithKey( "key1", 3 ) );
        final ProcessingResult result = operator.process( input, SuccessfulInvocation.INSTANCE );
        final PortsToTuples output = result.getPortsToTuples();

        assertStrategy( result.getSchedulingStrategy() );
        assertThat( output.getTuplesByDefaultPort(), hasSize( 1 ) );

        assertOutput( output.getTuple( Port.DEFAULT_PORT_INDEX, 0 ), "key1", 0, 7 );
        assertWindow( "key1", 1, 1 );
        assertAccumulator( "key1", 7 );
    }

    @Test
    public void shouldReduceWithMultipleKeysMultipleTuplesWithoutInitialValue ()
    {
        configureReducerAndTupleCount();
        operator.init( operatorContext );

        final PortsToTuples input = new PortsToTuples();
        input.add( newTupleWithKey( "key1", 1 ) );
        input.add( newTupleWithKey( "key1", 2 ) );
        input.add( newTupleWithKey( "key2", 3 ) );
        input.add( newTupleWithKey( "key2", 4 ) );
        final ProcessingResult result = operator.process( input, SuccessfulInvocation.INSTANCE );
        final PortsToTuples output = result.getPortsToTuples();

        assertStrategy( result.getSchedulingStrategy() );
        assertThat( output.getTuplesByDefaultPort(), hasSize( 2 ) );

        assertOutput( output.getTuple( Port.DEFAULT_PORT_INDEX, 0 ), "key1", 0, 3 );
        assertWindow( "key1", 1, 0 );
        assertAccumulatorNotExist( "key1" );

        assertOutput( output.getTuple( Port.DEFAULT_PORT_INDEX, 1 ), "key2", 0, 7 );
        assertWindow( "key2", 1, 0 );
        assertAccumulatorNotExist( "key2" );
    }

    @Test
    public void shouldReduceWithMultipleKeysMultipleTuplesWithoutInitialValue2 ()
    {
        configureReducerAndTupleCount();
        operator.init( operatorContext );

        final PortsToTuples input = new PortsToTuples();
        input.add( newTupleWithKey( "key1", 1 ) );
        input.add( newTupleWithKey( "key2", 3 ) );
        input.add( newTupleWithKey( "key2", 4 ) );
        input.add( newTupleWithKey( "key1", 2 ) );
        final ProcessingResult result = operator.process( input, SuccessfulInvocation.INSTANCE );
        final PortsToTuples output = result.getPortsToTuples();

        assertStrategy( result.getSchedulingStrategy() );
        assertThat( output.getTuplesByDefaultPort(), hasSize( 2 ) );

        assertOutput( output.getTuple( Port.DEFAULT_PORT_INDEX, 1 ), "key1", 0, 3 );
        assertWindow( "key1", 1, 0 );
        assertAccumulatorNotExist( "key1" );

        assertOutput( output.getTuple( Port.DEFAULT_PORT_INDEX, 0 ), "key2", 0, 7 );
        assertWindow( "key2", 1, 0 );
        assertAccumulatorNotExist( "key2" );
    }

    @Test
    public void shouldReduceWithMultipleKeysMultipleTuplesWithoutInitialValue3 ()
    {
        configureReducerAndTupleCount();
        operator.init( operatorContext );

        final PortsToTuples input = new PortsToTuples();
        input.add( newTupleWithKey( "key1", 1 ) );
        input.add( newTupleWithKey( "key2", 3 ) );
        input.add( newTupleWithKey( "key2", 4 ) );
        input.add( newTupleWithKey( "key1", 2 ) );
        input.add( newTupleWithKey( "key2", 5 ) );
        final ProcessingResult result = operator.process( input, SuccessfulInvocation.INSTANCE );
        final PortsToTuples output = result.getPortsToTuples();

        assertStrategy( result.getSchedulingStrategy() );
        assertThat( output.getTuplesByDefaultPort(), hasSize( 2 ) );

        assertOutput( output.getTuple( Port.DEFAULT_PORT_INDEX, 1 ), "key1", 0, 3 );
        assertWindow( "key1", 1, 0 );
        assertAccumulatorNotExist( "key1" );

        assertOutput( output.getTuple( Port.DEFAULT_PORT_INDEX, 0 ), "key2", 0, 7 );
        assertWindow( "key2", 1, 1 );
        assertAccumulator( "key2", 5 );
    }

    @Test
    public void shouldReduceWithMultipleKeysMultipleTuplesWithInitialValue ()
    {
        configureReducerAndTupleCount();
        operatorContext.getConfig().set( TupleCountBasedWindowReducerOperator.INITIAL_VALUE_CONFIG_PARAMETER, new Tuple( "count", 6 ) );
        operator.init( operatorContext );

        final PortsToTuples input = new PortsToTuples();
        input.add( newTupleWithKey( "key1", 1 ) );
        input.add( newTupleWithKey( "key2", 3 ) );
        input.add( newTupleWithKey( "key2", 4 ) );
        input.add( newTupleWithKey( "key1", 2 ) );
        input.add( newTupleWithKey( "key2", 5 ) );
        final ProcessingResult result = operator.process( input, SuccessfulInvocation.INSTANCE );
        final PortsToTuples output = result.getPortsToTuples();

        assertStrategy( result.getSchedulingStrategy() );
        assertThat( output.getTuplesByDefaultPort(), hasSize( 2 ) );

        assertOutput( output.getTuple( Port.DEFAULT_PORT_INDEX, 1 ), "key1", 0, 9 );
        assertWindow( "key1", 1, 0 );
        assertAccumulatorNotExist( "key1" );

        assertOutput( output.getTuple( Port.DEFAULT_PORT_INDEX, 0 ), "key2", 0, 13 );
        assertWindow( "key2", 1, 1 );
        assertAccumulator( "key2", 11 );
    }

    private void configureReducerAndTupleCount ()
    {
        operatorContext.getConfig().set( REDUCER_CONFIG_PARAMETER, adder );
        operatorContext.getConfig().set( TUPLE_COUNT_CONFIG_PARAMETER, tupleCount );
    }

    private Tuple newTupleWithKey ( final Object key, final int count )
    {
        final Tuple tuple = new Tuple( "count", count );
        TupleAccessor.setPartition( tuple, key, 1 );
        return tuple;
    }

    private void assertStrategy ( final SchedulingStrategy strategy )
    {
        assertTrue( strategy instanceof ScheduleWhenTuplesAvailable );
        ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailable = (ScheduleWhenTuplesAvailable) strategy;
        assertThat( scheduleWhenTuplesAvailable.getTupleCount( Port.DEFAULT_PORT_INDEX ), equalTo( 1 ) );
    }

    private void assertWindow ( final Object key, final int window, final int tupleCount )
    {
        final Tuple tuple = kvStore.get( toWindowKey( key ) );
        assertThat( tuple.getInteger( WINDOW_FIELD ), equalTo( window ) );
        assertThat( tuple.getInteger( TUPLE_COUNT_FIELD ), equalTo( tupleCount ) );
    }

    private void assertAccumulator ( final Object key, final int count )
    {
        final Tuple accumulator = kvStore.get( toAccumulatorKey( key ) );
        assertNotNull( accumulator );
        assertThat( accumulator.getInteger( "count" ), equalTo( count ) );
    }

    private void assertOutput ( final Tuple tuple, final Object key, final int window, final int count )
    {
        assertThat( tuple.getPartitionKey(), equalTo( key ) );
        assertThat( tuple.getInteger( WINDOW_FIELD ), equalTo( window ) );
        assertThat( tuple.getInteger( "count" ), equalTo( count ) );
    }

    private void assertAccumulatorNotExist ( final Object key )
    {
        assertFalse( kvStore.contains( toAccumulatorKey( key ) ) );
    }

}
