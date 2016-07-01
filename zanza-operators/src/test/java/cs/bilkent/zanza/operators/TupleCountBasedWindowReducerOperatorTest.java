package cs.bilkent.zanza.operators;

import java.util.function.BiFunction;

import org.junit.Test;

import static cs.bilkent.zanza.flow.Port.DEFAULT_PORT_INDEX;
import static cs.bilkent.zanza.operator.InvocationContext.InvocationReason.SUCCESS;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.impl.InitializationContextImpl;
import cs.bilkent.zanza.operator.impl.InvocationContextImpl;
import cs.bilkent.zanza.operator.impl.TuplesImpl;
import cs.bilkent.zanza.operator.kvstore.KVStore;
import cs.bilkent.zanza.operator.kvstore.impl.InMemoryKVStore;
import cs.bilkent.zanza.operator.kvstore.impl.KeyDecoratedKVStore;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import static cs.bilkent.zanza.operators.TupleCountBasedWindowReducerOperator.INITIAL_VALUE_CONFIG_PARAMETER;
import static cs.bilkent.zanza.operators.TupleCountBasedWindowReducerOperator.REDUCER_CONFIG_PARAMETER;
import static cs.bilkent.zanza.operators.TupleCountBasedWindowReducerOperator.TUPLE_COUNT_CONFIG_PARAMETER;
import static cs.bilkent.zanza.operators.TupleCountBasedWindowReducerOperator.TUPLE_COUNT_FIELD;
import static cs.bilkent.zanza.operators.TupleCountBasedWindowReducerOperator.WINDOW_FIELD;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class TupleCountBasedWindowReducerOperatorTest
{

    private static final String TUPLE_PARTITION_KEY = "key1";

    private final TupleCountBasedWindowReducerOperator operator = new TupleCountBasedWindowReducerOperator();

    private final InitializationContextImpl initContext = new InitializationContextImpl();

    private final TuplesImpl input = new TuplesImpl( 1 );

    private final TuplesImpl output = new TuplesImpl( 1 );

    private final KVStore kvStore = new KeyDecoratedKVStore( TUPLE_PARTITION_KEY, new InMemoryKVStore() );

    private final InvocationContextImpl invocationContext = new InvocationContextImpl( SUCCESS, input, output, kvStore );

    private final BiFunction<Tuple, Tuple, Tuple> adder = ( tuple1, tuple2 ) -> new Tuple( "count",
                                                                                           tuple1.getInteger( "count" ) + tuple2.getInteger(
                                                                                                   "count" ) );

    private final int tupleCount = 2;

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailToInitWithoutReducer ()
    {
        initContext.getConfig().set( TUPLE_COUNT_CONFIG_PARAMETER, tupleCount );
        operator.init( initContext );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailToInitWithoutTupleCount ()
    {
        initContext.getConfig().set( REDUCER_CONFIG_PARAMETER, adder );
        operator.init( initContext );
    }

    @Test
    public void shouldInitSuccessfully ()
    {
        configureReducerAndTupleCount();
        final SchedulingStrategy strategy = operator.init( initContext );

        assertStrategy( strategy );
    }

    @Test
    public void shouldReduceSingleTupleWithoutInitialValue ()
    {
        configureReducerAndTupleCount();
        operator.init( initContext );

        input.add( new Tuple( 1, "count", 1 ) );

        operator.invoke( invocationContext );

        assertThat( output.getTuplesByDefaultPort(), hasSize( 0 ) );

        assertWindow( 0, 1 );
        assertAccumulator( 1 );
    }

    @Test
    public void shouldReduceSingleTupleWithInitialValue ()
    {
        configureReducerAndTupleCount();
        initContext.getConfig().set( INITIAL_VALUE_CONFIG_PARAMETER, new Tuple( "count", 2 ) );
        operator.init( initContext );

        input.add( new Tuple( 1, "count", 1 ) );

        operator.invoke( invocationContext );

        assertThat( output.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 0 ) );

        assertWindow( 0, 1 );
        assertAccumulator( 3 );
    }

    @Test
    public void shouldReduceMultipleTuplesWithoutInitialValue ()
    {
        configureReducerAndTupleCount();
        operator.init( initContext );

        input.add( new Tuple( "count", 1 ) );
        input.add( new Tuple( "count", 2 ) );

        operator.invoke( invocationContext );

        assertThat( output.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 1 ) );

        final Tuple tuple = output.getTupleOrFail( DEFAULT_PORT_INDEX, 0 );
        assertOutput( tuple, TUPLE_PARTITION_KEY, 0, 3 );
        assertWindow( 1, 0 );
        assertAccumulatorNotExist();
    }

    @Test
    public void shouldReduceMultipleTuplesWithoutInitialValue2 ()
    {
        configureReducerAndTupleCount();
        operator.init( initContext );

        input.add( new Tuple( "count", 1 ) );
        input.add( new Tuple( "count", 2 ) );
        input.add( new Tuple( "count", 3 ) );

        operator.invoke( invocationContext );

        assertThat( output.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 1 ) );

        assertOutput( output.getTupleOrFail( DEFAULT_PORT_INDEX, 0 ), TUPLE_PARTITION_KEY, 0, 3 );
        assertWindow( 1, 1 );
        assertAccumulator( 3 );
    }

    @Test
    public void shouldReduceMultipleTuplesWithInitialValue ()
    {
        configureReducerAndTupleCount();
        initContext.getConfig().set( INITIAL_VALUE_CONFIG_PARAMETER, new Tuple( "count", 3 ) );
        operator.init( initContext );

        input.add( new Tuple( "count", 1 ) );
        input.add( new Tuple( "count", 2 ) );

        operator.invoke( invocationContext );

        assertThat( output.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 1 ) );

        assertOutput( output.getTupleOrFail( DEFAULT_PORT_INDEX, 0 ), TUPLE_PARTITION_KEY, 0, 6 );
        assertWindow( 1, 0 );
        assertAccumulator( 3 );
    }

    @Test
    public void shouldReduceMultipleTuplesWithInitialValue2 ()
    {
        configureReducerAndTupleCount();
        initContext.getConfig().set( INITIAL_VALUE_CONFIG_PARAMETER, new Tuple( "count", 4 ) );
        operator.init( initContext );

        input.add( new Tuple( "count", 1 ) );
        input.add( new Tuple( "count", 2 ) );
        input.add( new Tuple( "count", 3 ) );

        operator.invoke( invocationContext );

        assertThat( output.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 1 ) );

        assertOutput( output.getTupleOrFail( DEFAULT_PORT_INDEX, 0 ), TUPLE_PARTITION_KEY, 0, 7 );
        assertWindow( 1, 1 );
        assertAccumulator( 7 );
    }

    private void configureReducerAndTupleCount ()
    {
        initContext.getConfig().set( REDUCER_CONFIG_PARAMETER, adder );
        initContext.getConfig().set( TUPLE_COUNT_CONFIG_PARAMETER, tupleCount );
    }

    private void assertStrategy ( final SchedulingStrategy strategy )
    {
        assertTrue( strategy instanceof ScheduleWhenTuplesAvailable );
        ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailable = (ScheduleWhenTuplesAvailable) strategy;
        assertThat( scheduleWhenTuplesAvailable.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 1 ) );
    }

    private void assertWindow ( final int window, final int tupleCount )
    {
        final Tuple tuple = kvStore.get( TupleCountBasedWindowReducerOperator.CURRENT_WINDOW_KEY );
        assertThat( tuple.getInteger( WINDOW_FIELD ), equalTo( window ) );
        assertThat( tuple.getInteger( TUPLE_COUNT_FIELD ), equalTo( tupleCount ) );
    }

    private void assertAccumulator ( final int count )
    {
        final Tuple accumulator = kvStore.get( TupleCountBasedWindowReducerOperator.ACCUMULATOR_TUPLE_KEY );
        assertNotNull( accumulator );
        assertThat( accumulator.getInteger( "count" ), equalTo( count ) );
    }

    private void assertOutput ( final Tuple tuple, final Object key, final int window, final int count )
    {
        assertThat( tuple.getInteger( WINDOW_FIELD ), equalTo( window ) );
        assertThat( tuple.getInteger( "count" ), equalTo( count ) );
    }

    private void assertAccumulatorNotExist ()
    {
        assertFalse( kvStore.contains( TupleCountBasedWindowReducerOperator.ACCUMULATOR_TUPLE_KEY ) );
    }

}
