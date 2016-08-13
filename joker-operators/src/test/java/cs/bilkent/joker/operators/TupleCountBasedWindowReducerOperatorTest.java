package cs.bilkent.joker.operators;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Test;

import static cs.bilkent.joker.flow.Port.DEFAULT_PORT_INDEX;
import static cs.bilkent.joker.operator.InvocationContext.InvocationReason.SUCCESS;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.InitializationContextImpl;
import cs.bilkent.joker.operator.impl.InvocationContextImpl;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.kvstore.KVStore;
import cs.bilkent.joker.operator.kvstore.impl.InMemoryKVStore;
import cs.bilkent.joker.operator.kvstore.impl.KeyDecoratedKVStore;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchemaBuilder;
import static cs.bilkent.joker.operators.TupleCountBasedWindowReducerOperator.ACCUMULATOR_INITIALIZER_CONFIG_PARAMETER;
import static cs.bilkent.joker.operators.TupleCountBasedWindowReducerOperator.REDUCER_CONFIG_PARAMETER;
import static cs.bilkent.joker.operators.TupleCountBasedWindowReducerOperator.TUPLE_COUNT_CONFIG_PARAMETER;
import static cs.bilkent.joker.operators.TupleCountBasedWindowReducerOperator.TUPLE_COUNT_FIELD;
import static cs.bilkent.joker.operators.TupleCountBasedWindowReducerOperator.WINDOW_FIELD;
import cs.bilkent.joker.testutils.AbstractJokerTest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class TupleCountBasedWindowReducerOperatorTest extends AbstractJokerTest
{

    private static final String TUPLE_PARTITION_KEY = "key1";

    private final TupleCountBasedWindowReducerOperator operator = new TupleCountBasedWindowReducerOperator();

    private final InitializationContextImpl initContext = new InitializationContextImpl();

    private final TuplesImpl input = new TuplesImpl( 1 );

    private final TuplesImpl output = new TuplesImpl( 1 );

    private final KVStore kvStore = new KeyDecoratedKVStore( TUPLE_PARTITION_KEY, new InMemoryKVStore() );

    private final InvocationContextImpl invocationContext = new InvocationContextImpl( SUCCESS, input, output, kvStore );

    private final BiConsumer<Tuple, Tuple> adder = ( accumulator, val ) ->
    {
        final int curr = accumulator.getInteger( "count" );
        accumulator.set( "count", curr + val.getInteger( "count" ) );
    };

    private final int tupleCount = 2;

    @Before
    public void init ()
    {
        final OperatorRuntimeSchemaBuilder builder = new OperatorRuntimeSchemaBuilder( 1, 1 );
        builder.addInputField( 0, "count", Integer.class )
               .addOutputField( 0, "count", Integer.class )
               .addOutputField( 0, WINDOW_FIELD, Integer.class );
        initContext.setRuntimeSchema( builder.build() );
    }

    private Consumer<Tuple> createAccumulatorInitializer ( final int initialValue )
    {
        return accumulator -> accumulator.set( "count", initialValue );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailToInitWithoutReducer ()
    {
        initContext.getConfig().set( TUPLE_COUNT_CONFIG_PARAMETER, tupleCount );
        initContext.getConfig().set( ACCUMULATOR_INITIALIZER_CONFIG_PARAMETER, createAccumulatorInitializer( 0 ) );
        operator.init( initContext );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailToInitWithoutTupleCount ()
    {
        initContext.getConfig().set( REDUCER_CONFIG_PARAMETER, adder );
        initContext.getConfig().set( ACCUMULATOR_INITIALIZER_CONFIG_PARAMETER, createAccumulatorInitializer( 0 ) );
        operator.init( initContext );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailToInitWithoutAccumulatorInitializer ()
    {
        initContext.getConfig().set( TUPLE_COUNT_CONFIG_PARAMETER, tupleCount );
        initContext.getConfig().set( REDUCER_CONFIG_PARAMETER, adder );
        operator.init( initContext );
    }

    @Test
    public void shouldInitSuccessfully ()
    {
        configure();
        initContext.getConfig().set( ACCUMULATOR_INITIALIZER_CONFIG_PARAMETER, createAccumulatorInitializer( 0 ) );
        final SchedulingStrategy strategy = operator.init( initContext );

        assertStrategy( strategy );
    }

    @Test
    public void shouldReduceSingleTupleWithInitialValue ()
    {
        configure();
        initContext.getConfig().set( ACCUMULATOR_INITIALIZER_CONFIG_PARAMETER, createAccumulatorInitializer( 2 ) );
        operator.init( initContext );

        input.add( new Tuple( "count", 1 ) );

        operator.invoke( invocationContext );

        assertThat( output.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 0 ) );

        assertWindow( 0, 1 );
        assertAccumulator( 3 );
    }

    @Test
    public void shouldReduceMultipleTuplesWithInitialValue ()
    {
        configure();
        initContext.getConfig().set( ACCUMULATOR_INITIALIZER_CONFIG_PARAMETER, createAccumulatorInitializer( 3 ) );
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
        configure();
        initContext.getConfig().set( ACCUMULATOR_INITIALIZER_CONFIG_PARAMETER, createAccumulatorInitializer( 4 ) );
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

    private void configure ()
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

}
