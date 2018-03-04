package cs.bilkent.joker.operators;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Test;

import static cs.bilkent.joker.flow.Port.DEFAULT_PORT_INDEX;
import static cs.bilkent.joker.operator.InvocationContext.InvocationReason.SUCCESS;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.DefaultInvocationContext;
import cs.bilkent.joker.operator.impl.InMemoryKVStore;
import cs.bilkent.joker.operator.impl.InitializationContextImpl;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.kvstore.KVStore;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchemaBuilder;
import static cs.bilkent.joker.operators.TupleCountBasedWindowReducerOperator.ACCUMULATOR_INITIALIZER_CONFIG_PARAMETER;
import static cs.bilkent.joker.operators.TupleCountBasedWindowReducerOperator.REDUCER_CONFIG_PARAMETER;
import static cs.bilkent.joker.operators.TupleCountBasedWindowReducerOperator.TUPLE_COUNT_CONFIG_PARAMETER;
import static cs.bilkent.joker.operators.TupleCountBasedWindowReducerOperator.TUPLE_COUNT_FIELD;
import static cs.bilkent.joker.operators.TupleCountBasedWindowReducerOperator.WINDOW_FIELD;
import cs.bilkent.joker.partition.impl.PartitionKey;
import cs.bilkent.joker.partition.impl.PartitionKey1;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class TupleCountBasedWindowReducerOperatorTest extends AbstractJokerTest
{

    private final KVStore kvStore = new InMemoryKVStore();

    private final TuplesImpl output = new TuplesImpl( 1 );

    private final DefaultInvocationContext invocationContext = new DefaultInvocationContext( 1, key -> kvStore, output );

    private final PartitionKey key = new PartitionKey1( "key" );

    private final TuplesImpl input = invocationContext.createInputTuples( key );

    private final OperatorConfig config = new OperatorConfig();

    private TupleCountBasedWindowReducerOperator operator;

    private InitializationContextImpl initContext;

    private final BiConsumer<Tuple, Tuple> adder = ( accumulator, val ) -> {
        final int curr = accumulator.getInteger( "count" );
        accumulator.set( "count", curr + val.getInteger( "count" ) );
    };

    private final int tupleCount = 2;

    @Before
    public void init () throws InstantiationException, IllegalAccessException
    {
        invocationContext.setInvocationReason( SUCCESS );

        final OperatorRuntimeSchemaBuilder builder = new OperatorRuntimeSchemaBuilder( 1, 1 );
        builder.addInputField( 0, "key", String.class )
               .addInputField( 0, "count", Integer.class )
               .addOutputField( 0, "count", Integer.class );

        final OperatorDef operatorDef = OperatorDefBuilder.newInstance( "op", TupleCountBasedWindowReducerOperator.class )
                                                          .setConfig( config )
                                                          .setExtendingSchema( builder )
                                                          .setPartitionFieldNames( singletonList( "key" ) )
                                                          .build();
        operator = (TupleCountBasedWindowReducerOperator) operatorDef.createOperator();
        initContext = new InitializationContextImpl( operatorDef, new boolean[] { true } );
    }

    private Consumer<Tuple> createAccumulatorInitializer ( final int initialValue )
    {
        return accumulator -> accumulator.set( "count", initialValue );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailToInitWithoutReducer ()
    {
        config.set( TUPLE_COUNT_CONFIG_PARAMETER, tupleCount );
        config.set( ACCUMULATOR_INITIALIZER_CONFIG_PARAMETER, createAccumulatorInitializer( 0 ) );
        operator.init( initContext );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailToInitWithoutTupleCount ()
    {
        config.set( REDUCER_CONFIG_PARAMETER, adder );
        config.set( ACCUMULATOR_INITIALIZER_CONFIG_PARAMETER, createAccumulatorInitializer( 0 ) );
        operator.init( initContext );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailToInitWithoutAccumulatorInitializer ()
    {
        config.set( TUPLE_COUNT_CONFIG_PARAMETER, tupleCount );
        config.set( REDUCER_CONFIG_PARAMETER, adder );
        operator.init( initContext );
    }

    @Test
    public void shouldInitSuccessfully ()
    {
        configure();
        config.set( ACCUMULATOR_INITIALIZER_CONFIG_PARAMETER, createAccumulatorInitializer( 0 ) );
        final SchedulingStrategy strategy = operator.init( initContext );

        assertStrategy( strategy );
    }

    @Test
    public void shouldReduceSingleTupleWithInitialValue ()
    {
        configure();
        config.set( ACCUMULATOR_INITIALIZER_CONFIG_PARAMETER, createAccumulatorInitializer( 2 ) );
        operator.init( initContext );

        final Tuple tuple = new Tuple();
        tuple.set( "key", "key" );
        tuple.set( "count", 1 );
        input.add( tuple );

        operator.invoke( invocationContext );

        assertThat( output.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 0 ) );

        assertWindow( 0, 1 );
        assertAccumulator( 3 );
    }

    @Test
    public void shouldReduceMultipleTuplesWithInitialValue ()
    {
        configure();
        config.set( ACCUMULATOR_INITIALIZER_CONFIG_PARAMETER, createAccumulatorInitializer( 3 ) );
        operator.init( initContext );

        final Tuple tuple1 = new Tuple();
        tuple1.set( "key", "key" );
        tuple1.set( "count", 1 );
        input.add( tuple1 );
        final Tuple tuple2 = new Tuple();
        tuple2.set( "key", "key" );
        tuple2.set( "count", 2 );
        input.add( tuple2 );

        operator.invoke( invocationContext );

        assertThat( output.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 1 ) );

        assertOutput( output.getTupleOrFail( DEFAULT_PORT_INDEX, 0 ), 0, 6 );
        assertWindow( 1, 0 );
        assertAccumulator( 3 );
    }

    @Test
    public void shouldReduceMultipleTuplesWithInitialValue2 ()
    {
        configure();
        config.set( ACCUMULATOR_INITIALIZER_CONFIG_PARAMETER, createAccumulatorInitializer( 4 ) );
        operator.init( initContext );

        final Tuple tuple1 = new Tuple();
        tuple1.set( "key", "key" );
        tuple1.set( "count", 1 );
        input.add( tuple1 );
        final Tuple tuple2 = new Tuple();
        tuple2.set( "key", "key" );
        tuple2.set( "count", 2 );
        input.add( tuple2 );
        final Tuple tuple3 = new Tuple();
        tuple3.set( "key", "key" );
        tuple3.set( "count", 3 );
        input.add( tuple3 );

        operator.invoke( invocationContext );

        assertThat( output.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 1 ) );

        assertOutput( output.getTupleOrFail( DEFAULT_PORT_INDEX, 0 ), 0, 7 );
        assertWindow( 1, 1 );
        assertAccumulator( 7 );
    }

    private void configure ()
    {
        config.set( REDUCER_CONFIG_PARAMETER, adder );
        config.set( TUPLE_COUNT_CONFIG_PARAMETER, tupleCount );
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

    private void assertOutput ( final Tuple tuple, final int window, final int count )
    {
        assertThat( tuple.getInteger( WINDOW_FIELD ), equalTo( window ) );
        assertThat( tuple.getInteger( "count" ), equalTo( count ) );
    }

}
