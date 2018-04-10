package cs.bilkent.joker.operators;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Test;

import static cs.bilkent.joker.flow.Port.DEFAULT_PORT_INDEX;
import static cs.bilkent.joker.operator.InvocationCtx.InvocationReason.SUCCESS;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.DefaultInvocationCtx;
import cs.bilkent.joker.operator.impl.InMemoryKVStore;
import cs.bilkent.joker.operator.impl.InitCtxImpl;
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

    private final DefaultInvocationCtx invocationCtx = new DefaultInvocationCtx( 1, key -> kvStore, output );

    private final PartitionKey key = new PartitionKey1( "key" );

    private final TuplesImpl input = invocationCtx.createInputTuples( key );

    private final OperatorConfig config = new OperatorConfig();

    private TupleCountBasedWindowReducerOperator operator;

    private InitCtxImpl initCtx;

    private final BiConsumer<Tuple, Tuple> adder = ( accumulator, val ) -> {
        final int curr = accumulator.getInteger( "count" );
        accumulator.set( "count", curr + val.getInteger( "count" ) );
    };

    private final int tupleCount = 2;

    @Before
    public void init () throws InstantiationException, IllegalAccessException
    {
        invocationCtx.setInvocationReason( SUCCESS );

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
        initCtx = new InitCtxImpl( operatorDef, new boolean[] { true } );
    }

    private Consumer<Tuple> createAccumulatorInitializer ( final int initialValue )
    {
        return accumulator -> accumulator.set( "count", initialValue );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailToInitWithoutReducer ()
    {
        config.set( TUPLE_COUNT_CONFIG_PARAMETER, tupleCount )
              .set( ACCUMULATOR_INITIALIZER_CONFIG_PARAMETER, createAccumulatorInitializer( 0 ) );
        operator.init( initCtx );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailToInitWithoutTupleCount ()
    {
        config.set( REDUCER_CONFIG_PARAMETER, adder ).set( ACCUMULATOR_INITIALIZER_CONFIG_PARAMETER, createAccumulatorInitializer( 0 ) );
        operator.init( initCtx );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailToInitWithoutAccumulatorInitializer ()
    {
        config.set( TUPLE_COUNT_CONFIG_PARAMETER, tupleCount ).set( REDUCER_CONFIG_PARAMETER, adder );
        operator.init( initCtx );
    }

    @Test
    public void shouldInitSuccessfully ()
    {
        configure();
        config.set( ACCUMULATOR_INITIALIZER_CONFIG_PARAMETER, createAccumulatorInitializer( 0 ) );
        final SchedulingStrategy strategy = operator.init( initCtx );

        assertStrategy( strategy );
    }

    @Test
    public void shouldReduceSingleTupleWithInitialValue ()
    {
        configure();
        config.set( ACCUMULATOR_INITIALIZER_CONFIG_PARAMETER, createAccumulatorInitializer( 2 ) );
        operator.init( initCtx );

        final Tuple tuple = Tuple.of( "key", "key", "count", 1 );
        input.add( tuple );

        operator.invoke( invocationCtx );

        assertThat( output.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 0 ) );

        assertWindow( 0, 1 );
        assertAccumulator( 3 );
    }

    @Test
    public void shouldReduceMultipleTuplesWithInitialValue ()
    {
        configure();
        config.set( ACCUMULATOR_INITIALIZER_CONFIG_PARAMETER, createAccumulatorInitializer( 3 ) );
        operator.init( initCtx );

        final Tuple tuple1 = Tuple.of( "key", "key", "count", 1 );
        final Tuple tuple2 = Tuple.of( "key", "key", "count", 2 );
        input.add( tuple1, tuple2 );

        operator.invoke( invocationCtx );

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
        operator.init( initCtx );

        final Tuple tuple1 = Tuple.of( "key", "key", "count", 1 );
        final Tuple tuple2 = Tuple.of( "key", "key", "count", 2 );
        final Tuple tuple3 = Tuple.of( "key", "key", "count", 3 );
        input.add( tuple1, tuple2, tuple3 );

        operator.invoke( invocationCtx );

        assertThat( output.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 1 ) );

        assertOutput( output.getTupleOrFail( DEFAULT_PORT_INDEX, 0 ), 0, 7 );
        assertWindow( 1, 1 );
        assertAccumulator( 7 );
    }

    private void configure ()
    {
        config.set( REDUCER_CONFIG_PARAMETER, adder ).set( TUPLE_COUNT_CONFIG_PARAMETER, tupleCount );
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
