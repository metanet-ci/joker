package cs.bilkent.zanza.operators;

import org.junit.Test;

import static cs.bilkent.zanza.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.InvocationContext.InvocationReason;
import cs.bilkent.zanza.operator.InvocationResult;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.impl.InitializationContextImpl;
import cs.bilkent.zanza.operator.impl.InvocationContextImpl;
import cs.bilkent.zanza.operator.kvstore.KVStore;
import cs.bilkent.zanza.operator.kvstore.impl.InMemoryKVStore;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import static cs.bilkent.zanza.operators.ExponentialMovingAverageAggregationOperator.CURRENT_WINDOW_KEY;
import static cs.bilkent.zanza.operators.ExponentialMovingAverageAggregationOperator.TUPLE_COUNT_FIELD;
import static cs.bilkent.zanza.operators.ExponentialMovingAverageAggregationOperator.VALUE_FIELD;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class ExponentialMovingAverageAggregationOperatorTest
{

    private final ExponentialMovingAverageAggregationOperator operator = new ExponentialMovingAverageAggregationOperator();

    private final InitializationContextImpl initContext = new InitializationContextImpl();

    private final PortsToTuples input = new PortsToTuples();

    private final KVStore kvStore = new InMemoryKVStore();

    private final InvocationContext invocationContext = new InvocationContextImpl( InvocationReason.SUCCESS, input, kvStore );

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithNoTupleCount ()
    {
        initContext.getConfig().set( ExponentialMovingAverageAggregationOperator.FIELD_NAME_CONFIG_PARAMETER, "val" );
        operator.init( initContext );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithNoFieldName ()
    {
        initContext.getConfig().set( ExponentialMovingAverageAggregationOperator.WEIGHT_CONFIG_PARAMETER, .5 );
        operator.init( initContext );
    }

    @Test
    public void shouldInitializeWithProperConfig ()
    {
        setConfig();

        final SchedulingStrategy strategy = operator.init( initContext );
        assertTrue( strategy instanceof ScheduleWhenTuplesAvailable );

        final ScheduleWhenTuplesAvailable tupleAvailabilitySchedule = (ScheduleWhenTuplesAvailable) strategy;
        assertThat( tupleAvailabilitySchedule.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 1 ) );
        assertThat( tupleAvailabilitySchedule.getTupleAvailabilityByCount(), equalTo( AT_LEAST ) );
    }

    @Test
    public void shouldSetAccumulatorForFirstValue ()
    {
        setConfig();

        operator.init( initContext );
        input.add( new Tuple( "val", 10 ) );

        operator.invoke( invocationContext );

        final Tuple value = kvStore.get( CURRENT_WINDOW_KEY );
        assertNotNull( value );

        assertValue( value, 10 );
    }

    @Test
    public void shouldSetAccumulatorForSecondValue ()
    {
        setConfig();
        setCurrentAvgInKVStore( 0, 5 );

        operator.init( initContext );
        input.add( new Tuple( "val", 10 ) );

        operator.invoke( invocationContext );

        final Tuple value = kvStore.get( CURRENT_WINDOW_KEY );
        assertNotNull( value );

        assertValue( value, 10 );
    }

    @Test
    public void shouldReturnFirstAverage ()
    {
        setConfig();
        setCurrentAvgInKVStore( 3, 6 );

        operator.init( initContext );
        input.add( new Tuple( "val", 4 ) );

        final InvocationResult result = operator.invoke( invocationContext );
        final PortsToTuples output = result.getOutputTuples();
        assertThat( output.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 1 ) );
        final Tuple tuple = output.getTuple( DEFAULT_PORT_INDEX, 0 );
        assertValue( tuple, 5 );

        final Tuple value = kvStore.get( CURRENT_WINDOW_KEY );
        assertNotNull( value );

        assertValue( value, 5 );
        assertTupleCount( value, 4 );
    }

    @Test
    public void shouldReturnSecondAverage ()
    {
        setConfig();
        setCurrentAvgInKVStore( 4, 10 );

        operator.init( initContext );
        input.add( new Tuple( "val", 5 ) );

        final InvocationResult result = operator.invoke( invocationContext );
        final PortsToTuples output = result.getOutputTuples();
        assertThat( output.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 1 ) );
        final Tuple tuple = output.getTuple( DEFAULT_PORT_INDEX, 0 );
        assertValue( tuple, 7.5 );

        final Tuple value = kvStore.get( CURRENT_WINDOW_KEY );
        assertNotNull( value );

        assertValue( value, 7.5 );
        assertTupleCount( value, 5 );
    }

    @Test
    public void shouldReturnMultipleAverages ()
    {
        setConfig();
        setCurrentAvgInKVStore( 3, 6 );

        operator.init( initContext );
        input.add( new Tuple( "val", 4 ) );
        input.add( new Tuple( "val", 7 ) );

        final InvocationResult result = operator.invoke( invocationContext );
        final PortsToTuples output = result.getOutputTuples();
        assertThat( output.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 2 ) );

        final Tuple tuple1 = output.getTuple( DEFAULT_PORT_INDEX, 0 );
        assertValue( tuple1, 5 );
        final Tuple tuple2 = output.getTuple( DEFAULT_PORT_INDEX, 1 );
        assertValue( tuple2, 6 );

    }

    private void setConfig ()
    {
        initContext.getConfig().set( ExponentialMovingAverageAggregationOperator.FIELD_NAME_CONFIG_PARAMETER, "val" );
        initContext.getConfig().set( ExponentialMovingAverageAggregationOperator.WEIGHT_CONFIG_PARAMETER, .5 );
    }

    private void setCurrentAvgInKVStore ( final int tupleCount, final double value )
    {
        final Tuple tuple = new Tuple();
        tuple.set( TUPLE_COUNT_FIELD, tupleCount );
        tuple.set( VALUE_FIELD, value );
        kvStore.set( CURRENT_WINDOW_KEY, tuple );
    }

    private void assertValue ( final Tuple tuple, final double expectedValue )
    {
        assertThat( tuple.getDouble( VALUE_FIELD ), equalTo( expectedValue ) );
    }

    private void assertTupleCount ( final Tuple tuple, final int expectedTupleCount )
    {
        assertThat( tuple.getInteger( TUPLE_COUNT_FIELD ), equalTo( expectedTupleCount ) );
    }

}
