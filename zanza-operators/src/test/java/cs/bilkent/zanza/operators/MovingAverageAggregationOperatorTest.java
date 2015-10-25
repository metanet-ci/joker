package cs.bilkent.zanza.operators;

import org.junit.Test;

import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.InvocationContext.InvocationReason;
import cs.bilkent.zanza.operator.InvocationResult;
import cs.bilkent.zanza.operator.KVStore;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.SchedulingStrategy;
import cs.bilkent.zanza.operator.Tuple;
import static cs.bilkent.zanza.operator.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.zanza.operator.kvstore.InMemoryKVStore;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import static cs.bilkent.zanza.operators.MovingAverageAggregationOperator.CURRENT_AVERAGE_KEY;
import static cs.bilkent.zanza.operators.MovingAverageAggregationOperator.TUPLE_COUNT_FIELD;
import static cs.bilkent.zanza.operators.MovingAverageAggregationOperator.VALUE_FIELD;
import static cs.bilkent.zanza.operators.MovingAverageAggregationOperator.WINDOW_COUNT_FIELD;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MovingAverageAggregationOperatorTest
{

    private final MovingAverageAggregationOperator operator = new MovingAverageAggregationOperator();

    private final SimpleInitializationContext initContext = new SimpleInitializationContext();

    private final PortsToTuples input = new PortsToTuples();

    private final KVStore kvStore = new InMemoryKVStore();

    private final InvocationContext invocationContext = new SimpleInvocationContext( input, InvocationReason.SUCCESS, kvStore );

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithNoTupleCount ()
    {
        initContext.getConfig().set( MovingAverageAggregationOperator.FIELD_NAME_CONFIG_PARAMETER, "val" );
        operator.init( initContext );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithNoFieldName ()
    {
        initContext.getConfig().set( MovingAverageAggregationOperator.TUPLE_COUNT_CONFIG_PARAMETER, 4 );
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

        final InvocationResult result = operator.process( invocationContext );
        final PortsToTuples output = result.getOutputTuples();
        assertThat( output.getPortCount(), equalTo( 0 ) );

        final Tuple value = kvStore.get( CURRENT_AVERAGE_KEY );
        assertNotNull( value );

        assertThat( value.getInteger( WINDOW_COUNT_FIELD ), equalTo( 0 ) );
        assertThat( value.getDouble( VALUE_FIELD ), equalTo( 10d ) );
    }

    @Test
    public void shouldSetAccumulatorForSecondValue ()
    {
        setConfig();
        setCurrentAvgInKVStore( 0, 0, 5 );

        operator.init( initContext );
        input.add( new Tuple( "val", 10 ) );

        final InvocationResult result = operator.process( invocationContext );
        final PortsToTuples output = result.getOutputTuples();
        assertThat( output.getPortCount(), equalTo( 0 ) );

        final Tuple value = kvStore.get( CURRENT_AVERAGE_KEY );
        assertNotNull( value );

        assertThat( value.getInteger( WINDOW_COUNT_FIELD ), equalTo( 0 ) );
        assertThat( value.getDouble( VALUE_FIELD ), equalTo( 15d ) );
    }

    @Test
    public void shouldReturnFirstAverage ()
    {
        setConfig();
        setCurrentAvgInKVStore( 0, 3, 6 );

        operator.init( initContext );
        input.add( new Tuple( "val", 4 ) );

        final InvocationResult result = operator.process( invocationContext );
        final PortsToTuples output = result.getOutputTuples();
        assertThat( output.getPortCount(), equalTo( 1 ) );
        final Tuple tuple = output.getTuple( DEFAULT_PORT_INDEX, 0 );
        assertThat( tuple.getInteger( WINDOW_COUNT_FIELD ), equalTo( 0 ) );
        assertThat( tuple.getDouble( VALUE_FIELD ), equalTo( 2.5 ) );

        final Tuple value = kvStore.get( CURRENT_AVERAGE_KEY );
        assertNotNull( value );

        assertThat( value.getInteger( WINDOW_COUNT_FIELD ), equalTo( 1 ) );
        assertThat( value.getDouble( VALUE_FIELD ), equalTo( 2.5 ) );
        assertThat( value.getInteger( TUPLE_COUNT_FIELD ), equalTo( 4 ) );
    }

    @Test
    public void shouldReturnSecondAverage ()
    {
        setConfig();
        setCurrentAvgInKVStore( 0, 4, 2.5 );

        operator.init( initContext );
        input.add( new Tuple( "val", 5 ) );

        final InvocationResult result = operator.process( invocationContext );
        final PortsToTuples output = result.getOutputTuples();
        assertThat( output.getPortCount(), equalTo( 1 ) );
        final Tuple tuple = output.getTuple( DEFAULT_PORT_INDEX, 0 );
        assertThat( tuple.getInteger( WINDOW_COUNT_FIELD ), equalTo( 0 ) );
        assertThat( tuple.getDouble( VALUE_FIELD ), equalTo( 3.5 ) );

        final Tuple value = kvStore.get( CURRENT_AVERAGE_KEY );
        assertNotNull( value );

        assertThat( value.getInteger( WINDOW_COUNT_FIELD ), equalTo( 1 ) );
        assertThat( value.getDouble( VALUE_FIELD ), equalTo( 3.5 ) );
        assertThat( value.getInteger( TUPLE_COUNT_FIELD ), equalTo( 4 ) );
    }

    private void setConfig ()
    {
        initContext.getConfig().set( MovingAverageAggregationOperator.FIELD_NAME_CONFIG_PARAMETER, "val" );
        initContext.getConfig().set( MovingAverageAggregationOperator.TUPLE_COUNT_CONFIG_PARAMETER, 4 );
    }

    private void setCurrentAvgInKVStore ( final int window, final int tupleCount, final double value )
    {
        final Tuple tuple = new Tuple();
        tuple.set( WINDOW_COUNT_FIELD, window );
        tuple.set( TUPLE_COUNT_FIELD, tupleCount );
        tuple.set( VALUE_FIELD, value );
        kvStore.set( CURRENT_AVERAGE_KEY, tuple );
    }

}
