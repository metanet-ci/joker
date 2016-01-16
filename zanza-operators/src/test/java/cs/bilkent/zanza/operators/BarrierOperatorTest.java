package cs.bilkent.zanza.operators;

import java.util.List;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;

import cs.bilkent.zanza.operator.InvocationContext.InvocationReason;
import cs.bilkent.zanza.operator.InvocationResult;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operators.BarrierOperator.TupleValueMergePolicy;
import static cs.bilkent.zanza.operators.BarrierOperator.TupleValueMergePolicy.KEEP_EXISTING_VALUE;
import static cs.bilkent.zanza.operators.BarrierOperator.TupleValueMergePolicy.OVERWRITE_WITH_NEW_VALUE;
import cs.bilkent.zanza.scheduling.ScheduleNever;
import cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable;
import cs.bilkent.zanza.scheduling.SchedulingStrategy;
import cs.bilkent.zanza.utils.SimpleInitializationContext;
import cs.bilkent.zanza.utils.SimpleInvocationContext;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertTrue;


public class BarrierOperatorTest
{

    private final BarrierOperator operator = new BarrierOperator();

    private final SimpleInitializationContext initContext = new SimpleInitializationContext();

    private final int[] inputPorts = new int[] { 0, 1, 2 };

    @Before
    public void init ()
    {
        initContext.setInputPortCount( inputPorts.length );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithNoMergePolicy ()
    {
        operator.init( new SimpleInitializationContext() );
    }

    @Test
    public void shouldScheduleOnGivenInputPorts ()
    {
        initContext.getConfig().set( BarrierOperator.MERGE_POLICY_CONfIG_PARAMETER, KEEP_EXISTING_VALUE );
        final SchedulingStrategy initialStrategy = operator.init( initContext );
        assertSchedulingStrategy( initialStrategy );
    }

    private void assertSchedulingStrategy ( final SchedulingStrategy initialStrategy )
    {
        assertTrue( initialStrategy instanceof ScheduleWhenTuplesAvailable );
        final ScheduleWhenTuplesAvailable strategy = (ScheduleWhenTuplesAvailable) initialStrategy;

        final int tupleExpectedPortCount = (int) IntStream.of( inputPorts )
                                                          .map( strategy::getTupleCount )
                                                          .filter( count -> count == 1 )
                                                          .count();

        assertThat( tupleExpectedPortCount, equalTo( inputPorts.length ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithMissingTuplesOnSuccessfulInvocation ()
    {
        initContext.getConfig().set( BarrierOperator.MERGE_POLICY_CONfIG_PARAMETER, KEEP_EXISTING_VALUE );
        operator.init( initContext );
        operator.process( new SimpleInvocationContext( InvocationReason.SUCCESS, new PortsToTuples() ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithMissingTuplesOnErroneousInvocation ()
    {
        initContext.getConfig().set( BarrierOperator.MERGE_POLICY_CONfIG_PARAMETER, KEEP_EXISTING_VALUE );
        operator.init( initContext );
        final InvocationResult result = operator.process( new SimpleInvocationContext( InvocationReason.SHUTDOWN, new PortsToTuples() ) );
        assertTrue( result.getSchedulingStrategy() instanceof ScheduleNever );
        assertThat( result.getOutputTuples().getPortCount(), equalTo( 0 ) );
    }

    @Test
    public void shouldMergeSingleTuplePerPort ()
    {
        initContext.getConfig().set( BarrierOperator.MERGE_POLICY_CONfIG_PARAMETER, KEEP_EXISTING_VALUE );
        operator.init( initContext );

        final PortsToTuples input = new PortsToTuples();
        populateTuplesWithUniqueFields( input );

        final InvocationResult result = operator.process( new SimpleInvocationContext( InvocationReason.SUCCESS, input ) );
        assertSchedulingStrategy( result.getSchedulingStrategy() );
        final Tuple output = result.getOutputTuples().getTuple( 0, 0 );
        final int matchingFieldCount = getMatchingFieldCount( output );

        assertThat( matchingFieldCount, equalTo( inputPorts.length ) );
    }

    @Test
    public void shouldMergeTuplesWithKeepingExistingValue ()
    {
        testTupleMergeWithMergePolicy( KEEP_EXISTING_VALUE, inputPorts[ 0 ] );
    }

    @Test
    public void shouldMergeTuplesWithOverwritingWithNewValue ()
    {
        testTupleMergeWithMergePolicy( OVERWRITE_WITH_NEW_VALUE, inputPorts[ inputPorts.length - 1 ] );
    }

    private void testTupleMergeWithMergePolicy ( final TupleValueMergePolicy mergePolicy, final int expectedValue )
    {
        initContext.getConfig().set( BarrierOperator.MERGE_POLICY_CONfIG_PARAMETER, mergePolicy );
        operator.init( initContext );

        final PortsToTuples input = new PortsToTuples();
        IntStream.of( inputPorts ).forEach( portIndex -> {
            final Tuple tuple = new Tuple( 1, "count", portIndex );
            input.add( portIndex, tuple );
        } );
        final InvocationResult result = operator.process( new SimpleInvocationContext( InvocationReason.SUCCESS, input ) );

        final Tuple output = result.getOutputTuples().getTuple( 0, 0 );
        assertThat( output.getInteger( "count" ), equalTo( expectedValue ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailForDifferentNumberOfTuplesPerPort ()
    {
        initContext.getConfig().set( BarrierOperator.MERGE_POLICY_CONfIG_PARAMETER, TupleValueMergePolicy.KEEP_EXISTING_VALUE );
        operator.init( initContext );
        final PortsToTuples input = new PortsToTuples();
        IntStream.of( inputPorts ).forEach( portIndex -> {
            final Tuple tuple = new Tuple( "count", portIndex );
            input.add( portIndex, tuple );
        } );
        input.add( new Tuple( "count", -1 ) );

        operator.process( new SimpleInvocationContext( InvocationReason.SUCCESS, input ) );
    }

    @Test
    public void shouldMergeMultipleTuplesPerPort ()
    {
        initContext.getConfig().set( BarrierOperator.MERGE_POLICY_CONfIG_PARAMETER, KEEP_EXISTING_VALUE );
        operator.init( initContext );

        final PortsToTuples input = new PortsToTuples();
        populateTuplesWithUniqueFields( input );
        populateTuplesWithUniqueFields( input );

        final InvocationResult result = operator.process( new SimpleInvocationContext( InvocationReason.SUCCESS, input ) );
        assertSchedulingStrategy( result.getSchedulingStrategy() );

        final List<Tuple> output = result.getOutputTuples().getTuplesByDefaultPort();
        assertThat( output, hasSize( 2 ) );
        final Tuple tuple1 = output.get( 0 );
        assertThat( getMatchingFieldCount( tuple1 ), equalTo( inputPorts.length ) );
        final Tuple tuple2 = output.get( 1 );
        assertThat( getMatchingFieldCount( tuple2 ), equalTo( inputPorts.length ) );
    }

    private int getMatchingFieldCount ( final Tuple tuple )
    {
        return (int) IntStream.of( inputPorts ).filter( portIndex -> tuple.getInteger( "field" + portIndex ).equals( portIndex ) ).count();
    }

    private void populateTuplesWithUniqueFields ( final PortsToTuples input )
    {
        IntStream.of( inputPorts ).forEach( portIndex -> {
            final Tuple tuple = new Tuple( "field" + portIndex, portIndex );
            input.add( portIndex, tuple );
        } );
    }

}
