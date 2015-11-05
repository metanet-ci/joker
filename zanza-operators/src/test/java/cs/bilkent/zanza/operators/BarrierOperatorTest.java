package cs.bilkent.zanza.operators;

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
import static org.junit.Assert.assertTrue;

public class BarrierOperatorTest
{

    private final BarrierOperator operator = new BarrierOperator();

    private final SimpleInitializationContext initContext = new SimpleInitializationContext();

    private final int[] inputPorts = new int[] { 0, 1, 2 };

    @Before
    public void init ()
    {
        initContext.getConfig().setInputPortCount( inputPorts.length );
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
        operator.process( new SimpleInvocationContext( new PortsToTuples(), InvocationReason.SUCCESS ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithMissingTuplesOnErroneousInvocation ()
    {
        initContext.getConfig().set( BarrierOperator.MERGE_POLICY_CONfIG_PARAMETER, KEEP_EXISTING_VALUE );
        operator.init( initContext );
        final InvocationResult result = operator.process( new SimpleInvocationContext( new PortsToTuples(), InvocationReason.SHUTDOWN ) );
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

        final InvocationResult result = operator.process( new SimpleInvocationContext( input, InvocationReason.SUCCESS ) );
        assertSchedulingStrategy( result.getSchedulingStrategy() );
        final Tuple output = result.getOutputTuples().getTuple( 0, 0 );
        final int matchingFieldCount = (int) IntStream.of( inputPorts )
                                                       .filter( portIndex -> output.getInteger( "field" + portIndex ).equals( portIndex ) )
                                                       .count();

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
        IntStream.of( inputPorts ).forEach( portIndex -> input.add( portIndex, new Tuple( "count", portIndex ) ) );
        final InvocationResult result = operator.process( new SimpleInvocationContext( input, InvocationReason.SUCCESS ) );

        final Tuple output = result.getOutputTuples().getTuple( 0, 0 );
        assertThat( output.getInteger( "count" ), equalTo( expectedValue ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailForDifferentNumberOfTuplesPerPort ()
    {
        initContext.getConfig().set( BarrierOperator.MERGE_POLICY_CONfIG_PARAMETER, TupleValueMergePolicy.KEEP_EXISTING_VALUE );
        operator.init( initContext );
        final PortsToTuples input = new PortsToTuples();
        IntStream.of( inputPorts ).forEach( portIndex -> input.add( portIndex, new Tuple( "count", portIndex ) ) );
        input.add( new Tuple( "count", -1 ) );

        operator.process( new SimpleInvocationContext( input, InvocationReason.SUCCESS ) );
    }

    @Test
    public void shouldMergeMultipleTuplesPerPort ()
    {
        initContext.getConfig().set( BarrierOperator.MERGE_POLICY_CONfIG_PARAMETER, KEEP_EXISTING_VALUE );
        operator.init( initContext );

        final PortsToTuples input = new PortsToTuples();
        populateTuplesWithUniqueFields( input );
        populateTuplesWithUniqueFields( input );

        final InvocationResult result = operator.process( new SimpleInvocationContext( input, InvocationReason.SUCCESS ) );
        assertSchedulingStrategy( result.getSchedulingStrategy() );

        result.getOutputTuples().getTuplesByDefaultPort().forEach( output -> {
            final int matchingFieldCount = (int) IntStream.of( inputPorts )
                                                           .filter( portIndex -> output.getInteger( "field" + portIndex )
                                                                                       .equals( portIndex ) )
                                                           .count();

            assertThat( matchingFieldCount, equalTo( inputPorts.length ) );
        } );
    }

    private void populateTuplesWithUniqueFields ( final PortsToTuples input )
    {
        IntStream.of( inputPorts )
                  .forEach( portIndex -> input.add( portIndex, new Tuple( "field" + portIndex, portIndex ) ) );
    }

}
