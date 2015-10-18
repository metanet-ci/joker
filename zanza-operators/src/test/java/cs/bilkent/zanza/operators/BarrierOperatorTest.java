package cs.bilkent.zanza.operators;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import cs.bilkent.zanza.operator.Port;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.ProcessingResult;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.invocationreason.ShutdownRequested;
import cs.bilkent.zanza.operator.invocationreason.SuccessfulInvocation;
import cs.bilkent.zanza.operator.scheduling.ScheduleNever;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import cs.bilkent.zanza.operators.BarrierOperator.TupleValueMergePolicy;
import static cs.bilkent.zanza.operators.BarrierOperator.TupleValueMergePolicy.KEEP_EXISTING_VALUE;
import static cs.bilkent.zanza.operators.BarrierOperator.TupleValueMergePolicy.OVERWRITE_WITH_NEW_VALUE;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertTrue;

public class BarrierOperatorTest
{

    private final BarrierOperator operator = new BarrierOperator();

    private final SimpleOperatorContext operatorContext = new SimpleOperatorContext();

    private final List<Port> inputPorts = asList( new Port( "", 0 ), new Port( "", 1 ), new Port( "", 2 ), new Port( "", 3 ) );

    @Before
    public void init ()
    {
        operatorContext.setInputPorts( inputPorts );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithNoMergePolicy ()
    {
        operator.init( new SimpleOperatorContext() );
    }

    @Test
    public void shouldScheduleOnGivenInputPorts ()
    {
        operatorContext.getConfig().set( BarrierOperator.MERGE_POLICY_CONfIG_PARAMETER, KEEP_EXISTING_VALUE );
        final SchedulingStrategy initialStrategy = operator.init( operatorContext );
        assertSchedulingStrategy( initialStrategy );
    }

    private void assertSchedulingStrategy ( final SchedulingStrategy initialStrategy )
    {
        assertTrue( initialStrategy instanceof ScheduleWhenTuplesAvailable );
        final ScheduleWhenTuplesAvailable strategy = (ScheduleWhenTuplesAvailable) initialStrategy;

        final int tupleExpectedPortCount = (int) inputPorts.stream()
                                                           .map( port -> port.portIndex )
                                                           .map( strategy::getTupleCount )
                                                           .filter( count -> count == 1 )
                                                           .count();

        assertThat( tupleExpectedPortCount, equalTo( inputPorts.size() ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithMissingTuplesOnSuccessfulInvocation ()
    {
        operatorContext.getConfig().set( BarrierOperator.MERGE_POLICY_CONfIG_PARAMETER, KEEP_EXISTING_VALUE );
        operator.init( operatorContext );
        operator.process( new PortsToTuples(), SuccessfulInvocation.INSTANCE );
    }

    @Test
    public void shouldNotFailWithMissingTuplesOnErroneousInvocation ()
    {
        operatorContext.getConfig().set( BarrierOperator.MERGE_POLICY_CONfIG_PARAMETER, KEEP_EXISTING_VALUE );
        operator.init( operatorContext );
        final ProcessingResult result = operator.process( new PortsToTuples(), ShutdownRequested.INSTANCE );
        assertTrue( result.getSchedulingStrategy() instanceof ScheduleNever );
        assertThat( result.getPortsToTuples().getPortCount(), equalTo( 0 ) );
    }

    @Test
    public void shouldMergeTuples ()
    {
        operatorContext.getConfig().set( BarrierOperator.MERGE_POLICY_CONfIG_PARAMETER, KEEP_EXISTING_VALUE );
        operator.init( operatorContext );

        final PortsToTuples input = new PortsToTuples();
        inputPorts.stream()
                  .map( port -> port.portIndex )
                  .forEach( portIndex -> input.add( portIndex, new Tuple( "field" + portIndex, portIndex ) ) );

        final ProcessingResult result = operator.process( input, SuccessfulInvocation.INSTANCE );
        assertSchedulingStrategy( result.getSchedulingStrategy() );
        final Tuple output = result.getPortsToTuples().getTuple( 0, 0 );
        final int matchingFieldCount = (int) inputPorts.stream()
                                                       .map( port -> port.portIndex )
                                                       .filter( portIndex -> output.getInteger( "field" + portIndex ).equals( portIndex ) )
                                                       .count();

        assertThat( matchingFieldCount, equalTo( inputPorts.size() ) );
    }

    @Test
    public void shouldMergeTuplesWithKeepingExistingValue ()
    {
        testTupleMergeWithMergePolicy( KEEP_EXISTING_VALUE, inputPorts.get( 0 ).portIndex );
    }

    @Test
    public void shouldMergeTuplesWithOverwritingWithNewValue ()
    {
        testTupleMergeWithMergePolicy( OVERWRITE_WITH_NEW_VALUE, inputPorts.get( inputPorts.size() - 1 ).portIndex );
    }

    private void testTupleMergeWithMergePolicy ( final TupleValueMergePolicy mergePolicy, final int expectedValue )
    {
        operatorContext.getConfig().set( BarrierOperator.MERGE_POLICY_CONfIG_PARAMETER, mergePolicy );
        operator.init( operatorContext );

        final PortsToTuples input = new PortsToTuples();
        inputPorts.stream().map( port -> port.portIndex ).forEach( portIndex -> input.add( portIndex, new Tuple( "count", portIndex ) ) );
        final ProcessingResult result = operator.process( input, SuccessfulInvocation.INSTANCE );

        final Tuple output = result.getPortsToTuples().getTuple( 0, 0 );
        assertThat( output.getInteger( "count" ), equalTo( expectedValue ) );
    }

}
