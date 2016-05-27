package cs.bilkent.zanza.operators;

import java.util.List;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;

import static cs.bilkent.zanza.operator.InvocationContext.InvocationReason.SHUTDOWN;
import static cs.bilkent.zanza.operator.InvocationContext.InvocationReason.SUCCESS;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.impl.InitializationContextImpl;
import cs.bilkent.zanza.operator.impl.InvocationContextImpl;
import cs.bilkent.zanza.operator.impl.TuplesImpl;
import cs.bilkent.zanza.operator.scheduling.ScheduleNever;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import static cs.bilkent.zanza.operators.BarrierOperator.MERGE_POLICY_CONfIG_PARAMETER;
import cs.bilkent.zanza.operators.BarrierOperator.TupleValueMergePolicy;
import static cs.bilkent.zanza.operators.BarrierOperator.TupleValueMergePolicy.KEEP_EXISTING_VALUE;
import static cs.bilkent.zanza.operators.BarrierOperator.TupleValueMergePolicy.OVERWRITE_WITH_NEW_VALUE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class BarrierOperatorTest
{

    private final BarrierOperator operator = new BarrierOperator();

    private final InitializationContextImpl initContext = new InitializationContextImpl();

    private final int[] inputPorts = new int[] { 0, 1, 2 };

    private final TuplesImpl input = new TuplesImpl( 3 );

    private final TuplesImpl output = new TuplesImpl( 3 );

    private final InvocationContextImpl invocationContext = new InvocationContextImpl( SUCCESS, input, output );

    @Before
    public void init ()
    {
        initContext.setInputPortCount( inputPorts.length );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithNoMergePolicy ()
    {
        operator.init( new InitializationContextImpl() );
    }

    @Test
    public void shouldScheduleOnGivenInputPorts ()
    {
        initContext.getConfig().set( MERGE_POLICY_CONfIG_PARAMETER, KEEP_EXISTING_VALUE );
        final SchedulingStrategy initialStrategy = operator.init( initContext );
        assertSchedulingStrategy( initialStrategy );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithMissingTuplesOnSuccessfulInvocation ()
    {
        initContext.getConfig().set( MERGE_POLICY_CONfIG_PARAMETER, KEEP_EXISTING_VALUE );
        operator.init( initContext );
        operator.invoke( invocationContext );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithMissingTuplesOnErroneousInvocation ()
    {
        initContext.getConfig().set( MERGE_POLICY_CONfIG_PARAMETER, KEEP_EXISTING_VALUE );
        operator.init( initContext );
        invocationContext.setReason( SHUTDOWN );

        operator.invoke( invocationContext );
        assertTrue( invocationContext.getSchedulingStrategy() instanceof ScheduleNever );
        assertThat( invocationContext.getOutput().getNonEmptyPortCount(), equalTo( 0 ) );
    }

    @Test
    public void shouldMergeSingleTuplePerPort ()
    {
        initContext.getConfig().set( MERGE_POLICY_CONfIG_PARAMETER, KEEP_EXISTING_VALUE );
        operator.init( initContext );

        populateTuplesWithUniqueFields( input );

        operator.invoke( invocationContext );
        assertNull( invocationContext.getSchedulingStrategy() );
        final Tuple outputTuple = output.getTupleOrFail( 0, 0 );
        final int matchingFieldCount = getMatchingFieldCount( outputTuple );

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
        initContext.getConfig().set( MERGE_POLICY_CONfIG_PARAMETER, mergePolicy );
        operator.init( initContext );

        IntStream.of( inputPorts ).forEach( portIndex -> {
            final Tuple tuple = new Tuple( 1, "count", portIndex );
            input.add( portIndex, tuple );
        } );

        operator.invoke( invocationContext );

        final Tuple outputTuple = output.getTupleOrFail( 0, 0 );
        assertThat( outputTuple.getInteger( "count" ), equalTo( expectedValue ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailForDifferentNumberOfTuplesPerPort ()
    {
        initContext.getConfig().set( MERGE_POLICY_CONfIG_PARAMETER, KEEP_EXISTING_VALUE );
        operator.init( initContext );

        IntStream.of( inputPorts ).forEach( portIndex -> {
            final Tuple tuple = new Tuple( "count", portIndex );
            input.add( portIndex, tuple );
        } );
        input.add( new Tuple( "count", -1 ) );

        operator.invoke( invocationContext );
    }

    @Test
    public void shouldMergeMultipleTuplesPerPort ()
    {
        initContext.getConfig().set( MERGE_POLICY_CONfIG_PARAMETER, KEEP_EXISTING_VALUE );
        operator.init( initContext );

        populateTuplesWithUniqueFields( input );
        populateTuplesWithUniqueFields( input );

        operator.invoke( invocationContext );
        assertNull( invocationContext.getSchedulingStrategy() );

        final List<Tuple> outputTuples = output.getTuplesByDefaultPort();
        assertThat( outputTuples, hasSize( 2 ) );
        final Tuple tuple1 = outputTuples.get( 0 );
        assertThat( getMatchingFieldCount( tuple1 ), equalTo( inputPorts.length ) );
        final Tuple tuple2 = outputTuples.get( 1 );
        assertThat( getMatchingFieldCount( tuple2 ), equalTo( inputPorts.length ) );
    }

    private int getMatchingFieldCount ( final Tuple tuple )
    {
        return (int) IntStream.of( inputPorts ).filter( portIndex -> tuple.getInteger( "field" + portIndex ).equals( portIndex ) ).count();
    }

    private void populateTuplesWithUniqueFields ( final TuplesImpl input )
    {
        IntStream.of( inputPorts ).forEach( portIndex -> {
            final Tuple tuple = new Tuple( "field" + portIndex, portIndex );
            input.add( portIndex, tuple );
        } );
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

}
