package cs.bilkent.joker.operators;

import java.util.List;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;

import static cs.bilkent.joker.operator.InvocationCtx.InvocationReason.SHUTDOWN;
import static cs.bilkent.joker.operator.InvocationCtx.InvocationReason.SUCCESS;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.DefaultInvocationCtx;
import cs.bilkent.joker.operator.impl.InitCtxImpl;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchemaBuilder;
import static cs.bilkent.joker.operators.BarrierOperator.MERGE_POLICY_CONfIG_PARAMETER;
import cs.bilkent.joker.operators.BarrierOperator.TupleValueMergePolicy;
import static cs.bilkent.joker.operators.BarrierOperator.TupleValueMergePolicy.KEEP_EXISTING_VALUE;
import static cs.bilkent.joker.operators.BarrierOperator.TupleValueMergePolicy.OVERWRITE_WITH_NEW_VALUE;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.fill;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertTrue;


public class BarrierOperatorTest extends AbstractJokerTest
{

    private final BarrierOperator operator = new BarrierOperator();

    private final OperatorConfig config = new OperatorConfig();

    private InitCtxImpl initCtx;

    private final int[] inputPorts = new int[] { 0, 1, 2 };

    private final OperatorRuntimeSchemaBuilder schemaBuilder = new OperatorRuntimeSchemaBuilder( inputPorts.length, 1 );

    private final TuplesImpl output = new TuplesImpl( 3 );

    private final DefaultInvocationCtx invocationCtx = new DefaultInvocationCtx( inputPorts.length, key -> null, output );

    private final TuplesImpl input = invocationCtx.createInputTuples( null );

    @Before
    public void init ()
    {
        final OperatorDef operatorDef = OperatorDefBuilder.newInstance( "op", BarrierOperator.class )
                                                          .setInputPortCount( inputPorts.length )
                                                          .setExtendingSchema( schemaBuilder )
                                                          .setConfig( config )
                                                          .build();

        final boolean[] upstream = new boolean[ inputPorts.length ];
        fill( upstream, true );

        initCtx = new InitCtxImpl( operatorDef, upstream );
        invocationCtx.setInvocationReason( SUCCESS );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithNoMergePolicy ()
    {
        operator.init( initCtx );
    }

    @Test
    public void shouldScheduleOnGivenInputPorts ()
    {
        config.set( MERGE_POLICY_CONfIG_PARAMETER, KEEP_EXISTING_VALUE );
        final SchedulingStrategy initialStrategy = operator.init( initCtx );
        assertSchedulingStrategy( initialStrategy );
    }

    @Test
    public void shouldNotFailWithMissingTuplesOnErroneousInvocation ()
    {
        config.set( MERGE_POLICY_CONfIG_PARAMETER, KEEP_EXISTING_VALUE );
        operator.init( initCtx );

        invocationCtx.setInvocationReason( SHUTDOWN );

        populateTuplesWithUniqueFields( input );
        final Tuple tuple = Tuple.of( "field0", 0 );
        input.add( tuple );

        operator.invoke( invocationCtx );
        final Tuple outputTuple = output.getTupleOrFail( 0, 0 );
        final int matchingFieldCount = getMatchingFieldCount( outputTuple );

        assertThat( matchingFieldCount, equalTo( inputPorts.length ) );
    }

    @Test
    public void shouldMergeSingleTuplePerPort ()
    {
        config.set( MERGE_POLICY_CONfIG_PARAMETER, KEEP_EXISTING_VALUE );
        operator.init( initCtx );

        populateTuplesWithUniqueFields( input );

        operator.invoke( invocationCtx );
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
        config.set( MERGE_POLICY_CONfIG_PARAMETER, mergePolicy );
        operator.init( initCtx );

        IntStream.of( inputPorts ).forEach( portIndex -> {
            final Tuple tuple = Tuple.of( "count", portIndex );
            input.add( portIndex, tuple );
        } );

        operator.invoke( invocationCtx );

        final Tuple outputTuple = output.getTupleOrFail( 0, 0 );
        assertThat( outputTuple.getInteger( "count" ), equalTo( expectedValue ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailForDifferentNumberOfTuplesPerPort ()
    {
        config.set( MERGE_POLICY_CONfIG_PARAMETER, KEEP_EXISTING_VALUE );
        operator.init( initCtx );

        IntStream.of( inputPorts ).forEach( portIndex -> {
            final Tuple tuple = Tuple.of( "count", portIndex );
            input.add( portIndex, tuple );
        } );
        final Tuple tuple = Tuple.of( "count", -1 );
        input.add( tuple );

        operator.invoke( invocationCtx );
    }

    @Test
    public void shouldMergeMultipleTuplesPerPort ()
    {
        config.set( MERGE_POLICY_CONfIG_PARAMETER, KEEP_EXISTING_VALUE );
        operator.init( initCtx );

        populateTuplesWithUniqueFields( input );
        populateTuplesWithUniqueFields( input );

        operator.invoke( invocationCtx );

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
            final Tuple tuple = Tuple.of( "field" + portIndex, portIndex );
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
