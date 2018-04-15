package cs.bilkent.joker.engine.pipeline.impl.invocation;

import org.junit.Test;

import cs.bilkent.joker.engine.partition.impl.PartitionKeyExtractor1;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.DefaultInvocationCtx;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertTrue;

public class FusedInvocationCtxChainTest extends AbstractJokerTest
{

    @Test
    public void testNonPartitionedInvocationContextFusion ()
    {
        final TuplesImpl output = new TuplesImpl( 1 );
        final FusedInvocationCtx last = new FusedInvocationCtx( 1, key -> null, new DefaultOutputCollector( output ) );
        final DefaultInvocationCtx first = new DefaultInvocationCtx( 1, key -> null, last );

        final Tuple input1 = Tuple.of( "key", "val1" );
        final Tuple input2 = Tuple.of( "key", "val1" );

        first.createInputTuples( null ).add( input1 );
        first.createInputTuples( null ).add( input2 );

        first.getInputTuples( 0 ).forEach( first::output );
        first.nextInput();
        first.getInputTuples( 0 ).forEach( first::output );

        last.getInputTuples( 0 ).forEach( last::output );

        assertThat( output.getTuples( 0 ), equalTo( asList( input1, input2 ) ) );
    }

    @Test
    public void testPartitionedInvocationContextFusionWithSingleInput ()
    {
        final PartitionKeyExtractor1 partitionKeyExtractor = new PartitionKeyExtractor1( singletonList( "key" ) );
        final TuplesImpl output = new TuplesImpl( 1 );
        final FusedPartitionedInvocationCtx last = new FusedPartitionedInvocationCtx( 1,
                                                                                      key -> null,
                                                                                      partitionKeyExtractor,
                                                                                      new DefaultOutputCollector( output ) );
        final DefaultInvocationCtx first = new DefaultInvocationCtx( 1, key -> null, last );
        final Tuple input1 = Tuple.of( "key", "val1", "f", "f1" );
        final Tuple input2 = Tuple.of( "key", "val1", "f", "f2" );

        first.createInputTuples( null ).add( input1, input2 );

        first.getInputTuples( 0 ).forEach( first::output );

        assertThat( last.getInputCount(), equalTo( 1 ) );
        assertThat( last.getInputTuples( 0 ), equalTo( asList( input1, input2 ) ) );
    }

    @Test
    public void testPartitionedInvocationContextFusionWithMultipleInputs ()
    {
        final PartitionKeyExtractor1 partitionKeyExtractor = new PartitionKeyExtractor1( singletonList( "key" ) );
        final TuplesImpl output = new TuplesImpl( 1 );
        final FusedPartitionedInvocationCtx last = new FusedPartitionedInvocationCtx( 1,
                                                                                      key -> null,
                                                                                      partitionKeyExtractor,
                                                                                      new DefaultOutputCollector( output ) );
        final DefaultInvocationCtx first = new DefaultInvocationCtx( 1, key -> null, last );
        final Tuple input1 = Tuple.of( "key", "val1" );
        final Tuple input2 = Tuple.of( "key", "val2" );

        first.createInputTuples( null ).add( input1, input2 );

        first.getInputTuples( 0 ).forEach( first::output );

        assertThat( last.getInputCount(), equalTo( 2 ) );
        assertThat( last.getInputTuples( 0 ), equalTo( singletonList( input1 ) ) );
        assertTrue( last.nextInput() );
        assertThat( last.getInputTuples( 0 ), equalTo( singletonList( input2 ) ) );
    }

    @Test
    public void testChain ()
    {
        final TuplesImpl output = new TuplesImpl( 1 );
        final FusedInvocationCtx last = new FusedInvocationCtx( 1, key -> null, new DefaultOutputCollector( output ) );
        final FusedInvocationCtx middle = new FusedInvocationCtx( 1, key -> null, last );
        final DefaultInvocationCtx first = new DefaultInvocationCtx( 1, key -> null, middle );

        final Tuple input = Tuple.of( "key", "val" );
        first.createInputTuples( null ).add( input );

        first.getInputTuples( 0 ).forEach( first::output );

        middle.getInputTuples( 0 ).forEach( middle::output );

        last.getInputTuples( 0 ).forEach( last::output );

        assertThat( output.getTupleCount( 0 ), equalTo( 1 ) );

    }

}
