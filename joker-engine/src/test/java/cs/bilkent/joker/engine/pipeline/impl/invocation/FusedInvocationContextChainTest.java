package cs.bilkent.joker.engine.pipeline.impl.invocation;

import org.junit.Test;

import cs.bilkent.joker.engine.partition.impl.PartitionKeyExtractor1;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.DefaultInvocationContext;
import cs.bilkent.joker.operator.impl.DefaultOutputCollector;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertTrue;

public class FusedInvocationContextChainTest extends AbstractJokerTest
{

    @Test
    public void testNonPartitionedInvocationContextFusion ()
    {
        final TuplesImpl output = new TuplesImpl( 1 );
        final FusedInvocationContext last = new FusedInvocationContext( 1, key -> null, new DefaultOutputCollector( output ) );
        final DefaultInvocationContext first = new DefaultInvocationContext( 1, key -> null, last );

        final Tuple input1 = new Tuple();
        input1.set( "key", "val1" );

        final Tuple input2 = new Tuple();
        input2.set( "key", "val1" );

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
        final FusedPartitionedInvocationContext last = new FusedPartitionedInvocationContext( 1,
                                                                                              key -> null,
                                                                                              partitionKeyExtractor,
                                                                                              new DefaultOutputCollector( output ) );
        final DefaultInvocationContext first = new DefaultInvocationContext( 1, key -> null, last );
        final Tuple input1 = new Tuple();
        input1.set( "key", "val1" );
        input1.set( "f", "f1" );

        final Tuple input2 = new Tuple();
        input2.set( "key", "val1" );
        input2.set( "f", "f2" );

        first.createInputTuples( null ).addAll( asList( input1, input2 ) );

        first.getInputTuples( 0 ).forEach( first::output );

        assertThat( last.getInputCount(), equalTo( 1 ) );
        assertThat( last.getInputTuples( 0 ), equalTo( asList( input1, input2 ) ) );
    }

    @Test
    public void testPartitionedInvocationContextFusionWithMultipleInputs ()
    {
        final PartitionKeyExtractor1 partitionKeyExtractor = new PartitionKeyExtractor1( singletonList( "key" ) );
        final TuplesImpl output = new TuplesImpl( 1 );
        final FusedPartitionedInvocationContext last = new FusedPartitionedInvocationContext( 1,
                                                                                              key -> null,
                                                                                              partitionKeyExtractor,
                                                                                              new DefaultOutputCollector( output ) );
        final DefaultInvocationContext first = new DefaultInvocationContext( 1, key -> null, last );
        final Tuple input1 = new Tuple();
        input1.set( "key", "val1" );

        final Tuple input2 = new Tuple();
        input2.set( "key", "val2" );

        first.createInputTuples( null ).addAll( asList( input1, input2 ) );

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
        final FusedInvocationContext last = new FusedInvocationContext( 1, key -> null, new DefaultOutputCollector( output ) );
        final FusedInvocationContext middle = new FusedInvocationContext( 1, key -> null, last );
        final DefaultInvocationContext first = new DefaultInvocationContext( 1, key -> null, middle );

        final Tuple input = new Tuple();
        input.set( "key", "val" );
        first.createInputTuples( null ).add( input );

        first.getInputTuples( 0 ).forEach( first::output );

        middle.getInputTuples( 0 ).forEach( middle::output );

        last.getInputTuples( 0 ).forEach( last::output );

        assertThat( output.getTupleCount( 0 ), equalTo( 1 ) );

    }

}
