package cs.bilkent.joker.operator;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.junit.Test;

import cs.bilkent.joker.operator.InvocationCtx.InvocationReason;
import static cs.bilkent.joker.operator.TupleAccessor.getIngestionTime;
import static cs.bilkent.joker.operator.TupleAccessor.setIngestionTime;
import cs.bilkent.joker.operator.impl.DefaultInvocationCtx;
import cs.bilkent.joker.operator.impl.InMemoryKVStore;
import cs.bilkent.joker.operator.impl.InitCtxImpl;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operators.BarrierOperator;
import cs.bilkent.joker.operators.BarrierOperator.TupleValueMergePolicy;
import cs.bilkent.joker.operators.ConsoleAppenderOperator;
import cs.bilkent.joker.operators.ExponentialMovingAverageAggregationOperator;
import cs.bilkent.joker.operators.FilterOperator;
import cs.bilkent.joker.operators.FlatMapperOperator;
import cs.bilkent.joker.operators.ForEachOperator;
import cs.bilkent.joker.operators.MapperOperator;
import cs.bilkent.joker.test.AbstractJokerTest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class OperatorLatencyTest extends AbstractJokerTest
{

    @Test
    public void testMapperOperator () throws InstantiationException, IllegalAccessException
    {
        final BiConsumer<Tuple, Tuple> nop = ( tuple, tuple2 ) -> {
        };
        final OperatorConfig config = new OperatorConfig().set( MapperOperator.MAPPER_CONFIG_PARAMETER, nop );
        final OperatorDef operatorDef = OperatorDefBuilder.newInstance( "op", MapperOperator.class ).setConfig( config ).build();
        final InitCtx initCtx = new InitCtxImpl( operatorDef, new boolean[] { true } );
        final Operator operator = operatorDef.createOperator();
        operator.init( initCtx );

        final TuplesImpl output = new TuplesImpl( operatorDef.getOutputPortCount() );
        final DefaultInvocationCtx invCtx = new DefaultInvocationCtx( operatorDef.getInputPortCount(), key -> null, output );
        final Tuple input = new Tuple();
        final long ingestionTime = 10;
        setIngestionTime( input, ingestionTime, false );
        invCtx.createInputTuples( null ).add( input );
        invCtx.setInvocationReason( InvocationReason.SUCCESS );

        operator.invoke( invCtx );

        assertThat( getIngestionTime( output.getTupleOrFail( 0, 0 ) ), equalTo( ingestionTime ) );
    }

    @Test
    public void testFilterOperator () throws InstantiationException, IllegalAccessException
    {
        final Predicate<Tuple> predicate = tuple -> true;
        final OperatorConfig config = new OperatorConfig().set( FilterOperator.PREDICATE_CONFIG_PARAMETER, predicate );
        final OperatorDef operatorDef = OperatorDefBuilder.newInstance( "op", FilterOperator.class ).setConfig( config ).build();
        final InitCtx initCtx = new InitCtxImpl( operatorDef, new boolean[] { true } );
        final Operator operator = operatorDef.createOperator();
        operator.init( initCtx );

        final TuplesImpl output = new TuplesImpl( operatorDef.getOutputPortCount() );
        final DefaultInvocationCtx invCtx = new DefaultInvocationCtx( operatorDef.getInputPortCount(), key -> null, output );
        final Tuple input = new Tuple();
        final long ingestionTime = 10;
        setIngestionTime( input, ingestionTime, false );
        invCtx.createInputTuples( null ).add( input );
        invCtx.setInvocationReason( InvocationReason.SUCCESS );

        operator.invoke( invCtx );

        assertThat( getIngestionTime( output.getTupleOrFail( 0, 0 ) ), equalTo( ingestionTime ) );
    }

    @Test
    public void testFlatMapperOperator () throws InstantiationException, IllegalAccessException
    {
        final FlatMapperOperator.FlatMapperConsumer func = ( input, outputTupleSupplier, outputCollector ) -> {
            outputCollector.accept( outputTupleSupplier.get() );
            outputCollector.accept( outputTupleSupplier.get() );
        };
        final OperatorConfig config = new OperatorConfig().set( FlatMapperOperator.FLAT_MAPPER_CONFIG_PARAMETER, func );
        final OperatorDef operatorDef = OperatorDefBuilder.newInstance( "op", FlatMapperOperator.class ).setConfig( config ).build();
        final InitCtx initCtx = new InitCtxImpl( operatorDef, new boolean[] { true } );
        final Operator operator = operatorDef.createOperator();
        operator.init( initCtx );

        final TuplesImpl output = new TuplesImpl( operatorDef.getOutputPortCount() );
        final DefaultInvocationCtx invCtx = new DefaultInvocationCtx( operatorDef.getInputPortCount(), key -> null, output );
        final Tuple input = new Tuple();
        final long ingestionTime = 10;
        setIngestionTime( input, ingestionTime, false );
        invCtx.createInputTuples( null ).add( input );
        invCtx.setInvocationReason( InvocationReason.SUCCESS );

        operator.invoke( invCtx );

        assertThat( getIngestionTime( output.getTupleOrFail( 0, 0 ) ), equalTo( ingestionTime ) );
        assertThat( getIngestionTime( output.getTupleOrFail( 0, 1 ) ), equalTo( ingestionTime ) );
    }

    @Test
    public void testForEachOperator () throws InstantiationException, IllegalAccessException
    {
        final Consumer<Tuple> nop = tuple -> {
        };
        final OperatorConfig config = new OperatorConfig().set( ForEachOperator.CONSUMER_FUNCTION_CONFIG_PARAMETER, nop );
        final OperatorDef operatorDef = OperatorDefBuilder.newInstance( "op", ForEachOperator.class ).setConfig( config ).build();
        final InitCtx initCtx = new InitCtxImpl( operatorDef, new boolean[] { true } );
        final Operator operator = operatorDef.createOperator();
        operator.init( initCtx );

        final TuplesImpl output = new TuplesImpl( operatorDef.getOutputPortCount() );
        final DefaultInvocationCtx invCtx = new DefaultInvocationCtx( operatorDef.getInputPortCount(), key -> null, output );
        final Tuple input = new Tuple();
        final long ingestionTime = 10;
        setIngestionTime( input, ingestionTime, false );
        invCtx.createInputTuples( null ).add( input );
        invCtx.setInvocationReason( InvocationReason.SUCCESS );

        operator.invoke( invCtx );

        assertThat( getIngestionTime( output.getTupleOrFail( 0, 0 ) ), equalTo( ingestionTime ) );
    }

    @Test
    public void testConsoleAppenderOperator () throws InstantiationException, IllegalAccessException
    {
        final OperatorDef operatorDef = OperatorDefBuilder.newInstance( "op", ConsoleAppenderOperator.class ).build();
        final InitCtx initCtx = new InitCtxImpl( operatorDef, new boolean[] { true } );
        final Operator operator = operatorDef.createOperator();
        operator.init( initCtx );

        final TuplesImpl output = new TuplesImpl( operatorDef.getOutputPortCount() );
        final DefaultInvocationCtx invCtx = new DefaultInvocationCtx( operatorDef.getInputPortCount(), key -> null, output );
        final Tuple input = new Tuple();
        final long ingestionTime = 10;
        setIngestionTime( input, ingestionTime, false );
        invCtx.createInputTuples( null ).add( input );
        invCtx.setInvocationReason( InvocationReason.SUCCESS );

        operator.invoke( invCtx );

        assertThat( getIngestionTime( output.getTupleOrFail( 0, 0 ) ), equalTo( ingestionTime ) );
    }

    @Test
    public void testBarrierOperator () throws InstantiationException, IllegalAccessException
    {
        final OperatorConfig config = new OperatorConfig().set( BarrierOperator.MERGE_POLICY_CONfIG_PARAMETER,
                                                                TupleValueMergePolicy.KEEP_EXISTING_VALUE );
        final OperatorDef operatorDef = OperatorDefBuilder.newInstance( "op", BarrierOperator.class )
                                                          .setConfig( config )
                                                          .setInputPortCount( 3 )
                                                          .build();
        final InitCtx initCtx = new InitCtxImpl( operatorDef, new boolean[] { true, true, true } );
        final Operator operator = operatorDef.createOperator();
        operator.init( initCtx );

        final TuplesImpl output = new TuplesImpl( operatorDef.getOutputPortCount() );
        final DefaultInvocationCtx invCtx = new DefaultInvocationCtx( operatorDef.getInputPortCount(), key -> null, output );
        final Tuple tuple1 = new Tuple();
        final Tuple tuple2 = new Tuple();
        final Tuple tuple3 = new Tuple();
        final long ingestionTime = 10;
        setIngestionTime( tuple1, ingestionTime, false );
        setIngestionTime( tuple2, ingestionTime - 1, false );
        setIngestionTime( tuple3, ingestionTime + 1, false );
        final TuplesImpl input = invCtx.createInputTuples( null );
        input.add( 0, tuple1 );
        input.add( 1, tuple2 );
        input.add( 2, tuple3 );
        invCtx.setInvocationReason( InvocationReason.SUCCESS );

        operator.invoke( invCtx );

        assertThat( getIngestionTime( output.getTupleOrFail( 0, 0 ) ), equalTo( ingestionTime + 1 ) );
    }

    @Test
    public void testExponentialMovingAverageAggregationOperator () throws InstantiationException, IllegalAccessException
    {

        final OperatorConfig config = new OperatorConfig().set( ExponentialMovingAverageAggregationOperator.WEIGHT_CONFIG_PARAMETER, 0.1d )
                                                          .set( ExponentialMovingAverageAggregationOperator.FIELD_NAME_CONFIG_PARAMETER,
                                                                "val" );
        final OperatorDef operatorDef = OperatorDefBuilder.newInstance( "op", ExponentialMovingAverageAggregationOperator.class )
                                                          .setConfig( config )
                                                          .build();
        final InitCtx initCtx = new InitCtxImpl( operatorDef, new boolean[] { true } );
        final Operator operator = operatorDef.createOperator();
        operator.init( initCtx );

        final TuplesImpl output = new TuplesImpl( operatorDef.getOutputPortCount() );
        final DefaultInvocationCtx invCtx = new DefaultInvocationCtx( operatorDef.getInputPortCount(),
                                                                      key -> new InMemoryKVStore(),
                                                                      output );
        final Tuple tuple1 = Tuple.of( "val", 1 );
        final Tuple tuple2 = Tuple.of( "val", 2 );
        final long ingestionTime = 10;
        setIngestionTime( tuple1, ingestionTime, false );
        setIngestionTime( tuple2, ingestionTime + 1, false );
        final TuplesImpl input = invCtx.createInputTuples( null );
        input.add( tuple1, tuple2 );
        invCtx.setInvocationReason( InvocationReason.SUCCESS );

        operator.invoke( invCtx );

        assertThat( getIngestionTime( output.getTupleOrFail( 0, 0 ) ), equalTo( ingestionTime ) );
        assertThat( getIngestionTime( output.getTupleOrFail( 0, 1 ) ), equalTo( ingestionTime + 1 ) );
    }

}
