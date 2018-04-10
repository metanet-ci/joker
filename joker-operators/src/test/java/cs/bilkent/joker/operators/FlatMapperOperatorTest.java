package cs.bilkent.joker.operators;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import static cs.bilkent.joker.operator.InvocationCtx.InvocationReason.SUCCESS;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.DefaultInvocationCtx;
import cs.bilkent.joker.operator.impl.InitCtxImpl;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import static cs.bilkent.joker.operators.FlatMapperOperator.FLAT_MAPPER_CONFIG_PARAMETER;
import cs.bilkent.joker.test.AbstractJokerTest;
import static org.junit.Assert.assertEquals;

public class FlatMapperOperatorTest extends AbstractJokerTest
{

    private final TuplesImpl output = new TuplesImpl( 1 );

    private final DefaultInvocationCtx invocationCtx = new DefaultInvocationCtx( 1, key -> null, output );

    private final TuplesImpl input = invocationCtx.createInputTuples( null );

    private final OperatorConfig config = new OperatorConfig();

    private FlatMapperOperator operator;

    @Before
    public void init () throws InstantiationException, IllegalAccessException
    {
        invocationCtx.setInvocationReason( SUCCESS );

        final OperatorDef operatorDef = OperatorDefBuilder.newInstance( "flatMapper", FlatMapperOperator.class )
                                                          .setConfig( config )
                                                          .build();

        operator = (FlatMapperOperator) operatorDef.createOperator();

        final FlatMapperOperator.FlatMapperConsumer flatMapperFunc = ( input, outputTupleSupplier, outputCollector ) -> {
            final int val = input.getInteger( "val" );

            final Tuple output1 = outputTupleSupplier.get();
            output1.set( "val", val + 1 );
            final Tuple output2 = outputTupleSupplier.get();
            output2.set( "val", val + 2 );

            outputCollector.accept( output1 );
            outputCollector.accept( output2 );
        };

        config.set( FLAT_MAPPER_CONFIG_PARAMETER, flatMapperFunc );
        final InitCtxImpl initCtx = new InitCtxImpl( operatorDef, new boolean[] { true } );
        operator.init( initCtx );
    }

    @Test
    public void shouldFlatMapValues ()
    {
        final int value = 5;
        final Tuple tuple = Tuple.of( "val", value );
        input.add( tuple );

        operator.invoke( invocationCtx );

        final List<Tuple> outputTuples = output.getTuples( 0 );
        assertEquals( 2, outputTuples.size() );
        final Tuple output1 = outputTuples.get( 0 );
        final Tuple output2 = outputTuples.get( 1 );
        assertEquals( 1, output1.size() );
        assertEquals( value + 1, output1.getIntegerValueOrDefault( "val", 0 ) );
        assertEquals( 1, output2.size() );
        assertEquals( value + 2, output2.getIntegerValueOrDefault( "val", 0 ) );
    }

}
