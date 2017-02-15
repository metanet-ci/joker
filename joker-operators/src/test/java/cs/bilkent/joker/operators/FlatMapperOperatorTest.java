package cs.bilkent.joker.operators;

import java.util.List;
import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Test;
import com.google.common.base.Supplier;

import static cs.bilkent.joker.operator.InvocationContext.InvocationReason.SUCCESS;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.InitializationContextImpl;
import cs.bilkent.joker.operator.impl.InvocationContextImpl;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchema;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchemaBuilder;
import static cs.bilkent.joker.operators.FlatMapperOperator.FLAT_MAPPER_CONFIG_PARAMETER;
import cs.bilkent.joker.operators.FlatMapperOperator.FlatMapperConsumer;
import cs.bilkent.joker.test.AbstractJokerTest;
import static org.junit.Assert.assertEquals;

public class FlatMapperOperatorTest extends AbstractJokerTest
{

    private final FlatMapperOperator flatMapperOperator = new FlatMapperOperator();

    private final OperatorConfig operatorConfig = new OperatorConfig();

    private final InitializationContextImpl initContext = new InitializationContextImpl();

    private final OperatorRuntimeSchema runtimeSchema = new OperatorRuntimeSchemaBuilder( 1, 1 ).build();

    private final TuplesImpl input = new TuplesImpl( 1 );

    private final TuplesImpl output = new TuplesImpl( 1 );

    private final InvocationContextImpl invocationContext = new InvocationContextImpl( SUCCESS, input, output );

    @Before
    public void init ()
    {
        initContext.setRuntimeSchema( runtimeSchema );
        initContext.setConfig( operatorConfig );

        final FlatMapperOperator.FlatMapperConsumer flatMapperFunc = new FlatMapperConsumer()
        {
            @Override
            public void accept ( final Tuple input, final Supplier<Tuple> outputTupleSupplier, final Consumer<Tuple> outputCollector )
            {
                final int val = input.getInteger( "val" );

                final Tuple output1 = outputTupleSupplier.get();
                output1.set( "val", val + 1 );
                final Tuple output2 = outputTupleSupplier.get();
                output2.set( "val", val + 2 );

                outputCollector.accept( output1 );
                outputCollector.accept( output2 );
            }
        };

        operatorConfig.set( FLAT_MAPPER_CONFIG_PARAMETER, flatMapperFunc );

        flatMapperOperator.init( initContext );
    }

    @Test
    public void shouldFlatMapValues ()
    {
        final Tuple tuple = new Tuple();
        final int value = 5;
        tuple.set( "val", value );
        input.add( tuple );

        flatMapperOperator.invoke( invocationContext );

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
