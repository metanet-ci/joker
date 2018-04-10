package cs.bilkent.joker.operators;

import java.io.PrintStream;
import java.util.function.Function;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static cs.bilkent.joker.operator.InvocationCtx.InvocationReason.SUCCESS;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.DefaultInvocationCtx;
import cs.bilkent.joker.operator.impl.InitCtxImpl;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import static cs.bilkent.joker.operators.ConsoleAppenderOperator.TO_STRING_FUNCTION_CONFIG_PARAMETER;
import cs.bilkent.joker.test.AbstractJokerTest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.verify;


@RunWith( MockitoJUnitRunner.class )
public class ConsoleAppenderOperatorTest extends AbstractJokerTest
{

    @Mock
    private PrintStream sysOut;

    private PrintStream orgSysOut;

    private final OperatorConfig config = new OperatorConfig();

    private ConsoleAppenderOperator operator;

    private InitCtxImpl initCtx;

    @Before
    public void init () throws InstantiationException, IllegalAccessException
    {
        orgSysOut = System.out;
        System.setOut( sysOut );

        final OperatorDef operatorDef = OperatorDefBuilder.newInstance( "appender", ConsoleAppenderOperator.class )
                                                          .setConfig( config )
                                                          .build();
        operator = (ConsoleAppenderOperator) operatorDef.createOperator();
        initCtx = new InitCtxImpl( operatorDef, new boolean[] { true } );
    }

    @After
    public void after ()
    {
        System.setOut( orgSysOut );
    }

    @Test
    public void shouldPrintTuplesToConsoleWithTupleToString ()
    {
        operator.init( initCtx );
        final TuplesImpl output = new TuplesImpl( 1 );

        final DefaultInvocationCtx invocationCtx = new DefaultInvocationCtx( initCtx.getInputPortCount(), key -> null, output );
        final Tuple tuple1 = Tuple.of( "k1", "v1" );
        final Tuple tuple2 = Tuple.of( "k2", "v2" );
        final TuplesImpl input = invocationCtx.createInputTuples( null );
        input.add( tuple1, tuple2 );

        invocationCtx.setInvocationReason( SUCCESS );
        operator.invoke( invocationCtx );

        assertThat( output, equalTo( input ) );
        verify( sysOut ).println( tuple1.toString() );
        verify( sysOut ).println( tuple2.toString() );
    }

    @Test
    public void shouldPrintTuplesToConsoleWithToStringFunction ()
    {
        final Function<Tuple, String> toStringFunc = ( tuple ) -> tuple.toString().toUpperCase();
        config.set( TO_STRING_FUNCTION_CONFIG_PARAMETER, toStringFunc );
        operator.init( initCtx );
        final TuplesImpl output = new TuplesImpl( 1 );

        final DefaultInvocationCtx invocationCtx = new DefaultInvocationCtx( initCtx.getInputPortCount(), key -> null, output );
        final Tuple tuple1 = Tuple.of( "k1", "v1" );
        final Tuple tuple2 = Tuple.of( "k2", "v2" );
        final TuplesImpl input = invocationCtx.createInputTuples( null );
        input.add( tuple1, tuple2 );

        invocationCtx.setInvocationReason( SUCCESS );
        operator.invoke( invocationCtx );

        assertThat( output, equalTo( input ) );
        verify( sysOut ).println( toStringFunc.apply( tuple1 ) );
        verify( sysOut ).println( toStringFunc.apply( tuple2 ) );
    }

}
