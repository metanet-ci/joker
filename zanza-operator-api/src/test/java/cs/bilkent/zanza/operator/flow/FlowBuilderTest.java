package cs.bilkent.zanza.operator.flow;


import org.junit.Before;
import org.junit.Test;

import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.InvocationResult;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.OperatorConfig;
import cs.bilkent.zanza.operator.OperatorSpec;
import cs.bilkent.zanza.operator.OperatorType;
import cs.bilkent.zanza.operator.SchedulingStrategy;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;


public class FlowBuilderTest
{

    private static final int CONFIG_INPUT_PORT_COUNT = 2;

    private static final int CONFIG_OUTPUT_PORT_COUNT = 3;

    private static final int SPEC_INPUT_PORT_COUNT = 4;

    private static final int SPEC_OUTPUT_PORT_COUNT = 5;

    private static final int INVALID_PORT_COUNT = -2;

    private final FlowBuilder builder = new FlowBuilder();

    private final OperatorConfig emptyConfig = new OperatorConfig();

    private final OperatorConfig correctConfig = new OperatorConfig();

    private final OperatorConfig invalidConfigWithNegativeInputPortCount = new OperatorConfig();

    private final OperatorConfig invalidConfigWithNegativeOutputPortCount = new OperatorConfig();

    @Before
    public void init ()
    {
        correctConfig.setInputPortCount( CONFIG_INPUT_PORT_COUNT );
        correctConfig.setOutputPortCount( CONFIG_OUTPUT_PORT_COUNT );
        invalidConfigWithNegativeInputPortCount.setInputPortCount( INVALID_PORT_COUNT );
        invalidConfigWithNegativeOutputPortCount.setOutputPortCount( INVALID_PORT_COUNT );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddOperatorWithNullId ()
    {
        builder.add( null, OperatorWithDynamicPortCounts.class );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddOperatorWithEmptyId ()
    {
        builder.add( "", OperatorWithDynamicPortCounts.class );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddOperatorWithNullClass ()
    {
        builder.add( "op1", null );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddOperatorWithoutSpec ()
    {
        builder.add( "op1", OperatorWithNoSpec.class );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddOperatorMultipleTimesWithSameOperatorId ()
    {
        builder.add( "op1", OperatorWithDynamicPortCounts.class );
        builder.add( "op1", OperatorWithFixedPortCounts.class );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotAddOperatorAfterBuilt ()
    {
        builder.build();
        builder.add( "op", OperatorWithDynamicPortCounts.class );
    }

    @Test
    public void shouldAddOperatorsWithDynamicPortCounts ()
    {
        builder.add( "op1", OperatorWithDynamicPortCounts.class );
        builder.add( "op2", OperatorWithDynamicPortCounts.class, emptyConfig );
        builder.add( "op3", OperatorWithDynamicPortCounts.class, correctConfig );

        assertThat( builder.getOperator( "op1" ).config.getInputPortCount(), equalTo( Port.DEFAULT_PORT_COUNT ) );
        assertThat( builder.getOperator( "op1" ).config.getOutputPortCount(), equalTo( Port.DEFAULT_PORT_COUNT ) );

        assertThat( builder.getOperator( "op2" ).config.getInputPortCount(), equalTo( Port.DEFAULT_PORT_COUNT ) );
        assertThat( builder.getOperator( "op2" ).config.getOutputPortCount(), equalTo( Port.DEFAULT_PORT_COUNT ) );

        assertThat( builder.getOperator( "op3" ).config.getInputPortCount(), equalTo( CONFIG_INPUT_PORT_COUNT ) );
        assertThat( builder.getOperator( "op3" ).config.getOutputPortCount(), equalTo( CONFIG_OUTPUT_PORT_COUNT ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddOperatorWithDynamicInvalidInputPortCount ()
    {
        builder.add( "op1", OperatorWithDynamicPortCounts.class, invalidConfigWithNegativeInputPortCount );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddOperatorWithDynamicInvalidOutputPortCount ()
    {
        builder.add( "op1", OperatorWithDynamicPortCounts.class, invalidConfigWithNegativeOutputPortCount );
    }

    @Test
    public void shouldAddOperatorsWithFixedPortCount ()
    {
        builder.add( "op1", OperatorWithFixedPortCounts.class );
        builder.add( "op2", OperatorWithFixedPortCounts.class, emptyConfig );
        builder.add( "op3", OperatorWithFixedPortCounts.class, correctConfig );
        builder.add( "op4", OperatorWithFixedPortCounts.class, invalidConfigWithNegativeInputPortCount );
        builder.add( "op5", OperatorWithFixedPortCounts.class, invalidConfigWithNegativeOutputPortCount );

        for ( String operatorId : asList( "op1", "op2", "op3", "op4", "op5" ) )
        {
            assertThat( builder.getOperator( operatorId ).config.getInputPortCount(), equalTo( SPEC_INPUT_PORT_COUNT ) );
            assertThat( builder.getOperator( operatorId ).config.getOutputPortCount(), equalTo( SPEC_OUTPUT_PORT_COUNT ) );
        }
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddOperatorWithInvalidFixedInputCountAndNullConfig ()
    {
        builder.add( "op1", OperatorWithInvalidInputPortCount.class );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddOperatorWithInvalidFixedInputCountAndEmptyConfig ()
    {
        builder.add( "op1", OperatorWithInvalidInputPortCount.class, emptyConfig );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddOperatorWithInvalidFixedOutputCountAndNullConfig ()
    {
        builder.add( "op1", OperatorWithInvalidOutputPortCount.class );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddOperatorWithInvalidFixedOutputCountAndEmptyConfig ()
    {
        builder.add( "op1", OperatorWithInvalidOutputPortCount.class, emptyConfig );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddConnectionToNonExistingSourceOperator ()
    {
        builder.connect( "op1", "op2" );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddConnectionWithInvalidSourcePort ()
    {
        builder.connect( "op1", -1, "op2" );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddConnectionToNonExistingTargetOperator ()
    {
        builder.add( "op1", OperatorWithDynamicPortCounts.class );
        builder.connect( "op1", "op2" );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddConnectionWithInvalidTargetPort ()
    {
        builder.connect( "op1", "op2", -1 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddConnectionToItself ()
    {
        builder.add( "op1", OperatorWithDynamicPortCounts.class );
        builder.connect( "op1", "op1" );
    }

    @Test
    public void shouldAddConnection ()
    {
        builder.add( "op1", OperatorWithDynamicPortCounts.class );
        builder.add( "op2", OperatorWithDynamicPortCounts.class );
        builder.connect( "op1", "op2" );
    }

    @Test
    public void shouldConnectSingleOutputPortMultipleTimes ()
    {
        builder.add( "op1", OperatorWithDynamicPortCounts.class );
        builder.add( "op2", OperatorWithDynamicPortCounts.class );
        builder.add( "op3", OperatorWithDynamicPortCounts.class );
        builder.connect( "op1", "op2" );
        builder.connect( "op1", "op3" );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotConnectSingleInputPortMultipleTimes ()
    {
        builder.add( "op1", OperatorWithDynamicPortCounts.class );
        builder.add( "op2", OperatorWithDynamicPortCounts.class );
        builder.add( "op3", OperatorWithDynamicPortCounts.class );
        builder.connect( "op1", "op3" );
        builder.connect( "op2", "op3" );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotConnectToItself ()
    {
        builder.add( "op1", OperatorWithDynamicPortCounts.class );
        builder.connect( "op1", "op1" );
    }

    @Test
    public void shouldBuildEmptyFlow ()
    {
        assertNotNull( builder.build() );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotAddConnectionAfterBuilt ()
    {
        builder.add( "op1", OperatorWithDynamicPortCounts.class );
        builder.add( "op2", OperatorWithDynamicPortCounts.class );
        builder.connect( "op1", "op2" );
        builder.build();
        builder.connect( "op1", "op2" );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotBuildWhenThereAreNotConnectedOperators ()
    {
        builder.add( "op1", OperatorWithDynamicPortCounts.class );
        builder.add( "op2", OperatorWithDynamicPortCounts.class );
        builder.add( "op3", OperatorWithDynamicPortCounts.class );
        builder.connect( "op1", "op2" );
        builder.build();
    }


    private static class OperatorWithNoSpec implements Operator
    {

        @Override
        public InvocationResult process ( final InvocationContext invocationContext )
        {
            return null;
        }

        @Override
        public SchedulingStrategy init ( final InitializationContext context )
        {
            return null;
        }
    }


    @OperatorSpec( type = OperatorType.STATELESS )
    private static class OperatorWithDynamicPortCounts implements Operator
    {

        @Override
        public InvocationResult process ( final InvocationContext invocationContext )
        {
            return null;
        }

        @Override
        public SchedulingStrategy init ( final InitializationContext context )
        {
            return null;
        }
    }


    @OperatorSpec( type = OperatorType.PARTITIONED_STATEFUL, inputPortCount = SPEC_INPUT_PORT_COUNT, outputPortCount = SPEC_OUTPUT_PORT_COUNT )
    private static class OperatorWithFixedPortCounts implements Operator
    {

        @Override
        public InvocationResult process ( InvocationContext invocationContext )
        {
            return null;
        }

        @Override
        public SchedulingStrategy init ( final InitializationContext context )
        {
            return null;
        }
    }


    @OperatorSpec( type = OperatorType.STATEFUL, inputPortCount = INVALID_PORT_COUNT, outputPortCount = SPEC_OUTPUT_PORT_COUNT )
    private static class OperatorWithInvalidInputPortCount implements Operator
    {

        @Override
        public InvocationResult process ( InvocationContext invocationContext )
        {
            return null;
        }

        @Override
        public SchedulingStrategy init ( final InitializationContext context )
        {
            return null;
        }
    }


    @OperatorSpec( type = OperatorType.STATEFUL, inputPortCount = SPEC_INPUT_PORT_COUNT, outputPortCount = INVALID_PORT_COUNT )
    private static class OperatorWithInvalidOutputPortCount implements Operator
    {

        @Override
        public InvocationResult process ( InvocationContext invocationContext )
        {
            return null;
        }

        @Override
        public SchedulingStrategy init ( final InitializationContext context )
        {
            return null;
        }
    }
}
