package cs.bilkent.zanza.flow;


import java.util.Collection;

import org.junit.Test;

import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.InvocationResult;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.spec.OperatorSpec;
import cs.bilkent.zanza.operator.spec.OperatorType;
import cs.bilkent.zanza.scheduling.SchedulingStrategy;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class FlowBuilderTest
{

    private static final int SPEC_INPUT_PORT_COUNT = 4;

    private static final int SPEC_OUTPUT_PORT_COUNT = 5;

    private static final int INVALID_PORT_COUNT = -2;

    private final FlowBuilder builder = new FlowBuilder();


    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddOperatorMultipleTimesWithSameOperatorId ()
    {
        builder.add( OperatorDefinitionBuilder.newInstance( "op1", OperatorWithFixedPortCounts.class ) );
        builder.add( OperatorDefinitionBuilder.newInstance( "op1", OperatorWithFixedPortCounts.class ) );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotAddOperatorAfterBuilt ()
    {
        builder.build();
        builder.add( OperatorDefinitionBuilder.newInstance( "op", OperatorWithDynamicPortCounts.class ) );
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
    public void shouldNotAddConnectionWithInvalidSourcePort2 ()
    {
        builder.add( OperatorDefinitionBuilder.newInstance( "op1", OperatorWithDynamicPortCounts.class )
                                              .setInputPortCount( 1 )
                                              .setOutputPortCount( 1 ) );
        builder.add( OperatorDefinitionBuilder.newInstance( "op2", OperatorWithDynamicPortCounts.class )
                                              .setInputPortCount( 1 )
                                              .setOutputPortCount( 1 ) );
        builder.connect( "op1", 1, "op2" );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddConnectionToNonExistingTargetOperator ()
    {
        builder.add( OperatorDefinitionBuilder.newInstance( "op1", OperatorWithDynamicPortCounts.class )
                                              .setInputPortCount( 1 )
                                              .setOutputPortCount( 1 ) );
        builder.connect( "op1", "op2" );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddConnectionWithInvalidTargetPort ()
    {
        builder.connect( "op1", "op2", -1 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddConnectionWithInvalidTargetPort2 ()
    {
        builder.add( OperatorDefinitionBuilder.newInstance( "op1", OperatorWithDynamicPortCounts.class )
                                              .setInputPortCount( 1 )
                                              .setOutputPortCount( 1 ) );
        builder.add( OperatorDefinitionBuilder.newInstance( "op2", OperatorWithDynamicPortCounts.class )
                                              .setInputPortCount( 1 )
                                              .setOutputPortCount( 1 ) );
        builder.connect( "op1", "op2", 1 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddConnectionToItself ()
    {
        builder.add( OperatorDefinitionBuilder.newInstance( "op1", OperatorWithDynamicPortCounts.class )
                                              .setInputPortCount( 1 )
                                              .setOutputPortCount( 1 ) );
        builder.connect( "op1", "op1" );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotAddConnectionAfterBuilt ()
    {
        builder.add( OperatorDefinitionBuilder.newInstance( "op1", OperatorWithDynamicPortCounts.class ).setInputPortCount( 1 ) );
        builder.add( OperatorDefinitionBuilder.newInstance( "op2", OperatorWithDynamicPortCounts.class ).setInputPortCount( 1 ) );
        builder.connect( "op1", "op2" );
        builder.build();
        builder.connect( "op1", "op2" );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotBuildWhenThereAreNotConnectedOperators ()
    {
        builder.add( OperatorDefinitionBuilder.newInstance( "op1", OperatorWithDynamicPortCounts.class ).setInputPortCount( 1 ) );
        builder.add( OperatorDefinitionBuilder.newInstance( "op2", OperatorWithDynamicPortCounts.class ).setInputPortCount( 1 ) );
        builder.add( OperatorDefinitionBuilder.newInstance( "op3", OperatorWithDynamicPortCounts.class ).setInputPortCount( 1 ) );
        builder.connect( "op1", "op2" );
        builder.build();
    }

    @Test
    public void shouldAddConnection ()
    {
        builder.add( OperatorDefinitionBuilder.newInstance( "op1", OperatorWithDynamicPortCounts.class )
                                              .setInputPortCount( 1 )
                                              .setOutputPortCount( 1 ) );
        builder.add( OperatorDefinitionBuilder.newInstance( "op2", OperatorWithDynamicPortCounts.class )
                                              .setInputPortCount( 1 )
                                              .setOutputPortCount( 1 ) );
        builder.connect( "op1", "op2" );
    }

    @Test
    public void shouldConnectSingleOutputPortMultipleTimes ()
    {
        builder.add( OperatorDefinitionBuilder.newInstance( "op1", OperatorWithDynamicPortCounts.class )
                                              .setInputPortCount( 1 )
                                              .setOutputPortCount( 1 ) );
        builder.add( OperatorDefinitionBuilder.newInstance( "op2", OperatorWithDynamicPortCounts.class )
                                              .setInputPortCount( 1 )
                                              .setOutputPortCount( 1 ) );
        builder.add( OperatorDefinitionBuilder.newInstance( "op3", OperatorWithDynamicPortCounts.class )
                                              .setInputPortCount( 1 )
                                              .setOutputPortCount( 1 ) );
        builder.connect( "op1", "op2" );
        builder.connect( "op1", "op3" );
    }

    @Test
    public void shouldBuildEmptyFlow ()
    {
        assertNotNull( builder.build() );
    }

    @Test
    public void shouldBuildFlow ()
    {
        builder.add( OperatorDefinitionBuilder.newInstance( "op1", OperatorWithFixedPortCounts.class ) );
        builder.add( OperatorDefinitionBuilder.newInstance( "op2", OperatorWithFixedPortCounts.class ) );
        builder.add( OperatorDefinitionBuilder.newInstance( "op3", OperatorWithDynamicPortCounts.class )
                                              .setInputPortCount( 0 )
                                              .setOutputPortCount( 1 ) );
        builder.add( OperatorDefinitionBuilder.newInstance( "op4", OperatorWithDynamicPortCounts.class )
                                              .setInputPortCount( 2 )
                                              .setOutputPortCount( 0 ) );

        builder.connect( "op1", "op2" );
        builder.connect( "op2", "op4", 0 );
        builder.connect( "op3", "op4", 1 );
        final FlowDefinition flowDefinition = builder.build();

        assertTrue( flowDefinition.operators.containsKey( "op1" ) );
        assertTrue( flowDefinition.operators.containsKey( "op2" ) );
        assertTrue( flowDefinition.operators.containsKey( "op3" ) );
        assertTrue( flowDefinition.operators.containsKey( "op4" ) );

        final Collection<Port> op1Connections = flowDefinition.connections.get( new Port( "op1", 0 ) );
        assertThat( op1Connections, hasSize( 1 ) );
        assertThat( op1Connections.iterator().next(), equalTo( new Port( "op2", 0 ) ) );

        final Collection<Port> op2Connections = flowDefinition.connections.get( new Port( "op2", 0 ) );
        assertThat( op2Connections, hasSize( 1 ) );
        assertThat( op2Connections.iterator().next(), equalTo( new Port( "op4", 0 ) ) );

        final Collection<Port> op3Connections = flowDefinition.connections.get( new Port( "op3", 0 ) );
        assertThat( op3Connections, hasSize( 1 ) );
        assertThat( op3Connections.iterator().next(), equalTo( new Port( "op4", 1 ) ) );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotConnectPortsWithMismatchingSchemas ()
    {
        final OperatorRuntimeSchemaBuilder targetSchemaBuilder = new OperatorRuntimeSchemaBuilder( 1, 1 );
        targetSchemaBuilder.getInputPortSchemaBuilder( 0 ).addField( "field1", Integer.class );

        builder.add( OperatorDefinitionBuilder.newInstance( "op1", OperatorWithFixedPortCounts.class ) );
        builder.add( OperatorDefinitionBuilder.newInstance( "op2", OperatorWithFixedPortCounts.class )
                                              .setExtendingSchema( targetSchemaBuilder.build() ) );

        builder.connect( "op1", "op2" );
    }

    @Test
    public void shouldConnectCompatibleSourceSchemaToTargetSchema ()
    {
        final OperatorRuntimeSchemaBuilder sourceSchemaBuilder = new OperatorRuntimeSchemaBuilder( 1, 1 );
        final OperatorRuntimeSchemaBuilder targetSchemaBuilder = new OperatorRuntimeSchemaBuilder( 1, 1 );
        sourceSchemaBuilder.getOutputPortSchemaBuilder( 0 ).addField( "field1", Integer.class );
        targetSchemaBuilder.getInputPortSchemaBuilder( 0 ).addField( "field1", Number.class );

        builder.add( OperatorDefinitionBuilder.newInstance( "op1", OperatorWithFixedPortCounts.class )
                                              .setExtendingSchema( sourceSchemaBuilder.build() ) );
        builder.add( OperatorDefinitionBuilder.newInstance( "op2", OperatorWithFixedPortCounts.class )
                                              .setExtendingSchema( targetSchemaBuilder.build() ) );

        builder.connect( "op1", "op2" );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotConnectIncompatibleSourceSchemaToTargetSchema ()
    {
        final OperatorRuntimeSchemaBuilder sourceSchemaBuilder = new OperatorRuntimeSchemaBuilder( 1, 1 );
        final OperatorRuntimeSchemaBuilder targetSchemaBuilder = new OperatorRuntimeSchemaBuilder( 1, 1 );
        sourceSchemaBuilder.getOutputPortSchemaBuilder( 0 ).addField( "field1", Number.class );
        targetSchemaBuilder.getInputPortSchemaBuilder( 0 ).addField( "field1", Integer.class );

        builder.add( OperatorDefinitionBuilder.newInstance( "op1", OperatorWithFixedPortCounts.class )
                                              .setExtendingSchema( sourceSchemaBuilder.build() ) );
        builder.add( OperatorDefinitionBuilder.newInstance( "op2", OperatorWithFixedPortCounts.class )
                                              .setExtendingSchema( targetSchemaBuilder.build() ) );

        builder.connect( "op1", "op2" );
    }


    public static class OperatorWithNoSpec implements Operator
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
    public static class OperatorWithDynamicPortCounts implements Operator
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


    @OperatorSpec( type = OperatorType.STATEFUL, inputPortCount = SPEC_INPUT_PORT_COUNT, outputPortCount = SPEC_OUTPUT_PORT_COUNT )
    public static class OperatorWithFixedPortCounts implements Operator
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
    public static class OperatorWithInvalidInputPortCount implements Operator
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
    public static class OperatorWithInvalidOutputPortCount implements Operator
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
