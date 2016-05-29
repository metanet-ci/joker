package cs.bilkent.zanza.flow;


import java.util.Collection;

import org.junit.Test;

import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import cs.bilkent.zanza.operator.spec.OperatorSpec;
import cs.bilkent.zanza.operator.spec.OperatorType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertNotNull;


public class FlowDefinitionBuilderTest
{

    private static final int SPEC_INPUT_PORT_COUNT = 4;

    private static final int SPEC_OUTPUT_PORT_COUNT = 5;

    private static final int INVALID_PORT_COUNT = -2;

    private final FlowDefinitionBuilder builder = new FlowDefinitionBuilder();


    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddOperatorMultipleTimesWithSameOperatorId ()
    {
        builder.add( OperatorDefinitionBuilder.newInstance( "op1", StatefulOperatorWithFixedPortCounts.class ) );
        builder.add( OperatorDefinitionBuilder.newInstance( "op1", StatefulOperatorWithFixedPortCounts.class ) );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotAddOperatorAfterBuilt ()
    {
        builder.build();
        builder.add( OperatorDefinitionBuilder.newInstance( "op", StatelessOperatorWithDynamicPortCounts.class ) );
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
        builder.add( OperatorDefinitionBuilder.newInstance( "op1", StatelessOperatorWithDynamicPortCounts.class )
                                              .setInputPortCount( 1 )
                                              .setOutputPortCount( 1 ) );
        builder.add( OperatorDefinitionBuilder.newInstance( "op2", StatelessOperatorWithDynamicPortCounts.class )
                                              .setInputPortCount( 1 )
                                              .setOutputPortCount( 1 ) );
        builder.connect( "op1", 1, "op2" );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddConnectionToNonExistingTargetOperator ()
    {
        builder.add( OperatorDefinitionBuilder.newInstance( "op1", StatelessOperatorWithDynamicPortCounts.class )
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
        builder.add( OperatorDefinitionBuilder.newInstance( "op1", StatelessOperatorWithDynamicPortCounts.class )
                                              .setInputPortCount( 1 )
                                              .setOutputPortCount( 1 ) );
        builder.add( OperatorDefinitionBuilder.newInstance( "op2", StatelessOperatorWithDynamicPortCounts.class )
                                              .setInputPortCount( 1 )
                                              .setOutputPortCount( 1 ) );
        builder.connect( "op1", "op2", 1 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddConnectionToItself ()
    {
        builder.add( OperatorDefinitionBuilder.newInstance( "op1", StatelessOperatorWithDynamicPortCounts.class )
                                              .setInputPortCount( 1 )
                                              .setOutputPortCount( 1 ) );
        builder.connect( "op1", "op1" );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotAddConnectionAfterBuilt ()
    {
        builder.add( OperatorDefinitionBuilder.newInstance( "op1", StatelessOperatorWithDynamicPortCounts.class ).setInputPortCount( 1 ) );
        builder.add( OperatorDefinitionBuilder.newInstance( "op2", StatelessOperatorWithDynamicPortCounts.class ).setInputPortCount( 1 ) );
        builder.connect( "op1", "op2" );
        builder.build();
        builder.connect( "op1", "op2" );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotBuildWhenThereAreNotConnectedOperators ()
    {
        builder.add( OperatorDefinitionBuilder.newInstance( "op1", StatelessOperatorWithDynamicPortCounts.class ).setInputPortCount( 1 ) );
        builder.add( OperatorDefinitionBuilder.newInstance( "op2", StatelessOperatorWithDynamicPortCounts.class ).setInputPortCount( 1 ) );
        builder.add( OperatorDefinitionBuilder.newInstance( "op3", StatelessOperatorWithDynamicPortCounts.class ).setInputPortCount( 1 ) );
        builder.connect( "op1", "op2" );
        builder.build();
    }

    @Test
    public void shouldAddConnection ()
    {
        builder.add( OperatorDefinitionBuilder.newInstance( "op1", StatelessOperatorWithDynamicPortCounts.class )
                                              .setInputPortCount( 1 )
                                              .setOutputPortCount( 1 ) );
        builder.add( OperatorDefinitionBuilder.newInstance( "op2", StatelessOperatorWithDynamicPortCounts.class )
                                              .setInputPortCount( 1 )
                                              .setOutputPortCount( 1 ) );
        builder.connect( "op1", "op2" );
    }

    @Test
    public void shouldConnectSingleOutputPortMultipleTimes ()
    {
        builder.add( OperatorDefinitionBuilder.newInstance( "op1", StatelessOperatorWithDynamicPortCounts.class )
                                              .setInputPortCount( 1 )
                                              .setOutputPortCount( 1 ) );
        builder.add( OperatorDefinitionBuilder.newInstance( "op2", StatelessOperatorWithDynamicPortCounts.class )
                                              .setInputPortCount( 1 )
                                              .setOutputPortCount( 1 ) );
        builder.add( OperatorDefinitionBuilder.newInstance( "op3", StatelessOperatorWithDynamicPortCounts.class )
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
        builder.add( OperatorDefinitionBuilder.newInstance( "op1", StatefulOperatorWithFixedPortCounts.class ) );
        builder.add( OperatorDefinitionBuilder.newInstance( "op2", StatefulOperatorWithFixedPortCounts.class ) );
        builder.add( OperatorDefinitionBuilder.newInstance( "op3", StatefulOperatorWithDynamicPortCounts.class )
                                              .setInputPortCount( 0 )
                                              .setOutputPortCount( 1 ) );
        builder.add( OperatorDefinitionBuilder.newInstance( "op4", StatefulOperatorWithDynamicPortCounts.class )
                                              .setInputPortCount( 2 )
                                              .setOutputPortCount( 0 ) );

        builder.connect( "op1", "op2" );
        builder.connect( "op2", "op4", 0 );
        builder.connect( "op3", "op4", 1 );
        final FlowDefinition flowDefinition = builder.build();

        assertNotNull( flowDefinition.getOperator( "op1" ) );
        assertNotNull( flowDefinition.getOperator( "op2" ) );
        assertNotNull( flowDefinition.getOperator( "op3" ) );
        assertNotNull( flowDefinition.getOperator( "op4" ) );

        final Collection<Port> op1Connections = flowDefinition.getDownstreamConnections( "op1" );
        assertThat( op1Connections, hasSize( 1 ) );
        assertThat( op1Connections.iterator().next(), equalTo( new Port( "op2", 0 ) ) );

        final Collection<Port> op2Connections = flowDefinition.getDownstreamConnections( "op2" );
        assertThat( op2Connections, hasSize( 1 ) );
        assertThat( op2Connections.iterator().next(), equalTo( new Port( "op4", 0 ) ) );

        final Collection<Port> op3Connections = flowDefinition.getDownstreamConnections( "op3" );
        assertThat( op3Connections, hasSize( 1 ) );
        assertThat( op3Connections.iterator().next(), equalTo( new Port( "op4", 1 ) ) );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotConnectPortsWithMismatchingSchemas ()
    {
        final OperatorRuntimeSchemaBuilder targetSchemaBuilder = new OperatorRuntimeSchemaBuilder( 1, 1 );
        targetSchemaBuilder.getInputPortSchemaBuilder( 0 ).addField( "field1", Integer.class );

        builder.add( OperatorDefinitionBuilder.newInstance( "op1", StatefulOperatorWithFixedPortCounts.class ) );
        builder.add( OperatorDefinitionBuilder.newInstance( "op2", StatefulOperatorWithFixedPortCounts.class )
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

        builder.add( OperatorDefinitionBuilder.newInstance( "op1", StatefulOperatorWithFixedPortCounts.class )
                                              .setExtendingSchema( sourceSchemaBuilder.build() ) );
        builder.add( OperatorDefinitionBuilder.newInstance( "op2", StatefulOperatorWithFixedPortCounts.class )
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

        builder.add( OperatorDefinitionBuilder.newInstance( "op1", StatefulOperatorWithFixedPortCounts.class )
                                              .setExtendingSchema( sourceSchemaBuilder.build() ) );
        builder.add( OperatorDefinitionBuilder.newInstance( "op2", StatefulOperatorWithFixedPortCounts.class )
                                              .setExtendingSchema( targetSchemaBuilder.build() ) );

        builder.connect( "op1", "op2" );
    }


    public static class OperatorWithNoSpec extends NopOperator
    {

    }


    @OperatorSpec( type = OperatorType.STATELESS )
    public static class StatelessOperatorWithDynamicPortCounts extends NopOperator
    {

    }


    @OperatorSpec( type = OperatorType.STATEFUL, inputPortCount = SPEC_INPUT_PORT_COUNT, outputPortCount = SPEC_OUTPUT_PORT_COUNT )
    public static class StatefulOperatorWithFixedPortCounts extends NopOperator
    {

    }


    @OperatorSpec( type = OperatorType.STATEFUL )
    public static class StatefulOperatorWithDynamicPortCounts extends NopOperator
    {

    }


    @OperatorSpec( type = OperatorType.STATEFUL, inputPortCount = INVALID_PORT_COUNT, outputPortCount = SPEC_OUTPUT_PORT_COUNT )
    public static class StatefulOperatorWithInvalidInputPortCount extends NopOperator
    {

    }


    @OperatorSpec( type = OperatorType.STATEFUL, inputPortCount = SPEC_INPUT_PORT_COUNT, outputPortCount = INVALID_PORT_COUNT )
    public static class StatefulOperatorWithInvalidOutputPortCount extends NopOperator
    {

    }


    static class NopOperator implements Operator
    {

        @Override
        public SchedulingStrategy init ( final InitializationContext context )
        {
            return null;
        }

        @Override
        public void invoke ( final InvocationContext invocationContext )
        {

        }
    }

}
