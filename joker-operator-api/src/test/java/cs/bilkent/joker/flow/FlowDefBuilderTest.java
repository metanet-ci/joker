package cs.bilkent.joker.flow;


import java.util.Collection;

import org.junit.Test;

import cs.bilkent.joker.operator.InitializationContext;
import cs.bilkent.joker.operator.InvocationContext;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchemaBuilder;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;
import cs.bilkent.joker.testutils.AbstractJokerTest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;


public class FlowDefBuilderTest extends AbstractJokerTest
{

    private static final int SPEC_INPUT_PORT_COUNT = 4;

    private static final int SPEC_OUTPUT_PORT_COUNT = 5;

    private static final int INVALID_PORT_COUNT = -2;

    private final FlowDefBuilder builder = new FlowDefBuilder();


    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddOperatorMultipleTimesWithSameOperatorId ()
    {
        builder.add( OperatorDefBuilder.newInstance( "op1", StatefulOperatorWithFixedPortCounts.class ) );
        builder.add( OperatorDefBuilder.newInstance( "op1", StatefulOperatorWithFixedPortCounts.class ) );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotAddOperatorAfterBuilt ()
    {
        builder.add( OperatorDefBuilder.newInstance( "op1", StatefulOperatorWithFixedPortCounts.class ) );
        builder.build();
        builder.add( OperatorDefBuilder.newInstance( "op2", StatelessOperatorWithDynamicPortCounts.class ) );
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
        builder.add( OperatorDefBuilder.newInstance( "op1", StatelessOperatorWithDynamicPortCounts.class )
                                       .setInputPortCount( 1 )
                                       .setOutputPortCount( 1 ) );
        builder.add( OperatorDefBuilder.newInstance( "op2", StatelessOperatorWithDynamicPortCounts.class )
                                       .setInputPortCount( 1 )
                                       .setOutputPortCount( 1 ) );
        builder.connect( "op1", 1, "op2" );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddConnectionToNonExistingTargetOperator ()
    {
        builder.add( OperatorDefBuilder.newInstance( "op1", StatelessOperatorWithDynamicPortCounts.class )
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
        builder.add( OperatorDefBuilder.newInstance( "op1", StatelessOperatorWithDynamicPortCounts.class )
                                       .setInputPortCount( 1 )
                                       .setOutputPortCount( 1 ) );
        builder.add( OperatorDefBuilder.newInstance( "op2", StatelessOperatorWithDynamicPortCounts.class )
                                       .setInputPortCount( 1 )
                                       .setOutputPortCount( 1 ) );
        builder.connect( "op1", "op2", 1 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddConnectionToItself ()
    {
        builder.add( OperatorDefBuilder.newInstance( "op1", StatelessOperatorWithDynamicPortCounts.class )
                                       .setInputPortCount( 1 )
                                       .setOutputPortCount( 1 ) );
        builder.connect( "op1", "op1" );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotAddConnectionAfterBuilt ()
    {
        builder.add( OperatorDefBuilder.newInstance( "op1", StatelessOperatorWithDynamicPortCounts.class ).setInputPortCount( 1 ) );
        builder.add( OperatorDefBuilder.newInstance( "op2", StatelessOperatorWithDynamicPortCounts.class ).setInputPortCount( 1 ) );
        builder.connect( "op1", "op2" );
        builder.build();
        builder.connect( "op1", "op2" );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotBuildWhenThereAreNotConnectedOperators ()
    {
        builder.add( OperatorDefBuilder.newInstance( "op1", StatelessOperatorWithDynamicPortCounts.class ).setInputPortCount( 1 ) );
        builder.add( OperatorDefBuilder.newInstance( "op2", StatelessOperatorWithDynamicPortCounts.class ).setInputPortCount( 1 ) );
        builder.add( OperatorDefBuilder.newInstance( "op3", StatelessOperatorWithDynamicPortCounts.class ).setInputPortCount( 1 ) );
        builder.connect( "op1", "op2" );
        builder.build();
    }

    @Test
    public void shouldAddConnection ()
    {
        builder.add( OperatorDefBuilder.newInstance( "op1", StatelessOperatorWithDynamicPortCounts.class )
                                       .setInputPortCount( 1 )
                                       .setOutputPortCount( 1 ) );
        builder.add( OperatorDefBuilder.newInstance( "op2", StatelessOperatorWithDynamicPortCounts.class )
                                       .setInputPortCount( 1 )
                                       .setOutputPortCount( 1 ) );
        builder.connect( "op1", "op2" );
    }

    @Test
    public void shouldConnectSingleOutputPortMultipleTimes ()
    {
        builder.add( OperatorDefBuilder.newInstance( "op1", StatelessOperatorWithDynamicPortCounts.class )
                                       .setInputPortCount( 1 )
                                       .setOutputPortCount( 1 ) );
        builder.add( OperatorDefBuilder.newInstance( "op2", StatelessOperatorWithDynamicPortCounts.class )
                                       .setInputPortCount( 1 )
                                       .setOutputPortCount( 1 ) );
        builder.add( OperatorDefBuilder.newInstance( "op3", StatelessOperatorWithDynamicPortCounts.class )
                                       .setInputPortCount( 1 )
                                       .setOutputPortCount( 1 ) );
        builder.connect( "op1", "op2" );
        builder.connect( "op1", "op3" );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddCyclicConnection ()
    {
        builder.add( OperatorDefBuilder.newInstance( "op1", StatelessOperatorWithDynamicPortCounts.class )
                                       .setInputPortCount( 1 )
                                       .setOutputPortCount( 1 ) );
        builder.add( OperatorDefBuilder.newInstance( "op2", StatelessOperatorWithDynamicPortCounts.class )
                                       .setInputPortCount( 1 )
                                       .setOutputPortCount( 1 ) );
        try
        {
            builder.connect( "op1", "op2" );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            fail();
        }

        builder.connect( "op2", "op1" );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddCyclicConnection2 ()
    {
        builder.add( OperatorDefBuilder.newInstance( "op1", StatelessOperatorWithDynamicPortCounts.class )
                                       .setInputPortCount( 1 )
                                       .setOutputPortCount( 1 ) );
        builder.add( OperatorDefBuilder.newInstance( "op2", StatelessOperatorWithDynamicPortCounts.class )
                                       .setInputPortCount( 1 )
                                       .setOutputPortCount( 1 ) );
        builder.add( OperatorDefBuilder.newInstance( "op3", StatelessOperatorWithDynamicPortCounts.class )
                                       .setInputPortCount( 1 )
                                       .setOutputPortCount( 1 ) );
        try
        {
            builder.connect( "op1", "op2" );
            builder.connect( "op2", "op3" );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            fail();
        }

        builder.connect( "op3", "op1" );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddCyclicConnection3 ()
    {
        builder.add( OperatorDefBuilder.newInstance( "op1", StatelessOperatorWithDynamicPortCounts.class )
                                       .setInputPortCount( 1 )
                                       .setOutputPortCount( 1 ) );
        builder.add( OperatorDefBuilder.newInstance( "op2", StatelessOperatorWithDynamicPortCounts.class )
                                       .setInputPortCount( 1 )
                                       .setOutputPortCount( 1 ) );
        builder.add( OperatorDefBuilder.newInstance( "op3", StatelessOperatorWithDynamicPortCounts.class )
                                       .setInputPortCount( 1 )
                                       .setOutputPortCount( 1 ) );
        builder.add( OperatorDefBuilder.newInstance( "op4", StatelessOperatorWithDynamicPortCounts.class )
                                       .setInputPortCount( 1 )
                                       .setOutputPortCount( 1 ) );
        try
        {
            builder.connect( "op1", "op2" );
            builder.connect( "op1", "op3" );
            builder.connect( "op2", "op4" );
            builder.connect( "op3", "op4" );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            fail();
        }

        builder.connect( "op4", "op1" );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddCyclicConnection4 ()
    {
        builder.add( OperatorDefBuilder.newInstance( "op1", StatelessOperatorWithDynamicPortCounts.class )
                                       .setInputPortCount( 1 )
                                       .setOutputPortCount( 1 ) );
        builder.add( OperatorDefBuilder.newInstance( "op2", StatelessOperatorWithDynamicPortCounts.class )
                                       .setInputPortCount( 1 )
                                       .setOutputPortCount( 1 ) );
        builder.add( OperatorDefBuilder.newInstance( "op3", StatelessOperatorWithDynamicPortCounts.class )
                                       .setInputPortCount( 1 )
                                       .setOutputPortCount( 1 ) );
        builder.add( OperatorDefBuilder.newInstance( "op4", StatelessOperatorWithDynamicPortCounts.class )
                                       .setInputPortCount( 1 )
                                       .setOutputPortCount( 1 ) );
        try
        {
            builder.connect( "op1", "op2" );
            builder.connect( "op1", "op3" );
            builder.connect( "op3", "op4" );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            fail();
        }

        builder.connect( "op4", "op1" );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotBuildEmptyFlow ()
    {
        assertNotNull( builder.build() );
    }

    @Test
    public void shouldBuildFlow ()
    {
        builder.add( OperatorDefBuilder.newInstance( "op1", StatefulOperatorWithFixedPortCounts.class ) );
        builder.add( OperatorDefBuilder.newInstance( "op2", StatefulOperatorWithFixedPortCounts.class ) );
        builder.add( OperatorDefBuilder.newInstance( "op3", StatefulOperatorWithDynamicPortCounts.class )
                                       .setInputPortCount( 0 )
                                       .setOutputPortCount( 1 ) );
        builder.add( OperatorDefBuilder.newInstance( "op4", StatefulOperatorWithDynamicPortCounts.class )
                                       .setInputPortCount( 2 )
                                       .setOutputPortCount( 0 ) );

        builder.connect( "op1", "op2" );
        builder.connect( "op2", "op4", 0 );
        builder.connect( "op3", "op4", 1 );
        final FlowDef flowDef = builder.build();

        assertNotNull( flowDef.getOperator( "op1" ) );
        assertNotNull( flowDef.getOperator( "op2" ) );
        assertNotNull( flowDef.getOperator( "op3" ) );
        assertNotNull( flowDef.getOperator( "op4" ) );

        final Collection<Port> op1Connections = flowDef.getDownstreamConnections( new Port( "op1", 0 ) );
        assertThat( op1Connections, hasSize( 1 ) );
        assertThat( op1Connections.iterator().next(), equalTo( new Port( "op2", 0 ) ) );

        final Collection<Port> op2Connections = flowDef.getDownstreamConnections( new Port( "op2", 0 ) );
        assertThat( op2Connections, hasSize( 1 ) );
        assertThat( op2Connections.iterator().next(), equalTo( new Port( "op4", 0 ) ) );

        final Collection<Port> op3Connections = flowDef.getDownstreamConnections( new Port( "op3", 0 ) );
        assertThat( op3Connections, hasSize( 1 ) );
        assertThat( op3Connections.iterator().next(), equalTo( new Port( "op4", 1 ) ) );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotConnectPortsWithMismatchingSchemas ()
    {
        final OperatorRuntimeSchemaBuilder targetSchemaBuilder = new OperatorRuntimeSchemaBuilder( 1, 1 );
        targetSchemaBuilder.addInputField( 0, "field1", Integer.class );

        builder.add( OperatorDefBuilder.newInstance( "op1", StatefulOperatorWithFixedPortCounts.class ) );
        builder.add( OperatorDefBuilder.newInstance( "op2", StatefulOperatorWithFixedPortCounts.class )
                                       .setExtendingSchema( targetSchemaBuilder.build() ) );

        builder.connect( "op1", "op2" );
    }

    @Test
    public void shouldConnectCompatibleSourceSchemaToTargetSchema ()
    {
        final OperatorRuntimeSchemaBuilder sourceSchemaBuilder = new OperatorRuntimeSchemaBuilder( 1, 1 );
        final OperatorRuntimeSchemaBuilder targetSchemaBuilder = new OperatorRuntimeSchemaBuilder( 1, 1 );
        sourceSchemaBuilder.addOutputField( 0, "field1", Integer.class );
        targetSchemaBuilder.addInputField( 0, "field1", Number.class );

        builder.add( OperatorDefBuilder.newInstance( "op1", StatefulOperatorWithFixedPortCounts.class )
                                       .setExtendingSchema( sourceSchemaBuilder.build() ) );
        builder.add( OperatorDefBuilder.newInstance( "op2", StatefulOperatorWithFixedPortCounts.class )
                                       .setExtendingSchema( targetSchemaBuilder.build() ) );

        builder.connect( "op1", "op2" );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotConnectIncompatibleSourceSchemaToTargetSchema ()
    {
        final OperatorRuntimeSchemaBuilder sourceSchemaBuilder = new OperatorRuntimeSchemaBuilder( 1, 1 );
        final OperatorRuntimeSchemaBuilder targetSchemaBuilder = new OperatorRuntimeSchemaBuilder( 1, 1 );
        sourceSchemaBuilder.addOutputField( 0, "field1", Number.class );
        targetSchemaBuilder.addInputField( 0, "field1", Integer.class );

        builder.add( OperatorDefBuilder.newInstance( "op1", StatefulOperatorWithFixedPortCounts.class )
                                       .setExtendingSchema( sourceSchemaBuilder.build() ) );
        builder.add( OperatorDefBuilder.newInstance( "op2", StatefulOperatorWithFixedPortCounts.class )
                                       .setExtendingSchema( targetSchemaBuilder.build() ) );

        builder.connect( "op1", "op2" );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotBuildWithMultipleSCCs ()
    {
        builder.add( OperatorDefBuilder.newInstance( "op1", StatelessOperatorWithDynamicPortCounts.class )
                                       .setInputPortCount( 1 )
                                       .setOutputPortCount( 1 ) );
        builder.add( OperatorDefBuilder.newInstance( "op2", StatelessOperatorWithDynamicPortCounts.class )
                                       .setInputPortCount( 1 )
                                       .setOutputPortCount( 1 ) );
        builder.add( OperatorDefBuilder.newInstance( "op3", StatelessOperatorWithDynamicPortCounts.class )
                                       .setInputPortCount( 1 )
                                       .setOutputPortCount( 1 ) );
        builder.add( OperatorDefBuilder.newInstance( "op4", StatelessOperatorWithDynamicPortCounts.class )
                                       .setInputPortCount( 1 )
                                       .setOutputPortCount( 1 ) );
        try
        {
            builder.connect( "op1", "op2" );
            builder.connect( "op2", "op4" );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            fail();
        }

        builder.build();
    }

    public static class OperatorWithNoSpec extends NopOperator
    {

    }


    @OperatorSpec( type = STATELESS )
    public static class StatelessOperatorWithDynamicPortCounts extends NopOperator
    {

    }


    @OperatorSpec( type = STATEFUL, inputPortCount = SPEC_INPUT_PORT_COUNT, outputPortCount = SPEC_OUTPUT_PORT_COUNT )
    public static class StatefulOperatorWithFixedPortCounts extends NopOperator
    {

    }


    @OperatorSpec( type = STATEFUL )
    public static class StatefulOperatorWithDynamicPortCounts extends NopOperator
    {

    }


    @OperatorSpec( type = STATEFUL, inputPortCount = INVALID_PORT_COUNT, outputPortCount = SPEC_OUTPUT_PORT_COUNT )
    public static class StatefulOperatorWithInvalidInputPortCount extends NopOperator
    {

    }


    @OperatorSpec( type = STATEFUL, inputPortCount = SPEC_INPUT_PORT_COUNT, outputPortCount = INVALID_PORT_COUNT )
    public static class StatefulOperatorWithInvalidOutputPortCount extends NopOperator
    {

    }


    public static class NopOperator implements Operator
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
