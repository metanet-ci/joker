package cs.bilkent.zanza.operator.flow;


import org.junit.Test;

import cs.bilkent.zanza.operator.Operator;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class FlowBuilderTest
{

    private final FlowBuilder builder = new FlowBuilder();

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddOperatorWithNullId ()
    {
        builder.add( null, Operator.class, null );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddOperatorWithEmptyId ()
    {
        builder.add( "", Operator.class, null );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddOperatorWithNullClass ()
    {
        builder.add( "op1", null, null );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotAddOperatorAfterBuilt ()
    {
        builder.build();
        builder.add( "op", Operator.class );
    }

    @Test
    public void shouldAddOperators ()
    {
        builder.add( "op1", Operator.class );
        builder.add( "op2", Operator.class );
        builder.add( "op3", Operator.class );
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
        builder.add( "op1", Operator.class );
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
        builder.add( "op1", Operator.class );
        builder.connect( "op1", "op1" );
    }

    @Test
    public void shouldAddConnection ()
    {
        builder.add( "op1", Operator.class );
        builder.add( "op2", Operator.class );
        builder.connect( "op1", "op2" );
    }

    @Test
    public void shouldConnectSingleOutputPortMultipleTimes ()
    {
        builder.add( "op1", Operator.class );
        builder.add( "op2", Operator.class );
        builder.add( "op3", Operator.class );
        builder.connect( "op1", "op2" );
        builder.connect( "op1", "op3" );
        final FlowDefinition flow = builder.build();
        assertNotNull( flow );
        assertTrue( flow.containsOperator( "op1" ) );
        assertTrue( flow.containsOperator( "op2" ) );
        assertTrue( flow.containsOperator( "op3" ) );
        assertTrue( flow.isConnected( "op1", "op2" ) );
        assertTrue( flow.isConnected( "op1", "op3" ) );
        assertFalse( flow.isConnected( "op3", "op1" ) );
        assertFalse( flow.isConnected( "op2", "op1" ) );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldFailWithInvalidFlowDefinition ()
    {
        builder.add( "op1", Operator.class );
        builder.add( "op2", Operator.class );
        builder.add( "op3", Operator.class );
        builder.connect( "op1", "op2" );
        builder.build();
    }

    @Test
    public void shouldBuildEmptyFlow ()
    {
        assertNotNull( builder.build() );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotAddConnectionAfterBuilt ()
    {
        builder.add( "op1", Operator.class );
        builder.add( "op2", Operator.class );
        builder.connect( "op1", "op2" );
        builder.build();
        builder.connect( "op1", "op2" );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotBuildWhenThereAreNotConnectedOperators ()
    {
        builder.add( "op1", Operator.class );
        builder.add( "op2", Operator.class );
        builder.add( "op3", Operator.class );
        builder.connect( "op1", "op2" );
        builder.build();
    }

}
