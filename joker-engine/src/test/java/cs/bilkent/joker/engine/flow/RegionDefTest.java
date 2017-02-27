package cs.bilkent.joker.engine.flow;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchema;
import cs.bilkent.joker.operator.schema.runtime.PortRuntimeSchema;
import cs.bilkent.joker.operator.schema.runtime.RuntimeSchemaField;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class RegionDefTest extends AbstractJokerTest
{

    @Mock
    private OperatorDef operatorDef0;

    @Mock
    private OperatorDef operatorDef1;

    @Mock
    private OperatorDef operatorDef2;

    @Mock
    private OperatorDef operatorDef3;

    @Before
    public void init ()
    {
        when( operatorDef0.getId() ).thenReturn( "op0" );
        when( operatorDef1.getId() ).thenReturn( "op1" );
        when( operatorDef2.getId() ).thenReturn( "op2" );
        when( operatorDef3.getId() ).thenReturn( "op3" );
    }

    @Test
    public void shouldCreateStatelessRegionWithStatelessOperators ()
    {
        when( operatorDef0.getOperatorType() ).thenReturn( STATELESS );
        when( operatorDef1.getOperatorType() ).thenReturn( STATELESS );
        when( operatorDef2.getOperatorType() ).thenReturn( STATELESS );
        when( operatorDef3.getOperatorType() ).thenReturn( STATELESS );

        new RegionDef( 0, STATELESS, emptyList(), asList( operatorDef0, operatorDef1, operatorDef2, operatorDef3 ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateStatelessRegionWithStatefulOperators ()
    {
        when( operatorDef0.getOperatorType() ).thenReturn( STATELESS );
        when( operatorDef1.getOperatorType() ).thenReturn( STATELESS );
        when( operatorDef2.getOperatorType() ).thenReturn( STATELESS );
        when( operatorDef3.getOperatorType() ).thenReturn( STATEFUL );

        new RegionDef( 0, STATELESS, emptyList(), asList( operatorDef0, operatorDef1, operatorDef2, operatorDef3 ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateStatelessRegionWithPartitionedStatefulOperators ()
    {
        when( operatorDef0.getOperatorType() ).thenReturn( STATELESS );
        when( operatorDef1.getOperatorType() ).thenReturn( STATELESS );
        when( operatorDef2.getOperatorType() ).thenReturn( STATELESS );
        when( operatorDef3.getOperatorType() ).thenReturn( PARTITIONED_STATEFUL );

        new RegionDef( 0, STATELESS, emptyList(), asList( operatorDef0, operatorDef1, operatorDef2, operatorDef3 ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateStatelessRegionWithNonEmptyPartitionFieldNames ()
    {
        when( operatorDef0.getOperatorType() ).thenReturn( STATELESS );
        when( operatorDef1.getOperatorType() ).thenReturn( STATELESS );
        when( operatorDef2.getOperatorType() ).thenReturn( STATELESS );
        when( operatorDef3.getOperatorType() ).thenReturn( STATELESS );

        new RegionDef( 0, STATELESS, singletonList( "key" ), asList( operatorDef0, operatorDef1, operatorDef2, operatorDef3 ) );
    }

    @Test
    public void shouldCreateStatefulRegionWithStatefulAndStatelessOperators ()
    {
        when( operatorDef0.getOperatorType() ).thenReturn( STATELESS );
        when( operatorDef1.getOperatorType() ).thenReturn( STATELESS );
        when( operatorDef2.getOperatorType() ).thenReturn( STATEFUL );
        when( operatorDef3.getOperatorType() ).thenReturn( STATELESS );

        new RegionDef( 0, STATEFUL, emptyList(), asList( operatorDef0, operatorDef1, operatorDef2, operatorDef3 ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateStatefulRegionWithoutStatefulOperator ()
    {
        when( operatorDef0.getOperatorType() ).thenReturn( STATELESS );
        when( operatorDef1.getOperatorType() ).thenReturn( STATELESS );
        when( operatorDef2.getOperatorType() ).thenReturn( STATELESS );
        when( operatorDef3.getOperatorType() ).thenReturn( STATELESS );

        new RegionDef( 0, STATEFUL, emptyList(), asList( operatorDef0, operatorDef1, operatorDef2, operatorDef3 ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateStatefulRegionWithPartitionedStatefulOperator ()
    {
        when( operatorDef0.getOperatorType() ).thenReturn( STATELESS );
        when( operatorDef1.getOperatorType() ).thenReturn( STATELESS );
        when( operatorDef2.getOperatorType() ).thenReturn( PARTITIONED_STATEFUL );
        when( operatorDef3.getOperatorType() ).thenReturn( STATELESS );

        new RegionDef( 0, STATEFUL, emptyList(), asList( operatorDef0, operatorDef1, operatorDef2, operatorDef3 ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateStatefulRegionWithNonEmptyPartitionFieldNames ()
    {
        when( operatorDef0.getOperatorType() ).thenReturn( STATELESS );
        when( operatorDef1.getOperatorType() ).thenReturn( STATELESS );
        when( operatorDef2.getOperatorType() ).thenReturn( STATELESS );
        when( operatorDef3.getOperatorType() ).thenReturn( STATEFUL );

        new RegionDef( 0, STATEFUL, singletonList( "key" ), asList( operatorDef0, operatorDef1, operatorDef2, operatorDef3 ) );
    }

    @Test
    public void shouldCreatePartitionedStatefulRegionWithPartitionedStatefulAndStatelessOperators ()
    {
        when( operatorDef0.getOperatorType() ).thenReturn( STATELESS );
        when( operatorDef1.getOperatorType() ).thenReturn( STATELESS );
        when( operatorDef2.getOperatorType() ).thenReturn( PARTITIONED_STATEFUL );
        when( operatorDef0.getInputPortCount() ).thenReturn( 1 );
        when( operatorDef1.getInputPortCount() ).thenReturn( 1 );
        when( operatorDef2.getInputPortCount() ).thenReturn( 2 );
        final RuntimeSchemaField field0 = new RuntimeSchemaField( "key", Integer.class );
        final RuntimeSchemaField field1 = new RuntimeSchemaField( "key", Number.class );
        final RuntimeSchemaField field2 = new RuntimeSchemaField( "key", Integer.class );
        final PortRuntimeSchema operator0PortSchema0 = new PortRuntimeSchema( singletonList( field0 ) );
        final PortRuntimeSchema operator1PortSchema0 = new PortRuntimeSchema( singletonList( field1 ) );
        final PortRuntimeSchema operator2PortSchema0 = new PortRuntimeSchema( singletonList( field1 ) );
        final PortRuntimeSchema operator2PortSchema1 = new PortRuntimeSchema( singletonList( field2 ) );
        final OperatorRuntimeSchema operator0Schema = mock( OperatorRuntimeSchema.class );
        final OperatorRuntimeSchema operator1Schema = mock( OperatorRuntimeSchema.class );
        final OperatorRuntimeSchema operator2Schema = mock( OperatorRuntimeSchema.class );
        when( operatorDef0.getSchema() ).thenReturn( operator0Schema );
        when( operatorDef1.getSchema() ).thenReturn( operator1Schema );
        when( operatorDef2.getSchema() ).thenReturn( operator2Schema );
        when( operator0Schema.getInputPortCount() ).thenReturn( 1 );
        when( operator0Schema.getInputSchema( 0 ) ).thenReturn( operator0PortSchema0 );
        when( operator1Schema.getInputPortCount() ).thenReturn( 1 );
        when( operator1Schema.getInputSchema( 0 ) ).thenReturn( operator1PortSchema0 );
        when( operator2Schema.getInputPortCount() ).thenReturn( 2 );
        when( operator2Schema.getInputSchema( 0 ) ).thenReturn( operator2PortSchema0 );
        when( operator2Schema.getInputSchema( 1 ) ).thenReturn( operator2PortSchema1 );
        when( operatorDef1.getPartitionFieldNames() ).thenReturn( singletonList( "key" ) );
        when( operatorDef2.getPartitionFieldNames() ).thenReturn( singletonList( "key" ) );

        new RegionDef( 0, PARTITIONED_STATEFUL, singletonList( "key" ), asList( operatorDef0, operatorDef1, operatorDef2 ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreatePartitionedStatefulRegionWithIncompatibleInputFields ()
    {
        when( operatorDef0.getOperatorType() ).thenReturn( STATELESS );
        when( operatorDef1.getOperatorType() ).thenReturn( PARTITIONED_STATEFUL );
        when( operatorDef2.getOperatorType() ).thenReturn( PARTITIONED_STATEFUL );
        when( operatorDef0.getInputPortCount() ).thenReturn( 1 );
        when( operatorDef1.getInputPortCount() ).thenReturn( 1 );
        when( operatorDef2.getInputPortCount() ).thenReturn( 2 );
        final RuntimeSchemaField field0 = new RuntimeSchemaField( "key", Number.class );
        final RuntimeSchemaField field1 = new RuntimeSchemaField( "key", Integer.class );
        final RuntimeSchemaField field2 = new RuntimeSchemaField( "key", Number.class );
        final PortRuntimeSchema operator0PortSchema0 = new PortRuntimeSchema( singletonList( field0 ) );
        final PortRuntimeSchema operator1PortSchema0 = new PortRuntimeSchema( singletonList( field1 ) );
        final PortRuntimeSchema operator2PortSchema0 = new PortRuntimeSchema( singletonList( field1 ) );
        final PortRuntimeSchema operator2PortSchema1 = new PortRuntimeSchema( singletonList( field2 ) );
        final OperatorRuntimeSchema operator0Schema = mock( OperatorRuntimeSchema.class );
        final OperatorRuntimeSchema operator1Schema = mock( OperatorRuntimeSchema.class );
        final OperatorRuntimeSchema operator2Schema = mock( OperatorRuntimeSchema.class );
        when( operatorDef0.getSchema() ).thenReturn( operator0Schema );
        when( operatorDef1.getSchema() ).thenReturn( operator1Schema );
        when( operatorDef2.getSchema() ).thenReturn( operator2Schema );
        when( operator0Schema.getInputPortCount() ).thenReturn( 1 );
        when( operator0Schema.getInputSchema( 0 ) ).thenReturn( operator0PortSchema0 );
        when( operator1Schema.getInputPortCount() ).thenReturn( 1 );
        when( operator1Schema.getInputSchema( 0 ) ).thenReturn( operator1PortSchema0 );
        when( operator2Schema.getInputPortCount() ).thenReturn( 2 );
        when( operator2Schema.getInputSchema( 0 ) ).thenReturn( operator2PortSchema0 );
        when( operator2Schema.getInputSchema( 1 ) ).thenReturn( operator2PortSchema1 );
        when( operatorDef1.getPartitionFieldNames() ).thenReturn( singletonList( "key" ) );
        when( operatorDef2.getPartitionFieldNames() ).thenReturn( singletonList( "key" ) );

        new RegionDef( 0, PARTITIONED_STATEFUL, singletonList( "key" ), asList( operatorDef0, operatorDef1, operatorDef2 ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreatePartitionedStatefulRegionWithStatefulOperators ()
    {
        when( operatorDef0.getOperatorType() ).thenReturn( STATELESS );
        when( operatorDef1.getOperatorType() ).thenReturn( STATEFUL );
        when( operatorDef2.getOperatorType() ).thenReturn( PARTITIONED_STATEFUL );
        when( operatorDef0.getInputPortCount() ).thenReturn( 1 );
        when( operatorDef1.getInputPortCount() ).thenReturn( 1 );
        when( operatorDef2.getInputPortCount() ).thenReturn( 2 );
        final RuntimeSchemaField field0 = new RuntimeSchemaField( "key", Integer.class );
        final RuntimeSchemaField field1 = new RuntimeSchemaField( "key", Number.class );
        final RuntimeSchemaField field2 = new RuntimeSchemaField( "key", Integer.class );
        final PortRuntimeSchema operator0PortSchema0 = new PortRuntimeSchema( singletonList( field0 ) );
        final PortRuntimeSchema operator1PortSchema0 = new PortRuntimeSchema( singletonList( field1 ) );
        final PortRuntimeSchema operator2PortSchema0 = new PortRuntimeSchema( singletonList( field1 ) );
        final PortRuntimeSchema operator2PortSchema1 = new PortRuntimeSchema( singletonList( field2 ) );
        final OperatorRuntimeSchema operator0Schema = mock( OperatorRuntimeSchema.class );
        final OperatorRuntimeSchema operator1Schema = mock( OperatorRuntimeSchema.class );
        final OperatorRuntimeSchema operator2Schema = mock( OperatorRuntimeSchema.class );
        when( operatorDef0.getSchema() ).thenReturn( operator0Schema );
        when( operatorDef1.getSchema() ).thenReturn( operator1Schema );
        when( operatorDef2.getSchema() ).thenReturn( operator2Schema );
        when( operator0Schema.getInputPortCount() ).thenReturn( 1 );
        when( operator0Schema.getInputSchema( 0 ) ).thenReturn( operator0PortSchema0 );
        when( operator1Schema.getInputPortCount() ).thenReturn( 1 );
        when( operator1Schema.getInputSchema( 0 ) ).thenReturn( operator1PortSchema0 );
        when( operator2Schema.getInputPortCount() ).thenReturn( 2 );
        when( operator2Schema.getInputSchema( 0 ) ).thenReturn( operator2PortSchema0 );
        when( operator2Schema.getInputSchema( 1 ) ).thenReturn( operator2PortSchema1 );
        when( operatorDef1.getPartitionFieldNames() ).thenReturn( singletonList( "key" ) );
        when( operatorDef2.getPartitionFieldNames() ).thenReturn( singletonList( "key" ) );

        new RegionDef( 0, PARTITIONED_STATEFUL, singletonList( "key" ), asList( operatorDef0, operatorDef1, operatorDef2 ) );
    }

}
