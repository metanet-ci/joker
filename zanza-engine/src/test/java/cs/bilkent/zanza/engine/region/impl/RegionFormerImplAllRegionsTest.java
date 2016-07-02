package cs.bilkent.zanza.engine.region.impl;

import java.util.List;

import org.junit.Test;

import cs.bilkent.zanza.engine.region.RegionDefinition;
import static cs.bilkent.zanza.engine.region.impl.RegionFormerImplRegionTest.assertRegion;
import cs.bilkent.zanza.flow.FlowDefinition;
import cs.bilkent.zanza.flow.FlowDefinitionBuilder;
import cs.bilkent.zanza.flow.OperatorDefinition;
import cs.bilkent.zanza.flow.OperatorDefinitionBuilder;
import cs.bilkent.zanza.flow.OperatorRuntimeSchemaBuilder;
import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import cs.bilkent.zanza.operator.schema.annotation.OperatorSchema;
import cs.bilkent.zanza.operator.schema.annotation.PortSchema;
import static cs.bilkent.zanza.operator.schema.annotation.PortSchemaScope.EXACT_FIELD_SET;
import cs.bilkent.zanza.operator.schema.annotation.SchemaField;
import cs.bilkent.zanza.operator.spec.OperatorSpec;
import cs.bilkent.zanza.operator.spec.OperatorType;
import static cs.bilkent.zanza.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.zanza.operator.spec.OperatorType.STATELESS;
import cs.bilkent.zanza.operators.MapperOperator;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.fail;

public class RegionFormerImplAllRegionsTest
{

    private final RegionFormerImpl regionFormer = new RegionFormerImpl();

    private final FlowDefinitionBuilder flowBuilder = new FlowDefinitionBuilder();


    @Test
    public void testFlowWithSingleOperatorSequence ()
    {
        /**
         * O1 --> O2
         */

        final OperatorDefinition operator1 = OperatorDefinitionBuilder.newInstance( "o1", MapperOperator.class ).build();
        final OperatorDefinition operator2 = OperatorDefinitionBuilder.newInstance( "o2", MapperOperator.class ).build();
        flowBuilder.add( operator1 );
        flowBuilder.add( operator2 );
        flowBuilder.connect( "o1", "o2" );
        final FlowDefinition flow = flowBuilder.build();

        final List<RegionDefinition> regions = regionFormer.createRegions( flow );
        assertThat( regions, hasSize( 1 ) );
        assertRegion( regions.get( 0 ), STATELESS, emptyList(), asList( operator1, operator2 ) );
    }

    @Test
    public void testFlowWithTwoOperatorsWithMultipleConnections ()
    {
        /**
         *     /-\
         *    /   \
         * O1 -----> O2
         */

        final OperatorDefinition operator1 = OperatorDefinitionBuilder.newInstance( "o1", DoubleOutputPortOperator.class )
                                                                      .setPartitionFieldNames( singletonList( "f" ) )
                                                                      .build();
        final OperatorRuntimeSchemaBuilder mapperSchema = new OperatorRuntimeSchemaBuilder( 1, 1 );
        mapperSchema.getInputPortSchemaBuilder( 0 ).addField( "f", Integer.class );
        final OperatorDefinition operator2 = OperatorDefinitionBuilder.newInstance( "o2", MapperOperator.class )
                                                                      .setExtendingSchema( mapperSchema )
                                                                      .build();
        flowBuilder.add( operator1 );
        flowBuilder.add( operator2 );
        flowBuilder.connect( "o1", "o2" );
        final FlowDefinition flow = flowBuilder.build();

        final List<RegionDefinition> regions = regionFormer.createRegions( flow );
        assertThat( regions, hasSize( 1 ) );
        assertRegion( regions.get( 0 ), PARTITIONED_STATEFUL, singletonList( "f" ), asList( operator1, operator2 ) );
    }

    @Test
    public void testFlowWithMultipleOperatorSequences ()
    {
        /**
         *
         *          /--> O4
         *         /
         * O1 --> O2 --> O3
         *
         */

        final OperatorDefinition operator1 = OperatorDefinitionBuilder.newInstance( "o1", MapperOperator.class ).build();
        final OperatorDefinition operator2 = OperatorDefinitionBuilder.newInstance( "o2", MapperOperator.class ).build();
        final OperatorDefinition operator3 = OperatorDefinitionBuilder.newInstance( "o3", MapperOperator.class ).build();
        final OperatorDefinition operator4 = OperatorDefinitionBuilder.newInstance( "o4", MapperOperator.class ).build();
        flowBuilder.add( operator1 );
        flowBuilder.add( operator2 );
        flowBuilder.add( operator3 );
        flowBuilder.add( operator4 );
        flowBuilder.connect( "o1", "o2" );
        flowBuilder.connect( "o2", "o3" );
        flowBuilder.connect( "o2", "o4" );

        final FlowDefinition flow = flowBuilder.build();
        final List<RegionDefinition> regions = regionFormer.createRegions( flow );
        assertThat( regions, hasSize( 3 ) );
        assertRegionExists( regions, STATELESS, emptyList(), asList( operator1, operator2 ) );
        assertRegionExists( regions, STATELESS, emptyList(), singletonList( operator3 ) );
        assertRegionExists( regions, STATELESS, emptyList(), singletonList( operator4 ) );
    }

    private void assertRegionExists ( final List<RegionDefinition> regions,
                                      final OperatorType regionType,
                                      final List<String> partitionFieldNames,
                                      final List<OperatorDefinition> operators )
    {
        for ( RegionDefinition region : regions )
        {
            try
            {
                assertThat( region.getRegionType(), equalTo( regionType ) );
                assertThat( region.getPartitionFieldNames(), equalTo( partitionFieldNames ) );
                assertThat( region.getOperators(), equalTo( operators ) );
                return;
            }
            catch ( AssertionError expected )
            {

            }
        }

        fail();
    }

    @OperatorSpec( type = PARTITIONED_STATEFUL, inputPortCount = 1, outputPortCount = 2 )
    @OperatorSchema(
            inputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "f", type = Integer.class ) }
            ) },
            outputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "f", type = Integer.class )
            } ),
                        @PortSchema( portIndex = 1, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "f", type = Integer.class )
                        } ) } )
    private static class DoubleOutputPortOperator implements Operator
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
