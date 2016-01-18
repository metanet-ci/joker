package cs.bilkent.zanza.engine.region.impl;

import java.util.List;

import org.junit.Test;

import cs.bilkent.zanza.flow.OperatorDefinition;
import cs.bilkent.zanza.flow.OperatorDefinitionBuilder;
import cs.bilkent.zanza.flow.OperatorRuntimeSchemaBuilder;
import cs.bilkent.zanza.flow.PortRuntimeSchemaBuilder;
import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.InvocationResult;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.spec.OperatorSpec;
import cs.bilkent.zanza.operator.spec.OperatorType;
import static cs.bilkent.zanza.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.zanza.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.zanza.operator.spec.OperatorType.STATELESS;
import cs.bilkent.zanza.region.RegionDefinition;
import cs.bilkent.zanza.scheduling.SchedulingStrategy;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class RegionFormerImplOperatorSequenceRegionsTest
{

    private final RegionFormerImpl regionFormer = new RegionFormerImpl();

    @Test
    public void test_STATELESS ()
    {
        final OperatorDefinition operator = createOperator( "o1", StatelessOperator.class );

        final List<RegionDefinition> regions = regionFormer.createRegions( singletonList( operator ) );

        assertThat( regions, hasSize( 1 ) );
        assertStatelessRegion( regions.get( 0 ), singletonList( operator ) );
    }

    @Test
    public void test_PARTITIONED_STATEFUL ()
    {
        final OperatorDefinition operator = createOperator( "o1",
                                                            PartitionedStatefulOperator.class,
                                                            singletonList( "A" ),
                                                            singletonList( "A" ) );

        final List<RegionDefinition> regions = regionFormer.createRegions( singletonList( operator ) );

        assertThat( regions, hasSize( 1 ) );
        assertPartitionedStatefulRegion( regions.get( 0 ), singletonList( "A" ), singletonList( operator ) );
    }

    @Test
    public void test_STATEFUL ()
    {
        final OperatorDefinition operator = createOperator( "o1", StatefulOperator.class );

        final List<RegionDefinition> regions = regionFormer.createRegions( singletonList( operator ) );

        assertThat( regions, hasSize( 1 ) );
        assertStatefulRegion( regions.get( 0 ), singletonList( operator ) );
    }

    @Test
    public void test_STATELESS___STATELESS ()
    {
        final OperatorDefinition operator1 = createOperator( "o1", StatelessOperator.class );
        final OperatorDefinition operator2 = createOperator( "o2", StatelessOperator.class );

        final List<RegionDefinition> regions = regionFormer.createRegions( asList( operator1, operator2 ) );

        assertThat( regions, hasSize( 1 ) );
        assertStatelessRegion( regions.get( 0 ), asList( operator1, operator2 ) );
    }

    @Test
    public void test_PARTITIONED_STATEFUL___STATEFUL ()
    {
        final OperatorDefinition operator1 = createOperator( "o1",
                                                             PartitionedStatefulOperator.class,
                                                             singletonList( "A" ),
                                                             singletonList( "A" ) );
        final OperatorDefinition operator2 = createOperator( "o2", StatefulOperator.class );

        final List<RegionDefinition> regions = regionFormer.createRegions( asList( operator1, operator2 ) );

        assertThat( regions, hasSize( 2 ) );
        assertPartitionedStatefulRegion( regions.get( 0 ), singletonList( "A" ), singletonList( operator1 ) );
        assertStatefulRegion( regions.get( 1 ), singletonList( operator2 ) );
    }

    @Test
    public void test_STATELESS___STATEFUL ()
    {
        final OperatorDefinition operator1 = createOperator( "o1", StatelessOperator.class );
        final OperatorDefinition operator2 = createOperator( "o2", StatefulOperator.class );

        final List<RegionDefinition> regions = regionFormer.createRegions( asList( operator1, operator2 ) );

        assertThat( regions, hasSize( 2 ) );
        assertStatelessRegion( regions.get( 0 ), singletonList( operator1 ) );
        assertStatefulRegion( regions.get( 1 ), singletonList( operator2 ) );
    }

    @Test
    public void test_STATEFUL___STATELESS ()
    {
        final OperatorDefinition operator1 = createOperator( "o1", StatefulOperator.class );
        final OperatorDefinition operator2 = createOperator( "o2", StatelessOperator.class );

        final List<RegionDefinition> regions = regionFormer.createRegions( asList( operator1, operator2 ) );

        assertThat( regions, hasSize( 2 ) );
        assertStatefulRegion( regions.get( 0 ), singletonList( operator1 ) );
        assertStatelessRegion( regions.get( 1 ), singletonList( operator2 ) );
    }

    @Test
    public void test_STATEFUL___PARTITIONED_STATEFUL ()
    {
        final OperatorDefinition operator1 = createOperator( "o1", StatefulOperator.class );
        final OperatorDefinition operator2 = createOperator( "o2",
                                                             PartitionedStatefulOperator.class,
                                                             singletonList( "A" ),
                                                             singletonList( "A" ) );

        final List<RegionDefinition> regions = regionFormer.createRegions( asList( operator1, operator2 ) );

        assertThat( regions, hasSize( 2 ) );
        assertStatefulRegion( regions.get( 0 ), singletonList( operator1 ) );
        assertPartitionedStatefulRegion( regions.get( 1 ), singletonList( "A" ), singletonList( operator2 ) );
    }

    @Test
    public void test_STATEFUL___STATEFUL ()
    {
        final OperatorDefinition operator1 = createOperator( "o1", StatefulOperator.class );
        final OperatorDefinition operator2 = createOperator( "o2", StatefulOperator.class );

        final List<RegionDefinition> regions = regionFormer.createRegions( asList( operator1, operator2 ) );

        assertThat( regions, hasSize( 2 ) );
        assertStatefulRegion( regions.get( 0 ), singletonList( operator1 ) );
        assertStatefulRegion( regions.get( 1 ), singletonList( operator2 ) );
    }

    @Test
    public void test_PARTITIONED_STATEFUL___STATELESS ()
    {
        final OperatorDefinition operator1 = createOperator( "o1",
                                                             PartitionedStatefulOperator.class,
                                                             singletonList( "A" ),
                                                             singletonList( "A" ) );
        final OperatorDefinition operator2 = createOperator( "o2", StatelessOperator.class );

        final List<RegionDefinition> regions = regionFormer.createRegions( asList( operator1, operator2 ) );

        assertThat( regions, hasSize( 1 ) );
        assertPartitionedStatefulRegion( regions.get( 0 ), singletonList( "A" ), asList( operator1, operator2 ) );
    }

    @Test
    public void test_PARTITIONED_STATEFUL___PARTITIONED_STATEFUL___ABC___ABC ()
    {
        final OperatorDefinition operator1 = createOperator( "o1",
                                                             PartitionedStatefulOperator.class,
                                                             asList( "A", "B", "C" ),
                                                             asList( "A", "B", "C" ) );
        final OperatorDefinition operator2 = createOperator( "o1",
                                                             PartitionedStatefulOperator.class,
                                                             asList( "A", "B", "C" ),
                                                             asList( "A", "B", "C" ) );

        final List<RegionDefinition> regions = regionFormer.createRegions( asList( operator1, operator2 ) );

        assertThat( regions, hasSize( 1 ) );
        assertPartitionedStatefulRegion( regions.get( 0 ), asList( "A", "B", "C" ), asList( operator1, operator2 ) );
    }

    @Test
    public void test_PARTITIONED_STATEFUL___PARTITIONED_STATEFUL___ABC___DEF ()
    {
        final OperatorDefinition operator1 = createOperator( "o1",
                                                             PartitionedStatefulOperator.class,
                                                             asList( "A", "B", "C" ),
                                                             asList( "A", "B", "C" ) );
        final OperatorDefinition operator2 = createOperator( "o1",
                                                             PartitionedStatefulOperator.class,
                                                             asList( "D", "E", "F" ),
                                                             asList( "D", "E", "F" ) );

        final List<RegionDefinition> regions = regionFormer.createRegions( asList( operator1, operator2 ) );

        assertThat( regions, hasSize( 2 ) );
        assertPartitionedStatefulRegion( regions.get( 0 ), asList( "A", "B", "C" ), singletonList( operator1 ) );
        assertPartitionedStatefulRegion( regions.get( 1 ), asList( "D", "E", "F" ), singletonList( operator2 ) );
    }

    @Test
    public void test_PARTITIONED_STATEFUL___PARTITIONED_STATEFUL___ABD___ABC ()
    {
        final OperatorDefinition operator1 = createOperator( "o1",
                                                             PartitionedStatefulOperator.class,
                                                             asList( "A", "B", "D" ),
                                                             asList( "A", "B", "D" ) );
        final OperatorDefinition operator2 = createOperator( "o1",
                                                             PartitionedStatefulOperator.class,
                                                             asList( "A", "B", "C" ),
                                                             asList( "A", "B", "C" ) );

        final List<RegionDefinition> regions = regionFormer.createRegions( asList( operator1, operator2 ) );

        assertThat( regions, hasSize( 1 ) );
        assertPartitionedStatefulRegion( regions.get( 0 ), asList( "A", "B" ), asList( operator1, operator2 ) );
    }

    @Test
    public void test_PARTITIONED_STATEFUL___PARTITIONED_STATEFUL___AB___ABC ()
    {
        final OperatorDefinition operator1 = createOperator( "o1",
                                                             PartitionedStatefulOperator.class,
                                                             asList( "A", "B" ),
                                                             asList( "A", "B" ) );
        final OperatorDefinition operator2 = createOperator( "o1",
                                                             PartitionedStatefulOperator.class,
                                                             asList( "A", "B", "C" ),
                                                             asList( "A", "B", "C" ) );

        final List<RegionDefinition> regions = regionFormer.createRegions( asList( operator1, operator2 ) );

        assertThat( regions, hasSize( 1 ) );
        assertPartitionedStatefulRegion( regions.get( 0 ), asList( "A", "B" ), asList( operator1, operator2 ) );
    }

    @Test
    public void test_PARTITIONED_STATEFUL___PARTITIONED_STATEFUL___ABC___AB ()
    {
        final OperatorDefinition operator1 = createOperator( "o1",
                                                             PartitionedStatefulOperator.class,
                                                             asList( "A", "B", "C" ),
                                                             asList( "A", "B", "C" ) );
        final OperatorDefinition operator2 = createOperator( "o1",
                                                             PartitionedStatefulOperator.class,
                                                             asList( "A", "B" ),
                                                             asList( "A", "B" ) );

        final List<RegionDefinition> regions = regionFormer.createRegions( asList( operator1, operator2 ) );

        assertThat( regions, hasSize( 1 ) );
        assertPartitionedStatefulRegion( regions.get( 0 ), asList( "A", "B" ), asList( operator1, operator2 ) );
    }

    static void assertStatelessRegion ( final RegionDefinition region, final List<OperatorDefinition> operators )
    {
        assertRegion( region, STATELESS, emptyList(), operators );
    }

    static void assertPartitionedStatefulRegion ( final RegionDefinition region,
                                                  final List<String> partitionFieldNames,
                                                  final List<OperatorDefinition> operators )
    {
        assertRegion( region, PARTITIONED_STATEFUL, partitionFieldNames, operators );
    }

    static void assertStatefulRegion ( final RegionDefinition region, final List<OperatorDefinition> operators )
    {
        assertRegion( region, STATEFUL, emptyList(), operators );
    }


    static void assertRegion ( final RegionDefinition region,
                               final OperatorType regionType,
                               final List<String> partitionFieldNames,
                               final List<OperatorDefinition> operators )
    {
        assertThat( region.getRegionType(), equalTo( regionType ) );
        assertThat( region.getPartitionFieldNames(), equalTo( partitionFieldNames ) );
        assertThat( region.getOperators(), equalTo( operators ) );
    }

    static OperatorDefinition createOperator ( final String operatorId, final Class<? extends Operator> operatorClazz )
    {
        return createOperator( operatorId, operatorClazz, emptyList(), emptyList() );
    }

    static OperatorDefinition createOperator ( final String operatorId,
                                               final Class<? extends Operator> operatorClazz,
                                               final List<String> schemaFieldNames,
                                               final List<String> partitionFieldNames )
    {
        final OperatorDefinitionBuilder builder = OperatorDefinitionBuilder.newInstance( operatorId, operatorClazz );
        final OperatorRuntimeSchemaBuilder schemaBuilder = new OperatorRuntimeSchemaBuilder( 1, 1 );
        final PortRuntimeSchemaBuilder portSchemaBuilder = schemaBuilder.getInputPortSchemaBuilder( 0 );
        for ( String schemaFieldName : schemaFieldNames )
        {
            portSchemaBuilder.addField( schemaFieldName, Integer.class );
        }

        return builder.setInputPortCount( 1 )
                      .setOutputPortCount( 1 )
                      .setExtendingSchema( schemaBuilder )
                      .setPartitionFieldNames( partitionFieldNames )
                      .build();
    }

    @OperatorSpec( type = STATELESS )
    static class StatelessOperator implements Operator
    {

        @Override
        public SchedulingStrategy init ( final InitializationContext context )
        {
            return null;
        }

        @Override
        public InvocationResult process ( final InvocationContext invocationContext )
        {
            return null;
        }

    }


    @OperatorSpec( type = PARTITIONED_STATEFUL )
    static class PartitionedStatefulOperator implements Operator
    {

        @Override
        public SchedulingStrategy init ( final InitializationContext context )
        {
            return null;
        }

        @Override
        public InvocationResult process ( final InvocationContext invocationContext )
        {
            return null;
        }

    }


    @OperatorSpec( type = STATEFUL )
    static class StatefulOperator implements Operator
    {

        @Override
        public SchedulingStrategy init ( final InitializationContext context )
        {
            return null;
        }

        @Override
        public InvocationResult process ( final InvocationContext invocationContext )
        {
            return null;
        }

    }

}
