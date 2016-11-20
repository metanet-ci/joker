package cs.bilkent.joker.engine.region.impl;

import java.util.List;

import org.junit.Test;

import cs.bilkent.joker.engine.region.RegionDef;
import cs.bilkent.joker.operator.InitializationContext;
import cs.bilkent.joker.operator.InvocationContext;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchemaBuilder;
import cs.bilkent.joker.operator.schema.runtime.PortRuntimeSchemaBuilder;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import cs.bilkent.joker.operator.spec.OperatorType;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class RegionFormerImplRegionDefTest extends AbstractJokerTest
{

    private final RegionDefFormerImpl regionFormer = new RegionDefFormerImpl( new IdGenerator() );

    @Test
    public void test_STATELESS ()
    {
        final OperatorDef operator = createOperator( "o1", StatelessOperator.class );

        final List<RegionDef> regions = regionFormer.createRegions( singletonList( operator ) );

        assertThat( regions, hasSize( 1 ) );
        assertStatelessRegion( regions.get( 0 ), singletonList( operator ) );
    }

    @Test
    public void test_PARTITIONED_STATEFUL ()
    {
        final OperatorDef operator = createPartitionedStatefulOperator( "o1", singletonList( "A" ) );

        final List<RegionDef> regions = regionFormer.createRegions( singletonList( operator ) );

        assertThat( regions, hasSize( 1 ) );
        assertPartitionedStatefulRegion( regions.get( 0 ), singletonList( "A" ), singletonList( operator ) );
    }

    @Test
    public void test_STATEFUL ()
    {
        final OperatorDef operator = createOperator( "o1", StatefulOperator.class );

        final List<RegionDef> regions = regionFormer.createRegions( singletonList( operator ) );

        assertThat( regions, hasSize( 1 ) );
        assertStatefulRegion( regions.get( 0 ), singletonList( operator ) );
    }

    @Test
    public void test_STATELESS___STATELESS ()
    {
        final OperatorDef operator1 = createOperator( "o1", StatelessOperator.class );
        final OperatorDef operator2 = createOperator( "o2", StatelessOperator.class );

        final List<RegionDef> regions = regionFormer.createRegions( asList( operator1, operator2 ) );

        assertThat( regions, hasSize( 1 ) );
        assertStatelessRegion( regions.get( 0 ), asList( operator1, operator2 ) );
    }

    @Test
    public void test_PARTITIONED_STATEFUL___STATEFUL ()
    {
        final OperatorDef operator1 = createPartitionedStatefulOperator( "o1", singletonList( "A" ) );
        final OperatorDef operator2 = createOperator( "o2", StatefulOperator.class );

        final List<RegionDef> regions = regionFormer.createRegions( asList( operator1, operator2 ) );

        assertThat( regions, hasSize( 2 ) );
        assertPartitionedStatefulRegion( regions.get( 0 ), singletonList( "A" ), singletonList( operator1 ) );
        assertStatefulRegion( regions.get( 1 ), singletonList( operator2 ) );
    }

    @Test
    public void test_STATELESS___STATEFUL ()
    {
        final OperatorDef operator1 = createOperator( "o1", StatelessOperator.class );
        final OperatorDef operator2 = createOperator( "o2", StatefulOperator.class );

        final List<RegionDef> regions = regionFormer.createRegions( asList( operator1, operator2 ) );

        assertThat( regions, hasSize( 2 ) );
        assertStatelessRegion( regions.get( 0 ), singletonList( operator1 ) );
        assertStatefulRegion( regions.get( 1 ), singletonList( operator2 ) );
    }

    @Test
    public void test_STATEFUL___STATELESS ()
    {
        final OperatorDef operator1 = createOperator( "o1", StatefulOperator.class );
        final OperatorDef operator2 = createOperator( "o2", StatelessOperator.class );

        final List<RegionDef> regions = regionFormer.createRegions( asList( operator1, operator2 ) );

        assertThat( regions, hasSize( 2 ) );
        assertStatefulRegion( regions.get( 0 ), singletonList( operator1 ) );
        assertStatelessRegion( regions.get( 1 ), singletonList( operator2 ) );
    }

    @Test
    public void test_STATEFUL___PARTITIONED_STATEFUL ()
    {
        final OperatorDef operator1 = createOperator( "o1", StatefulOperator.class );
        final OperatorDef operator2 = createPartitionedStatefulOperator( "o2", singletonList( "A" ) );

        final List<RegionDef> regions = regionFormer.createRegions( asList( operator1, operator2 ) );

        assertThat( regions, hasSize( 2 ) );
        assertStatefulRegion( regions.get( 0 ), singletonList( operator1 ) );
        assertPartitionedStatefulRegion( regions.get( 1 ), singletonList( "A" ), singletonList( operator2 ) );
    }

    @Test
    public void test_STATELESS___PARTITIONED_STATEFUL___ABC___AB ()
    {
        final OperatorDef operator1 = createStatelessOperator( "o1", asList( "A", "B", "C" ) );
        final OperatorDef operator2 = createPartitionedStatefulOperator( "o2", asList( "A", "B" ) );

        final List<RegionDef> regions = regionFormer.createRegions( asList( operator1, operator2 ) );

        assertThat( regions, hasSize( 1 ) );
        assertPartitionedStatefulRegion( regions.get( 0 ), asList( "A", "B" ), asList( operator1, operator2 ) );
    }

    @Test
    public void test_STATELESS___STATELESS___PARTITIONED_STATEFUL___ABCD___ABC___AB ()
    {
        final OperatorDef operator1 = createStatelessOperator( "o1", asList( "A", "B", "C", "D" ) );
        final OperatorDef operator2 = createStatelessOperator( "o2", asList( "A", "B", "C" ) );
        final OperatorDef operator3 = createPartitionedStatefulOperator( "o3", asList( "A", "B" ) );

        final List<RegionDef> regions = regionFormer.createRegions( asList( operator1, operator2, operator3 ) );

        assertThat( regions, hasSize( 1 ) );
        assertPartitionedStatefulRegion( regions.get( 0 ), asList( "A", "B" ), asList( operator1, operator2, operator3 ) );
    }

    @Test
    public void test_STATELESS___STATELESS___PARTITIONED_STATEFUL___ABC___ABC___AB ()
    {
        final OperatorDef operator1 = createStatelessOperator( "o1", asList( "A", "B", "C" ) );
        final OperatorDef operator2 = createStatelessOperator( "o2", asList( "A", "B", "C" ) );
        final OperatorDef operator3 = createPartitionedStatefulOperator( "o3", asList( "A", "B" ) );

        final List<RegionDef> regions = regionFormer.createRegions( asList( operator1, operator2, operator3 ) );

        assertThat( regions, hasSize( 1 ) );
        assertPartitionedStatefulRegion( regions.get( 0 ), asList( "A", "B" ), asList( operator1, operator2, operator3 ) );
    }

    @Test
    public void test_STATELESS___STATELESS___PARTITIONED_STATEFUL___ABD___ABC___AB ()
    {
        final OperatorDef operator1 = createStatelessOperator( "o1", asList( "A", "B", "D" ) );
        final OperatorDef operator2 = createStatelessOperator( "o2", asList( "A", "B", "C" ) );
        final OperatorDef operator3 = createPartitionedStatefulOperator( "o3", asList( "A", "B" ) );

        final List<RegionDef> regions = regionFormer.createRegions( asList( operator1, operator2, operator3 ) );

        assertThat( regions, hasSize( 1 ) );
        assertPartitionedStatefulRegion( regions.get( 0 ), asList( "A", "B" ), asList( operator1, operator2, operator3 ) );
    }

    @Test
    public void test_STATELESS___STATELESS___PARTITIONED_STATEFUL___AB___ABC___AB ()
    {
        final OperatorDef operator1 = createStatelessOperator( "o1", asList( "A", "B" ) );
        final OperatorDef operator2 = createStatelessOperator( "o2", asList( "A", "B", "C" ) );
        final OperatorDef operator3 = createPartitionedStatefulOperator( "o3", asList( "A", "B" ) );

        final List<RegionDef> regions = regionFormer.createRegions( asList( operator1, operator2, operator3 ) );

        assertThat( regions, hasSize( 1 ) );
        assertPartitionedStatefulRegion( regions.get( 0 ), asList( "A", "B" ), asList( operator1, operator2, operator3 ) );
    }

    @Test
    public void test_STATELESS___PARTITIONED_STATEFUL___ABC___ABC ()
    {
        final OperatorDef operator1 = createStatelessOperator( "o1", asList( "A", "B", "C" ) );
        final OperatorDef operator2 = createPartitionedStatefulOperator( "o2", asList( "A", "B", "C" ) );

        final List<RegionDef> regions = regionFormer.createRegions( asList( operator1, operator2 ) );

        assertThat( regions, hasSize( 1 ) );
        assertPartitionedStatefulRegion( regions.get( 0 ), asList( "A", "B", "C" ), asList( operator1, operator2 ) );
    }

    @Test
    public void test_STATELESS___STATELESS___PARTITIONED_STATEFUL___AB___ABC___ABC ()
    {
        final OperatorDef operator1 = createStatelessOperator( "o1", asList( "A", "B" ) );
        final OperatorDef operator2 = createStatelessOperator( "o2", asList( "A", "B", "C" ) );
        final OperatorDef operator3 = createPartitionedStatefulOperator( "o3", asList( "A", "B", "C" ) );

        final List<RegionDef> regions = regionFormer.createRegions( asList( operator1, operator2, operator3 ) );

        assertThat( regions, hasSize( 2 ) );
        assertStatelessRegion( regions.get( 0 ), singletonList( operator1 ) );
        assertPartitionedStatefulRegion( regions.get( 1 ), asList( "A", "B", "C" ), asList( operator2, operator3 ) );
    }

    @Test
    public void test_STATELESS___STATELESS___PARTITIONED_STATEFUL___ABC___ABC___ABC ()
    {
        final OperatorDef operator1 = createStatelessOperator( "o1", asList( "A", "B", "C" ) );
        final OperatorDef operator2 = createStatelessOperator( "o2", asList( "A", "B", "C" ) );
        final OperatorDef operator3 = createPartitionedStatefulOperator( "o3", asList( "A", "B", "C" ) );

        final List<RegionDef> regions = regionFormer.createRegions( asList( operator1, operator2, operator3 ) );

        assertThat( regions, hasSize( 1 ) );
        assertPartitionedStatefulRegion( regions.get( 0 ), asList( "A", "B", "C" ), asList( operator1, operator2, operator3 ) );
    }

    @Test
    public void test_STATELESS___STATELESS___PARTITIONED_STATEFUL___ABD___ABC___ABC ()
    {
        final OperatorDef operator1 = createStatelessOperator( "o1", asList( "A", "B", "D" ) );
        final OperatorDef operator2 = createStatelessOperator( "o2", asList( "A", "B", "C" ) );
        final OperatorDef operator3 = createPartitionedStatefulOperator( "o3", asList( "A", "B", "C" ) );

        final List<RegionDef> regions = regionFormer.createRegions( asList( operator1, operator2, operator3 ) );

        assertThat( regions, hasSize( 2 ) );
        assertStatelessRegion( regions.get( 0 ), singletonList( operator1 ) );
        assertPartitionedStatefulRegion( regions.get( 1 ), asList( "A", "B", "C" ), asList( operator2, operator3 ) );
    }

    @Test
    public void test_STATELESS___STATELESS___PARTITIONED_STATEFUL___ABCD___ABC___ABC ()
    {
        final OperatorDef operator1 = createStatelessOperator( "o1", asList( "A", "B", "C", "D" ) );
        final OperatorDef operator2 = createStatelessOperator( "o2", asList( "A", "B", "C" ) );
        final OperatorDef operator3 = createPartitionedStatefulOperator( "o3", asList( "A", "B", "C" ) );

        final List<RegionDef> regions = regionFormer.createRegions( asList( operator1, operator2, operator3 ) );

        assertThat( regions, hasSize( 1 ) );
        assertPartitionedStatefulRegion( regions.get( 0 ), asList( "A", "B", "C" ), asList( operator1, operator2, operator3 ) );
    }

    @Test
    public void test_STATELESS___PARTITIONED_STATEFUL___ABC___ABCD ()
    {
        final OperatorDef operator1 = createStatelessOperator( "o1", asList( "A", "B", "C" ) );
        final OperatorDef operator2 = createPartitionedStatefulOperator( "o2", asList( "A", "B", "C", "D" ) );

        final List<RegionDef> regions = regionFormer.createRegions( asList( operator1, operator2 ) );

        assertThat( regions, hasSize( 2 ) );
        assertStatelessRegion( regions.get( 0 ), singletonList( operator1 ) );
        assertPartitionedStatefulRegion( regions.get( 1 ), asList( "A", "B", "C", "D" ), singletonList( operator2 ) );
    }

    @Test
    public void test_STATELESS___STATELESS___PARTITIONED_STATEFUL___AB___ABC___ABCD ()
    {
        final OperatorDef operator1 = createStatelessOperator( "o1", asList( "A", "B" ) );
        final OperatorDef operator2 = createStatelessOperator( "o2", asList( "A", "B", "C" ) );
        final OperatorDef operator3 = createPartitionedStatefulOperator( "o3", asList( "A", "B", "C", "D" ) );

        final List<RegionDef> regions = regionFormer.createRegions( asList( operator1, operator2, operator3 ) );

        assertThat( regions, hasSize( 2 ) );
        assertStatelessRegion( regions.get( 0 ), asList( operator1, operator2 ) );
        assertPartitionedStatefulRegion( regions.get( 1 ), asList( "A", "B", "C", "D" ), singletonList( operator3 ) );
    }

    @Test
    public void test_STATELESS___STATELESS___PARTITIONED_STATEFUL___ABC___ABC___ABCD ()
    {
        final OperatorDef operator1 = createStatelessOperator( "o1", asList( "A", "B", "C" ) );
        final OperatorDef operator2 = createStatelessOperator( "o2", asList( "A", "B", "C" ) );
        final OperatorDef operator3 = createPartitionedStatefulOperator( "o3", asList( "A", "B", "C", "D" ) );

        final List<RegionDef> regions = regionFormer.createRegions( asList( operator1, operator2, operator3 ) );

        assertThat( regions, hasSize( 2 ) );
        assertStatelessRegion( regions.get( 0 ), asList( operator1, operator2 ) );
        assertPartitionedStatefulRegion( regions.get( 1 ), asList( "A", "B", "C", "D" ), singletonList( operator3 ) );
    }

    @Test
    public void test_STATELESS___STATELESS___PARTITIONED_STATEFUL___ABD___ABC___ABCD ()
    {
        final OperatorDef operator1 = createStatelessOperator( "o1", asList( "A", "B", "D" ) );
        final OperatorDef operator2 = createStatelessOperator( "o2", asList( "A", "B", "C" ) );
        final OperatorDef operator3 = createPartitionedStatefulOperator( "o3", asList( "A", "B", "C", "D" ) );

        final List<RegionDef> regions = regionFormer.createRegions( asList( operator1, operator2, operator3 ) );

        assertThat( regions, hasSize( 2 ) );
        assertStatelessRegion( regions.get( 0 ), asList( operator1, operator2 ) );
        assertPartitionedStatefulRegion( regions.get( 1 ), asList( "A", "B", "C", "D" ), singletonList( operator3 ) );
    }

    @Test
    public void test_STATELESS___STATELESS___PARTITIONED_STATEFUL___ABCD___ABC___ABCD ()
    {
        final OperatorDef operator1 = createStatelessOperator( "o1", asList( "A", "B", "C", "D" ) );
        final OperatorDef operator2 = createStatelessOperator( "o2", asList( "A", "B", "C" ) );
        final OperatorDef operator3 = createPartitionedStatefulOperator( "o3", asList( "A", "B", "C", "D" ) );

        final List<RegionDef> regions = regionFormer.createRegions( asList( operator1, operator2, operator3 ) );

        assertThat( regions, hasSize( 2 ) );
        assertStatelessRegion( regions.get( 0 ), asList( operator1, operator2 ) );
        assertPartitionedStatefulRegion( regions.get( 1 ), asList( "A", "B", "C", "D" ), singletonList( operator3 ) );
    }

    @Test
    public void test_STATEFUL___STATEFUL ()
    {
        final OperatorDef operator1 = createOperator( "o1", StatefulOperator.class );
        final OperatorDef operator2 = createOperator( "o2", StatefulOperator.class );

        final List<RegionDef> regions = regionFormer.createRegions( asList( operator1, operator2 ) );

        assertThat( regions, hasSize( 2 ) );
        assertStatefulRegion( regions.get( 0 ), singletonList( operator1 ) );
        assertStatefulRegion( regions.get( 1 ), singletonList( operator2 ) );
    }

    @Test
    public void test_PARTITIONED_STATEFUL___STATELESS ()
    {
        final OperatorDef operator1 = createPartitionedStatefulOperator( "o1", singletonList( "A" ) );
        final OperatorDef operator2 = createOperator( "o2", StatelessOperator.class );

        final List<RegionDef> regions = regionFormer.createRegions( asList( operator1, operator2 ) );

        assertThat( regions, hasSize( 1 ) );
        assertPartitionedStatefulRegion( regions.get( 0 ), singletonList( "A" ), asList( operator1, operator2 ) );
    }

    @Test
    public void test_PARTITIONED_STATEFUL___PARTITIONED_STATEFUL___ABC___ABC ()
    {
        final OperatorDef operator1 = createPartitionedStatefulOperator( "o1", asList( "A", "B", "C" ) );
        final OperatorDef operator2 = createPartitionedStatefulOperator( "o1", asList( "A", "B", "C" ) );

        final List<RegionDef> regions = regionFormer.createRegions( asList( operator1, operator2 ) );

        assertThat( regions, hasSize( 1 ) );
        assertPartitionedStatefulRegion( regions.get( 0 ), asList( "A", "B", "C" ), asList( operator1, operator2 ) );
    }

    @Test
    public void test_PARTITIONED_STATEFUL___PARTITIONED_STATEFUL___ABC___DEF ()
    {
        final OperatorDef operator1 = createPartitionedStatefulOperator( "o1", asList( "A", "B", "C" ) );
        final OperatorDef operator2 = createPartitionedStatefulOperator( "o1", asList( "D", "E", "F" ) );

        final List<RegionDef> regions = regionFormer.createRegions( asList( operator1, operator2 ) );

        assertThat( regions, hasSize( 2 ) );
        assertPartitionedStatefulRegion( regions.get( 0 ), asList( "A", "B", "C" ), singletonList( operator1 ) );
        assertPartitionedStatefulRegion( regions.get( 1 ), asList( "D", "E", "F" ), singletonList( operator2 ) );
    }

    @Test
    public void test_PARTITIONED_STATEFUL___PARTITIONED_STATEFUL___ABD___ABC ()
    {
        final OperatorDef operator1 = createPartitionedStatefulOperator( "o1", asList( "A", "B", "D" ) );
        final OperatorDef operator2 = createPartitionedStatefulOperator( "o1", asList( "A", "B", "C" ) );

        final List<RegionDef> regions = regionFormer.createRegions( asList( operator1, operator2 ) );

        assertThat( regions, hasSize( 2 ) );
        assertPartitionedStatefulRegion( regions.get( 0 ), asList( "A", "B", "D" ), singletonList( operator1 ) );
        assertPartitionedStatefulRegion( regions.get( 1 ), asList( "A", "B", "C" ), singletonList( operator2 ) );
    }

    @Test
    public void test_PARTITIONED_STATEFUL___PARTITIONED_STATEFUL___AB___ABC ()
    {
        final OperatorDef operator1 = createPartitionedStatefulOperator( "o1", asList( "A", "B" ) );
        final OperatorDef operator2 = createPartitionedStatefulOperator( "o1", asList( "A", "B", "C" ) );

        final List<RegionDef> regions = regionFormer.createRegions( asList( operator1, operator2 ) );

        assertThat( regions, hasSize( 1 ) );
        assertPartitionedStatefulRegion( regions.get( 0 ), asList( "A", "B" ), asList( operator1, operator2 ) );
    }

    @Test
    public void test_PARTITIONED_STATEFUL___PARTITIONED_STATEFUL___ABC___AB ()
    {
        final OperatorDef operator1 = createPartitionedStatefulOperator( "o1", asList( "A", "B", "C" ) );
        final OperatorDef operator2 = createPartitionedStatefulOperator( "o1", asList( "A", "B" ) );

        final List<RegionDef> regions = regionFormer.createRegions( asList( operator1, operator2 ) );

        assertThat( regions, hasSize( 2 ) );
        assertPartitionedStatefulRegion( regions.get( 0 ), asList( "A", "B", "C" ), singletonList( operator1 ) );
        assertPartitionedStatefulRegion( regions.get( 1 ), asList( "A", "B" ), singletonList( operator2 ) );
    }

    static void assertStatelessRegion ( final RegionDef region, final List<OperatorDef> operators )
    {
        assertRegion( region, STATELESS, emptyList(), operators );
    }

    static void assertPartitionedStatefulRegion ( final RegionDef region,
                                                  final List<String> partitionFieldNames,
                                                  final List<OperatorDef> operators )
    {
        assertRegion( region, PARTITIONED_STATEFUL, partitionFieldNames, operators );
    }

    static void assertStatefulRegion ( final RegionDef region, final List<OperatorDef> operators )
    {
        assertRegion( region, STATEFUL, emptyList(), operators );
    }


    static void assertRegion ( final RegionDef region,
                               final OperatorType regionType,
                               final List<String> partitionFieldNames,
                               final List<OperatorDef> operators )
    {
        assertThat( region.getRegionType(), equalTo( regionType ) );
        assertThat( region.getPartitionFieldNames(), equalTo( partitionFieldNames ) );
        assertThat( region.getOperators(), equalTo( operators ) );
    }

    OperatorDef createOperator ( final String operatorId, final Class<? extends Operator> operatorClazz )
    {
        return createOperator( operatorId, operatorClazz, emptyList(), emptyList() );
    }

    OperatorDef createStatelessOperator ( final String operatorId, final List<String> schemaFieldNames )
    {
        return createOperator( operatorId, StatelessOperator.class, schemaFieldNames, emptyList() );
    }

    OperatorDef createPartitionedStatefulOperator ( final String operatorId, final List<String> schemaFieldNames )
    {
        return createOperator( operatorId, PartitionedStatefulOperator.class, schemaFieldNames, schemaFieldNames );
    }

    OperatorDef createOperator ( final String operatorId,
                                 final Class<? extends Operator> operatorClazz,
                                 final List<String> schemaFieldNames,
                                 final List<String> partitionFieldNames )
    {
        final OperatorDefBuilder builder = OperatorDefBuilder.newInstance( operatorId, operatorClazz );
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
        public void invoke ( final InvocationContext invocationContext )
        {

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
        public void invoke ( final InvocationContext invocationContext )
        {

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
        public void invoke ( final InvocationContext invocationContext )
        {

        }

    }

}
