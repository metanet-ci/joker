package cs.bilkent.zanza.engine.region.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import cs.bilkent.zanza.engine.region.RegionDef;
import cs.bilkent.zanza.flow.FlowDef;
import cs.bilkent.zanza.flow.FlowDefBuilder;
import cs.bilkent.zanza.flow.OperatorDef;
import cs.bilkent.zanza.flow.OperatorDefBuilder;
import cs.bilkent.zanza.flow.OperatorRuntimeSchemaBuilder;
import cs.bilkent.zanza.flow.Port;
import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import cs.bilkent.zanza.operator.spec.OperatorSpec;
import static cs.bilkent.zanza.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.zanza.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.zanza.operator.spec.OperatorType.STATELESS;
import cs.bilkent.zanza.testutils.ZanzaAbstractTest;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FlowOptimizerImplTest extends ZanzaAbstractTest
{

    private final RegionDefFormerImpl regionDefFormer = new RegionDefFormerImpl();

    private final FlowOptimizerImpl flowOptimizer = new FlowOptimizerImpl();

    @Test
    public void test_mergeStatelessAndStatefulRegions ()
    {
        final OperatorDef stateless = OperatorDefBuilder.newInstance( "stateless", StatelessOperator.class ).build();
        final OperatorDef stateful = OperatorDefBuilder.newInstance( "stateful", StatefulOperator.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( stateless ).add( stateful ).connect( "stateless", "stateful" ).build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );

        flowOptimizer.mergeRegions( flow.getOperatorsMap(), flow.getConnectionsMap(), regions );

        assertEquals( 1, regions.size() );
        final RegionDef region = regions.get( 0 );
        assertEquals( STATEFUL, region.getRegionType() );
        assertEquals( asList( stateless, stateful ), region.getOperators() );
    }

    @Test
    public void test_mergeStatefulAndStatelessRegions ()
    {
        final OperatorDef stateful = OperatorDefBuilder.newInstance( "stateful", StatefulOperator.class ).build();
        final OperatorDef stateless = OperatorDefBuilder.newInstance( "stateless", StatelessOperator.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( stateful ).add( stateless ).connect( "stateful", "stateless" ).build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );

        flowOptimizer.mergeRegions( flow.getOperatorsMap(), flow.getConnectionsMap(), regions );

        assertEquals( 1, regions.size() );
        final RegionDef region = regions.get( 0 );
        assertEquals( STATEFUL, region.getRegionType() );
        assertEquals( asList( stateful, stateless ), region.getOperators() );
    }

    @Test
    public void test_mergeStatefulAndStatelessRegions_afterOptimization ()
    {
        final OperatorDef stateful1 = OperatorDefBuilder.newInstance( "stateful1", StatefulOperator.class ).build();
        final OperatorDef stateful2 = OperatorDefBuilder.newInstance( "stateful2", StatefulOperator.class ).build();
        final OperatorDef stateless = OperatorDefBuilder.newInstance( "stateless", StatelessOperator.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( stateful1 )
                                                 .add( stateful2 )
                                                 .add( stateless )
                                                 .connect( "stateful1", "stateless" )
                                                 .connect( "stateful2", "stateless" )
                                                 .build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );

        final Map<String, OperatorDef> operators = flow.getOperatorsMap();
        final Multimap<Port, Port> connections = flow.getConnectionsMap();
        flowOptimizer.duplicateStatelessRegions( operators, connections, regions );
        flowOptimizer.mergeRegions( operators, connections, regions );

        assertEquals( 2, regions.size() );
        final RegionDef region1 = regions.get( 0 );
        assertEquals( STATEFUL, region1.getRegionType() );
        assertEquals( asList( StatefulOperator.class, StatelessOperator.class ),
                      region1.getOperators().stream().map( OperatorDef::operatorClazz ).collect( toList() ) );
        final RegionDef region2 = regions.get( 1 );
        assertEquals( STATEFUL, region2.getRegionType() );
        assertEquals( asList( StatefulOperator.class, StatelessOperator.class ),
                      region2.getOperators().stream().map( OperatorDef::operatorClazz ).collect( toList() ) );
    }

    @Test
    public void test_mergeMultipleStatefulAndStatelessRegions ()
    {
        final OperatorDef stateful1 = OperatorDefBuilder.newInstance( "stateful1", StatefulOperator.class ).build();
        final OperatorDef stateful2 = OperatorDefBuilder.newInstance( "stateful2", StatefulOperator.class ).build();
        final OperatorDef stateful3 = OperatorDefBuilder.newInstance( "stateful3", StatefulOperator.class ).build();
        final OperatorDef stateless1 = OperatorDefBuilder.newInstance( "stateless1", StatelessOperator.class ).build();
        final OperatorDef stateless2 = OperatorDefBuilder.newInstance( "stateless2", StatelessOperator.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( stateful1 )
                                                 .add( stateful2 )
                                                 .add( stateful3 )
                                                 .add( stateless1 )
                                                 .add( stateless2 )
                                                 .connect( "stateful1", "stateless1" )
                                                 .connect( "stateless1", "stateful2" )
                                                 .connect( "stateful2", "stateless2" )
                                                 .connect( "stateless2", "stateful3" )
                                                 .build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );

        flowOptimizer.mergeRegions( flow.getOperatorsMap(), flow.getConnectionsMap(), regions );

        assertEquals( 1, regions.size() );
        final RegionDef region = regions.get( 0 );
        assertEquals( STATEFUL, region.getRegionType() );
        assertEquals( asList( stateful1, stateless1, stateful2, stateless2, stateful3 ), region.getOperators() );
    }

    @Test
    public void test_mergeStatefulAndStatefulRegions ()
    {
        final OperatorDef stateful1 = OperatorDefBuilder.newInstance( "stateful1", StatefulOperator.class ).build();
        final OperatorDef stateful2 = OperatorDefBuilder.newInstance( "stateful2", StatefulOperator.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( stateful1 ).add( stateful2 ).connect( "stateful1", "stateful2" ).build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );

        flowOptimizer.mergeRegions( flow.getOperatorsMap(), flow.getConnectionsMap(), regions );

        assertEquals( 1, regions.size() );
        final RegionDef region = regions.get( 0 );
        assertEquals( STATEFUL, region.getRegionType() );
        assertEquals( asList( stateful1, stateful2 ), region.getOperators() );
    }

    @Test
    public void test_mergeStatelessAndStatelessRegions ()
    {
        final OperatorDef stateless1 = OperatorDefBuilder.newInstance( "stateless1", StatelessOperator.class ).build();
        final OperatorDef stateless2 = OperatorDefBuilder.newInstance( "stateless2", StatelessOperator.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( stateless1 ).add( stateless2 ).connect( "stateless1", "stateless2" ).build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );

        flowOptimizer.mergeRegions( flow.getOperatorsMap(), flow.getConnectionsMap(), regions );

        assertEquals( 1, regions.size() );
        final RegionDef region = regions.get( 0 );
        assertEquals( STATELESS, region.getRegionType() );
        assertEquals( asList( stateless1, stateless2 ), region.getOperators() );
    }

    @Test
    public void test_mergePartitionedStatefulAndStatelessRegions ()
    {
        final OperatorRuntimeSchemaBuilder partitionedStatefulSchema = new OperatorRuntimeSchemaBuilder( 1, 1 );
        partitionedStatefulSchema.addInputField( 0, "field1", Integer.class ).addOutputField( 0, "field1", Integer.class );

        final OperatorDef partitionedStateful = OperatorDefBuilder.newInstance( "partitionedStateful", PartitionedStatefulOperator.class )
                                                                  .setExtendingSchema( partitionedStatefulSchema )
                                                                  .setPartitionFieldNames( singletonList( "field1" ) )
                                                                  .build();

        final OperatorRuntimeSchemaBuilder statelessSchema = new OperatorRuntimeSchemaBuilder( 1, 1 );
        statelessSchema.addInputField( 0, "field1", Integer.class );

        final OperatorDef stateless = OperatorDefBuilder.newInstance( "stateless", StatelessOperator.class )
                                                        .setExtendingSchema( statelessSchema )
                                                        .build();

        final FlowDef flow = new FlowDefBuilder().add( partitionedStateful )
                                                 .add( stateless )
                                                 .connect( "partitionedStateful", "stateless" )
                                                 .build();

        final List<RegionDef> regions = new ArrayList<>();
        regions.add( new RegionDef( PARTITIONED_STATEFUL,
                                    partitionedStateful.partitionFieldNames(),
                                    singletonList( partitionedStateful ) ) );
        regions.add( new RegionDef( STATELESS, emptyList(), singletonList( stateless ) ) );

        flowOptimizer.mergeRegions( flow.getOperatorsMap(), flow.getConnectionsMap(), regions );

        assertEquals( 1, regions.size() );
        final RegionDef region = regions.get( 0 );
        assertEquals( PARTITIONED_STATEFUL, region.getRegionType() );
        assertEquals( asList( partitionedStateful, stateless ), region.getOperators() );
    }

    @Test
    public void test_mergePartitionedStatefulAndStatelessRegions_afterOptimization ()
    {
        final OperatorRuntimeSchemaBuilder partitionedStatefulSchema1 = new OperatorRuntimeSchemaBuilder( 1, 1 );
        partitionedStatefulSchema1.addInputField( 0, "field1", Integer.class ).addOutputField( 0, "field1", Integer.class );

        final OperatorDef partitionedStateful1 = OperatorDefBuilder.newInstance( "partitionedStateful1", PartitionedStatefulOperator.class )
                                                                   .setExtendingSchema( partitionedStatefulSchema1 )
                                                                   .setPartitionFieldNames( singletonList( "field1" ) )
                                                                   .build();

        final OperatorRuntimeSchemaBuilder partitionedStatefulSchema2 = new OperatorRuntimeSchemaBuilder( 1, 1 );
        partitionedStatefulSchema2.addInputField( 0, "field1", Integer.class ).addOutputField( 0, "field1", Integer.class );

        final OperatorDef partitionedStateful2 = OperatorDefBuilder.newInstance( "partitionedStateful2", PartitionedStatefulOperator.class )
                                                                   .setExtendingSchema( partitionedStatefulSchema2 )
                                                                   .setPartitionFieldNames( singletonList( "field1" ) )
                                                                   .build();

        final OperatorRuntimeSchemaBuilder statelessSchema = new OperatorRuntimeSchemaBuilder( 1, 1 );
        statelessSchema.addInputField( 0, "field1", Integer.class );

        final OperatorDef stateless = OperatorDefBuilder.newInstance( "stateless", StatelessOperator.class )
                                                        .setExtendingSchema( statelessSchema )
                                                        .build();

        final FlowDef flow = new FlowDefBuilder().add( partitionedStateful1 )
                                                 .add( partitionedStateful2 )
                                                 .add( stateless )
                                                 .connect( "partitionedStateful1", "stateless" )
                                                 .connect( "partitionedStateful2", "stateless" )
                                                 .build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );
        final Map<String, OperatorDef> operators = flow.getOperatorsMap();
        final Multimap<Port, Port> connections = flow.getConnectionsMap();
        flowOptimizer.duplicateStatelessRegions( operators, connections, regions );
        flowOptimizer.mergeRegions( operators, connections, regions );

        assertEquals( 2, regions.size() );
        final RegionDef region1 = regions.get( 0 );
        assertEquals( PARTITIONED_STATEFUL, region1.getRegionType() );
        assertEquals( asList( PartitionedStatefulOperator.class, StatelessOperator.class ),
                      region1.getOperators().stream().map( OperatorDef::operatorClazz ).collect( toList() ) );
        final RegionDef region2 = regions.get( 1 );
        assertEquals( PARTITIONED_STATEFUL, region2.getRegionType() );
        assertEquals( asList( PartitionedStatefulOperator.class, StatelessOperator.class ),
                      region2.getOperators().stream().map( OperatorDef::operatorClazz ).collect( toList() ) );
    }

    @Test
    public void test_optimizeStatelessRegion ()
    {
        final OperatorDef stateful1 = OperatorDefBuilder.newInstance( "stateful1", StatefulOperator.class ).build();
        final OperatorDef stateful2 = OperatorDefBuilder.newInstance( "stateful2", StatefulOperator.class ).build();
        final OperatorDef stateless1 = OperatorDefBuilder.newInstance( "stateless1", StatelessOperator.class ).build();
        final OperatorDef stateless2 = OperatorDefBuilder.newInstance( "stateless2", StatelessOperator.class ).build();
        final OperatorDef stateless3 = OperatorDefBuilder.newInstance( "stateless3", StatelessOperator.class ).build();
        final OperatorDef stateful3 = OperatorDefBuilder.newInstance( "stateful3", StatefulOperator.class ).build();
        final OperatorDef stateful4 = OperatorDefBuilder.newInstance( "stateful4", StatefulOperator.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( stateful1 )
                                                 .add( stateful2 )
                                                 .add( stateless1 )
                                                 .add( stateless2 )
                                                 .add( stateless3 )
                                                 .add( stateful3 )
                                                 .add( stateful4 )
                                                 .connect( "stateful1", "stateless1" )
                                                 .connect( "stateful2", "stateless1" )
                                                 .connect( "stateless1", "stateless2" )
                                                 .connect( "stateless2", "stateless3" )
                                                 .connect( "stateless3", "stateful3" )
                                                 .connect( "stateless3", "stateful4" )
                                                 .build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );
        final Multimap<Port, Port> connections = flow.getConnectionsMap();

        final Map<String, OperatorDef> operators = flow.getOperatorsMap();
        flowOptimizer.duplicateStatelessRegions( operators, connections, regions );

        final List<RegionDef> statelessRegions = regions.stream().filter( r -> r.getRegionType() == STATELESS ).collect( toList() );
        assertEquals( 2, statelessRegions.size() );
        assertStatelessRegions( stateless1, stateless2, stateless3, operators, connections, statelessRegions, false );
    }

    @Test
    public void test_optimizeMultipleStatelessRegions ()
    {
        final OperatorDef stateful1 = OperatorDefBuilder.newInstance( "stateful1", StatefulOperator.class ).build();
        final OperatorDef stateful2 = OperatorDefBuilder.newInstance( "stateful2", StatefulOperator.class ).build();
        final OperatorDef stateless1 = OperatorDefBuilder.newInstance( "stateless1", StatelessOperator.class ).build();
        final OperatorDef stateless2 = OperatorDefBuilder.newInstance( "stateless2", StatelessOperator.class ).build();
        final OperatorDef stateless3 = OperatorDefBuilder.newInstance( "stateless3", StatelessOperator.class ).build();
        final OperatorDef stateful3 = OperatorDefBuilder.newInstance( "stateful3", StatefulOperator.class ).build();
        final OperatorDef stateful4 = OperatorDefBuilder.newInstance( "stateful4", StatefulOperator.class ).build();
        final OperatorDef stateless4 = OperatorDefBuilder.newInstance( "stateless4", StatelessOperator2.class ).build();
        final OperatorDef stateless5 = OperatorDefBuilder.newInstance( "stateless5", StatelessOperator2.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( stateful1 )
                                                 .add( stateful2 )
                                                 .add( stateless1 )
                                                 .add( stateless2 )
                                                 .add( stateless3 )
                                                 .add( stateful3 )
                                                 .add( stateful4 )
                                                 .add( stateless4 )
                                                 .add( stateless5 )
                                                 .connect( "stateful1", "stateless1" )
                                                 .connect( "stateful2", "stateless1" )
                                                 .connect( "stateless1", "stateless2" )
                                                 .connect( "stateless2", "stateless3" )
                                                 .connect( "stateless3", "stateful3" )
                                                 .connect( "stateless3", "stateful4" )
                                                 .connect( "stateless3", "stateless4" )
                                                 .connect( "stateless4", "stateless5" )
                                                 .build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );
        final Multimap<Port, Port> connectionsMap = HashMultimap.create();

        for ( Map.Entry<Port, Port> e : flow.getAllConnections() )
        {
            connectionsMap.put( e.getKey(), e.getValue() );
        }

        final Map<String, OperatorDef> operators = flow.getOperatorsMap();
        flowOptimizer.duplicateStatelessRegions( operators, connectionsMap, regions );

        final List<RegionDef> statelessRegions = regions.stream().filter( r -> r.getRegionType() == STATELESS ).collect( toList() );
        assertEquals( 4, statelessRegions.size() );
        assertStatelessRegions( stateless1, stateless2, stateless3, operators, connectionsMap, statelessRegions, true );
        assertStatelessRegions( stateless4, stateless5, operators, connectionsMap, statelessRegions );
    }

    private void assertStatelessRegions ( final OperatorDef stateless1,
                                          final OperatorDef stateless2,
                                          final OperatorDef stateless3,
                                          final Map<String, OperatorDef> optimizedOperators,
                                          final Multimap<Port, Port> optimizedConnections,
                                          final List<RegionDef> statelessRegions,
                                          final boolean expectedNonMatchingStatelessRegion )
    {
        boolean stateful1ConnectionExists = false, stateful2ConnectionExists = false;
        for ( RegionDef region : statelessRegions )
        {
            if ( region.getOperatorCount() != 3 )
            {
                assertTrue( expectedNonMatchingStatelessRegion );
                continue;
            }

            final List<OperatorDef> operators = region.getOperators();

            final OperatorDef optimizedStateless1 = operators.get( 0 );
            final OperatorDef optimizedStateless2 = operators.get( 1 );
            final OperatorDef optimizedStateless3 = operators.get( 2 );

            assertEquals( StatelessOperator.class, optimizedStateless1.operatorClazz() );
            assertEquals( StatelessOperator.class, optimizedStateless2.operatorClazz() );
            assertEquals( StatelessOperator.class, optimizedStateless3.operatorClazz() );

            assertTrue( optimizedOperators.containsKey( optimizedStateless1.id() ) );
            assertTrue( optimizedOperators.containsKey( optimizedStateless2.id() ) );
            assertTrue( optimizedOperators.containsKey( optimizedStateless3.id() ) );

            final Collection<Port> stateful1DownstreamConnections = optimizedConnections.get( new Port( "stateful1", 0 ) );
            final Collection<Port> stateful2DownstreamConnections = optimizedConnections.get( new Port( "stateful2", 0 ) );
            assertEquals( 1, stateful1DownstreamConnections.size() );
            assertEquals( 1, stateful2DownstreamConnections.size() );
            final Port stateful1DownstreamPort = stateful1DownstreamConnections.iterator().next();
            if ( stateful1DownstreamPort.operatorId.equals( optimizedStateless1.id() ) )
            {
                stateful1ConnectionExists = true;
            }
            else
            {
                final Port stateful2DownstreamPort = stateful2DownstreamConnections.iterator().next();
                if ( stateful2DownstreamPort.operatorId.equals( optimizedStateless1.id() ) )
                {
                    stateful2ConnectionExists = true;
                }
            }

            assertTrue( optimizedConnections.containsEntry( new Port( optimizedStateless1.id(), 0 ),
                                                            new Port( optimizedStateless2.id(), 0 ) ) );
            assertTrue( optimizedConnections.containsEntry( new Port( optimizedStateless2.id(), 0 ),
                                                            new Port( optimizedStateless3.id(), 0 ) ) );
            assertTrue( optimizedConnections.containsEntry( new Port( optimizedStateless3.id(), 0 ), new Port( "stateful3", 0 ) ) );
            assertTrue( optimizedConnections.containsEntry( new Port( optimizedStateless3.id(), 0 ), new Port( "stateful4", 0 ) ) );
        }

        assertTrue( stateful1ConnectionExists );
        assertTrue( stateful2ConnectionExists );

        assertFalse( optimizedOperators.containsKey( stateless1.id() ) );
        assertFalse( optimizedOperators.containsKey( stateless2.id() ) );
        assertFalse( optimizedOperators.containsKey( stateless3.id() ) );
    }

    private void assertStatelessRegions ( final OperatorDef stateless1,
                                          final OperatorDef stateless2,
                                          final Map<String, OperatorDef> optimizedOperators,
                                          final Multimap<Port, Port> optimizedConnections,
                                          final List<RegionDef> statelessRegions )
    {
        for ( RegionDef region : statelessRegions )
        {
            if ( region.getOperatorCount() != 2 )
            {
                continue;
            }

            final List<OperatorDef> operators = region.getOperators();

            final OperatorDef optimizedStateless1 = operators.get( 0 );
            final OperatorDef optimizedStateless2 = operators.get( 1 );

            assertEquals( StatelessOperator2.class, optimizedStateless1.operatorClazz() );
            assertEquals( StatelessOperator2.class, optimizedStateless2.operatorClazz() );

            assertTrue( optimizedOperators.containsKey( optimizedStateless1.id() ) );
            assertTrue( optimizedOperators.containsKey( optimizedStateless2.id() ) );

            assertTrue( optimizedConnections.containsEntry( new Port( optimizedStateless1.id(), 0 ),
                                                            new Port( optimizedStateless2.id(), 0 ) ) );
        }

        assertFalse( optimizedOperators.containsKey( stateless1.id() ) );
        assertFalse( optimizedOperators.containsKey( stateless2.id() ) );
    }

    @OperatorSpec( inputPortCount = 1, outputPortCount = 1, type = STATELESS )
    private static class StatelessOperator2 implements Operator
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


    @OperatorSpec( inputPortCount = 1, outputPortCount = 1, type = STATELESS )
    private static class StatelessOperator implements Operator
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


    @OperatorSpec( inputPortCount = 1, outputPortCount = 1, type = STATEFUL )
    private static class StatefulOperator implements Operator
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


    @OperatorSpec( inputPortCount = 1, outputPortCount = 1, type = PARTITIONED_STATEFUL )
    private static class PartitionedStatefulOperator implements Operator
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
