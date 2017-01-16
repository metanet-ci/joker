package cs.bilkent.joker.engine.region.impl;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Before;
import org.junit.Test;

import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.region.RegionDef;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.flow.Port;
import cs.bilkent.joker.operator.InitializationContext;
import cs.bilkent.joker.operator.InvocationContext;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchemaBuilder;
import cs.bilkent.joker.operator.schema.runtime.RuntimeSchemaField;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FlowDefOptimizerImplTest extends AbstractJokerTest
{

    private final IdGenerator idGenerator = new IdGenerator();

    private final RegionDefFormerImpl regionDefFormer = new RegionDefFormerImpl( idGenerator );

    private final FlowDefOptimizerImpl flowOptimizer = new FlowDefOptimizerImpl( new JokerConfig(), idGenerator );

    private final OperatorRuntimeSchemaBuilder schema = new OperatorRuntimeSchemaBuilder( 1, 1 );

    private final List<String> partitionFieldNames = asList( "field1", "field2" );

    @Before
    public void init ()
    {
        schema.addInputField( 0, "field1", Integer.class )
              .addInputField( 0, "field2", Integer.class )
              .addOutputField( 0, "field1", Integer.class )
              .addOutputField( 0, "field2", Integer.class );
    }

    @Test
    public void shouldMergeStatelessAndStatefulRegions ()
    {
        final OperatorDef stateless = OperatorDefBuilder.newInstance( "stateless", StatelessOperator.class ).build();
        final OperatorDef stateful = OperatorDefBuilder.newInstance( "stateful", StatefulOperator.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( stateless ).add( stateful ).connect( "stateless", "stateful" ).build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );

        flowOptimizer.mergeRegions( flow.getOperatorsMap(), flow.getConnections(), regions );

        assertEquals( 1, regions.size() );
        final RegionDef region = regions.get( 0 );
        assertEquals( STATEFUL, region.getRegionType() );
        assertEquals( asList( stateless, stateful ), region.getOperators() );
    }

    @Test
    public void shouldMergeStatefulAndStatelessRegions ()
    {
        final OperatorDef stateful = OperatorDefBuilder.newInstance( "stateful", StatefulOperator.class ).build();
        final OperatorDef stateless = OperatorDefBuilder.newInstance( "stateless", StatelessOperator.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( stateful ).add( stateless ).connect( "stateful", "stateless" ).build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );

        flowOptimizer.mergeRegions( flow.getOperatorsMap(), flow.getConnections(), regions );

        assertEquals( 1, regions.size() );
        final RegionDef region = regions.get( 0 );
        assertEquals( STATEFUL, region.getRegionType() );
        assertEquals( asList( stateful, stateless ), region.getOperators() );
    }

    @Test
    public void shouldMergeStatefulAndStatelessRegionsAfterOptimization ()
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
        final Collection<Entry<Port, Port>> connections = flow.getConnections();
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
    public void shouldMergeMultipleStatefulAndStatelessRegions ()
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

        flowOptimizer.mergeRegions( flow.getOperatorsMap(), flow.getConnections(), regions );

        assertEquals( 1, regions.size() );
        final RegionDef region = regions.get( 0 );
        assertEquals( STATEFUL, region.getRegionType() );
        assertEquals( asList( stateful1, stateless1, stateful2, stateless2, stateful3 ), region.getOperators() );
    }

    @Test
    public void shouldMergeStatefulAndStatefulRegions ()
    {
        final OperatorDef stateful1 = OperatorDefBuilder.newInstance( "stateful1", StatefulOperator.class ).build();
        final OperatorDef stateful2 = OperatorDefBuilder.newInstance( "stateful2", StatefulOperator.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( stateful1 ).add( stateful2 ).connect( "stateful1", "stateful2" ).build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );

        flowOptimizer.mergeRegions( flow.getOperatorsMap(), flow.getConnections(), regions );

        assertEquals( 1, regions.size() );
        final RegionDef region = regions.get( 0 );
        assertEquals( STATEFUL, region.getRegionType() );
        assertEquals( asList( stateful1, stateful2 ), region.getOperators() );
    }

    @Test
    public void shouldMergeStatelessAndStatelessRegions ()
    {
        final OperatorDef stateless1 = OperatorDefBuilder.newInstance( "stateless1", StatelessOperator.class ).build();
        final OperatorDef stateless2 = OperatorDefBuilder.newInstance( "stateless2", StatelessOperator.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( stateless1 ).add( stateless2 ).connect( "stateless1", "stateless2" ).build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );

        flowOptimizer.mergeRegions( flow.getOperatorsMap(), flow.getConnections(), regions );

        assertEquals( 1, regions.size() );
        final RegionDef region = regions.get( 0 );
        assertEquals( STATELESS, region.getRegionType() );
        assertEquals( asList( stateless1, stateless2 ), region.getOperators() );
    }

    @Test
    public void shouldNotMergePartitionedStatefulAndStatelessRegionsWhenPartitionFieldNamesAreNotPresentInStatelessRegion ()
    {
        final OperatorDef partitionedStateful = OperatorDefBuilder.newInstance( "partitionedStateful", PartitionedStatefulOperator.class )
                                                                  .setExtendingSchema( schema )
                                                                  .setPartitionFieldNames( partitionFieldNames )
                                                                  .build();

        final OperatorRuntimeSchemaBuilder statelessSchema = new OperatorRuntimeSchemaBuilder( 1, 1 );

        final RuntimeSchemaField inputField = schema.getInputPortSchemaBuilder( 0 ).getFields().iterator().next();
        final RuntimeSchemaField outputField = schema.getOutputPortSchemaBuilder( 0 ).getFields().iterator().next();

        statelessSchema.addInputField( 0, inputField.getName(), inputField.getType() );
        statelessSchema.addOutputField( 0, outputField.getName(), outputField.getType() );

        final OperatorDef stateless = OperatorDefBuilder.newInstance( "stateless", StatelessOperator.class )
                                                        .setExtendingSchema( statelessSchema )
                                                        .build();

        final FlowDef flow = new FlowDefBuilder().add( partitionedStateful )
                                                 .add( stateless )
                                                 .connect( "partitionedStateful", "stateless" )
                                                 .build();

        final List<RegionDef> regions = new ArrayList<>();
        regions.add( new RegionDef( 0,
                                    PARTITIONED_STATEFUL,
                                    partitionedStateful.partitionFieldNames(),
                                    singletonList( partitionedStateful ) ) );
        regions.add( new RegionDef( 1, STATELESS, emptyList(), singletonList( stateless ) ) );

        flowOptimizer.mergeRegions( flow.getOperatorsMap(), flow.getConnections(), regions );

        assertEquals( 2, regions.size() );
    }

    @Test
    public void shouldMergePartitionedStatefulAndStatelessRegionsAfterOptimization ()
    {
        final OperatorDef partitionedStateful1 = OperatorDefBuilder.newInstance( "partitionedStateful1", PartitionedStatefulOperator.class )
                                                                   .setExtendingSchema( schema )
                                                                   .setPartitionFieldNames( partitionFieldNames )
                                                                   .build();

        final OperatorDef partitionedStateful2 = OperatorDefBuilder.newInstance( "partitionedStateful2", PartitionedStatefulOperator.class )
                                                                   .setExtendingSchema( schema )
                                                                   .setPartitionFieldNames( partitionFieldNames )
                                                                   .build();

        final OperatorDef stateless = OperatorDefBuilder.newInstance( "stateless", StatelessOperator.class )
                                                        .setExtendingSchema( schema )
                                                        .build();

        final FlowDef flow = new FlowDefBuilder().add( partitionedStateful1 )
                                                 .add( partitionedStateful2 )
                                                 .add( stateless )
                                                 .connect( "partitionedStateful1", "stateless" )
                                                 .connect( "partitionedStateful2", "stateless" )
                                                 .build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );
        final Map<String, OperatorDef> operators = flow.getOperatorsMap();
        final Collection<Entry<Port, Port>> connections = flow.getConnections();
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
    public void shouldOptimizeStatelessRegion ()
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
        final Collection<Entry<Port, Port>> connections = flow.getConnections();

        final Map<String, OperatorDef> operators = flow.getOperatorsMap();
        flowOptimizer.duplicateStatelessRegions( operators, connections, regions );

        final List<RegionDef> statelessRegions = regions.stream().filter( r -> r.getRegionType() == STATELESS ).collect( toList() );
        assertEquals( 2, statelessRegions.size() );
        assertStatelessRegions( stateless1, stateless2, stateless3, operators, connections, statelessRegions, false );
    }

    @Test
    public void shouldOptimizeMultipleStatelessRegions ()
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
        final Collection<Entry<Port, Port>> connections = new HashSet<>( flow.getConnections() );

        final Map<String, OperatorDef> operators = flow.getOperatorsMap();
        flowOptimizer.duplicateStatelessRegions( operators, connections, regions );

        final List<RegionDef> statelessRegions = regions.stream().filter( r -> r.getRegionType() == STATELESS ).collect( toList() );
        assertEquals( 4, statelessRegions.size() );
        assertStatelessRegions( stateless1, stateless2, stateless3, operators, connections, statelessRegions, true );
        assertStatelessRegions( stateless4, stateless5, operators, connections, statelessRegions );
    }

    private void assertStatelessRegions ( final OperatorDef stateless1,
                                          final OperatorDef stateless2,
                                          final OperatorDef stateless3,
                                          final Map<String, OperatorDef> optimizedOperators,
                                          final Collection<Entry<Port, Port>> optimizedConnections,
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

            final Collection<Port> stateful1DownstreamConnections = getDownstream( optimizedConnections, new Port( "stateful1", 0 ) );
            final Collection<Port> stateful2DownstreamConnections = getDownstream( optimizedConnections, new Port( "stateful2", 0 ) );
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

            assertTrue( optimizedConnections.contains( new SimpleEntry<>( new Port( optimizedStateless1.id(), 0 ),
                                                                          new Port( optimizedStateless2.id(), 0 ) ) ) );
            assertTrue( optimizedConnections.contains( new SimpleEntry<>( new Port( optimizedStateless2.id(), 0 ),
                                                                          new Port( optimizedStateless3.id(), 0 ) ) ) );
            assertTrue( optimizedConnections.contains( new SimpleEntry<>( new Port( optimizedStateless3.id(), 0 ),
                                                                          new Port( "stateful3", 0 ) ) ) );
            assertTrue( optimizedConnections.contains( new SimpleEntry<>( new Port( optimizedStateless3.id(), 0 ),
                                                                          new Port( "stateful4", 0 ) ) ) );
        }

        assertTrue( stateful1ConnectionExists );
        assertTrue( stateful2ConnectionExists );

        assertFalse( optimizedOperators.containsKey( stateless1.id() ) );
        assertFalse( optimizedOperators.containsKey( stateless2.id() ) );
        assertFalse( optimizedOperators.containsKey( stateless3.id() ) );
    }

    private Collection<Port> getDownstream ( final Collection<Entry<Port, Port>> connections, final Port upstream )
    {
        final Collection<Port> downstream = new ArrayList<>();
        for ( Entry<Port, Port> e : connections )
        {
            if ( e.getKey().equals( upstream ) )
            {
                downstream.add( e.getValue() );
            }
        }

        return downstream;
    }

    private void assertStatelessRegions ( final OperatorDef stateless1,
                                          final OperatorDef stateless2,
                                          final Map<String, OperatorDef> optimizedOperators,
                                          final Collection<Entry<Port, Port>> optimizedConnections,
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

            assertTrue( optimizedConnections.contains( new SimpleEntry<>( new Port( optimizedStateless1.id(), 0 ),
                                                                          new Port( optimizedStateless2.id(), 0 ) ) ) );
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