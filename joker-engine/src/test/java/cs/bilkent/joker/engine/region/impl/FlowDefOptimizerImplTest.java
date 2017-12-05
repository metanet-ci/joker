package cs.bilkent.joker.engine.region.impl;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.flow.RegionDef;
import static cs.bilkent.joker.engine.region.impl.FlowDefOptimizerImpl.toDuplicateOperatorId;
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class FlowDefOptimizerImplTest extends AbstractJokerTest
{

    private final IdGenerator idGenerator = new IdGenerator();

    private final RegionDefFormerImpl regionDefFormer = new RegionDefFormerImpl( idGenerator );

    private final FlowDefOptimizerImpl flowOptimizer = new FlowDefOptimizerImpl( new JokerConfig(), idGenerator );

    private final OperatorRuntimeSchemaBuilder schema0 = new OperatorRuntimeSchemaBuilder( 0, 1 );

    private final OperatorRuntimeSchemaBuilder schema1 = new OperatorRuntimeSchemaBuilder( 1, 1 );

    private final List<String> partitionFieldNames = asList( "field1", "field2" );

    @Before
    public void init ()
    {
        schema0.addOutputField( 0, "field1", Integer.class ).addOutputField( 0, "field2", Integer.class );

        schema1.addInputField( 0, "field1", Integer.class )
               .addInputField( 0, "field2", Integer.class )
               .addOutputField( 0, "field1", Integer.class )
               .addOutputField( 0, "field2", Integer.class );
    }

    @Test
    public void shouldReassignRegionIdsBasedOnTopologicalSort ()
    {
        final OperatorDef stateful1 = OperatorDefBuilder.newInstance( "stateful1", StatefulOperator0.class ).build();
        final OperatorDef stateful2 = OperatorDefBuilder.newInstance( "stateful2", StatefulOperator0.class ).build();
        final OperatorDef stateful3 = OperatorDefBuilder.newInstance( "stateful3", StatefulOperator0.class ).build();
        final OperatorDef stateful4 = OperatorDefBuilder.newInstance( "stateful4", StatefulOperator2.class ).build();
        final OperatorDef stateful5 = OperatorDefBuilder.newInstance( "stateful5", StatefulOperator2.class ).build();
        final OperatorDef stateful6 = OperatorDefBuilder.newInstance( "stateful6", StatefulOperator2.class ).build();

        final FlowDef flowDef = new FlowDefBuilder().add( stateful1 )
                                                    .add( stateful2 )
                                                    .add( stateful3 )
                                                    .add( stateful4 )
                                                    .add( stateful5 )
                                                    .add( stateful6 )
                                                    .connect( "stateful1", "stateful4" )
                                                    .connect( "stateful2", "stateful4" )
                                                    .connect( "stateful4", "stateful5" )
                                                    .connect( "stateful3", "stateful5" )
                                                    .connect( "stateful5", "stateful6" )
                                                    .build();

        final List<RegionDef> regions = new ArrayList<>();
        regions.add( new RegionDef( 6, STATEFUL, emptyList(), singletonList( stateful1 ) ) );
        regions.add( new RegionDef( 5, STATEFUL, emptyList(), singletonList( stateful2 ) ) );
        regions.add( new RegionDef( 4, STATEFUL, emptyList(), singletonList( stateful3 ) ) );
        regions.add( new RegionDef( 3, STATEFUL, emptyList(), singletonList( stateful4 ) ) );
        regions.add( new RegionDef( 2, STATEFUL, emptyList(), singletonList( stateful5 ) ) );
        regions.add( new RegionDef( 1, STATEFUL, emptyList(), singletonList( stateful6 ) ) );

        final List<RegionDef> sortedRegions = flowOptimizer.reassignRegionIds( flowDef.getOperatorsMap(),
                                                                               flowDef.getConnections(),
                                                                               regions );

        assertEquals( new RegionDef( 1, STATEFUL, emptyList(), singletonList( stateful1 ) ), sortedRegions.get( 0 ) );
        assertEquals( new RegionDef( 2, STATEFUL, emptyList(), singletonList( stateful2 ) ), sortedRegions.get( 1 ) );
        assertEquals( new RegionDef( 3, STATEFUL, emptyList(), singletonList( stateful3 ) ), sortedRegions.get( 2 ) );
        assertEquals( new RegionDef( 4, STATEFUL, emptyList(), singletonList( stateful4 ) ), sortedRegions.get( 3 ) );
        assertEquals( new RegionDef( 5, STATEFUL, emptyList(), singletonList( stateful5 ) ), sortedRegions.get( 4 ) );
        assertEquals( new RegionDef( 6, STATEFUL, emptyList(), singletonList( stateful6 ) ), sortedRegions.get( 5 ) );
    }

    @Test
    public void shouldMergeStatelessAndStatefulRegions ()
    {
        final OperatorDef stateless0 = OperatorDefBuilder.newInstance( "stateless0", StatelessOperator0.class ).build();
        final OperatorDef stateless1 = OperatorDefBuilder.newInstance( "stateless1", StatelessOperator1.class ).build();
        final OperatorDef stateful = OperatorDefBuilder.newInstance( "stateful", StatefulOperator2.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( stateless0 )
                                                 .add( stateless1 )
                                                 .add( stateful )
                                                 .connect( "stateless0", "stateless1" )
                                                 .connect( "stateless1", "stateful" )
                                                 .build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );

        flowOptimizer.mergeRegions( flow.getOperatorsMap(), flow.getConnections(), regions );

        assertEquals( 2, regions.size() );
        final RegionDef region = regions.get( 1 );
        assertEquals( STATEFUL, region.getRegionType() );
        assertEquals( asList( stateless1, stateful ), region.getOperators() );
    }

    @Test
    public void shouldMergeStatefulAndStatelessRegions ()
    {
        final OperatorDef stateful0 = OperatorDefBuilder.newInstance( "stateful0", StatefulOperator0.class ).build();
        final OperatorDef stateful1 = OperatorDefBuilder.newInstance( "stateful1", StatefulOperator2.class ).build();
        final OperatorDef stateless = OperatorDefBuilder.newInstance( "stateless", StatelessOperator1.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( stateful0 )
                                                 .add( stateful1 )
                                                 .add( stateless )
                                                 .connect( "stateful0", "stateful1" )
                                                 .connect( "stateful1", "stateless" )
                                                 .build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );

        flowOptimizer.mergeRegions( flow.getOperatorsMap(), flow.getConnections(), regions );

        assertEquals( 2, regions.size() );
        final RegionDef region = regions.get( 1 );
        assertEquals( STATEFUL, region.getRegionType() );
        assertEquals( asList( stateful1, stateless ), region.getOperators() );
    }

    @Test
    public void shouldMergeStatefulAndStatelessRegionsAfterDuplicationForUpstream ()
    {
        final OperatorDef stateful0 = OperatorDefBuilder.newInstance( "stateful0", StatefulOperator0.class ).build();
        final OperatorDef stateful1 = OperatorDefBuilder.newInstance( "stateful1", StatefulOperator2.class ).build();
        final OperatorDef stateful2 = OperatorDefBuilder.newInstance( "stateful2", StatefulOperator2.class ).build();
        final OperatorDef stateless = OperatorDefBuilder.newInstance( "stateless", StatelessOperator1.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( stateful0 )
                                                 .add( stateful1 )
                                                 .add( stateful2 )
                                                 .add( stateless )
                                                 .connect( "stateful0", "stateful1" )
                                                 .connect( "stateful0", "stateful2" )
                                                 .connect( "stateful1", "stateless" )
                                                 .connect( "stateful2", "stateless" )
                                                 .build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );
        final Map<String, OperatorDef> operators = flow.getOperatorsMap();
        final Set<Entry<Port, Port>> connections = flow.getConnections();

        flowOptimizer.duplicateStatelessRegions( operators, connections, regions );
        flowOptimizer.mergeRegions( operators, connections, regions );

        assertEquals( 3, regions.size() );
        final RegionDef region1 = regions.get( 1 );
        assertEquals( STATEFUL, region1.getRegionType() );
        assertEquals( asList( StatefulOperator2.class, StatelessOperator1.class ),
                      region1.getOperators().stream().map( OperatorDef::getOperatorClazz ).collect( toList() ) );
        final RegionDef region2 = regions.get( 2 );
        assertEquals( STATEFUL, region2.getRegionType() );
        assertEquals( asList( StatefulOperator2.class, StatelessOperator1.class ),
                      region2.getOperators().stream().map( OperatorDef::getOperatorClazz ).collect( toList() ) );
    }

    @Test
    public void shouldMergeStatefulAndStatelessRegionsAfterDownstream ()
    {
        final OperatorDef stateful0 = OperatorDefBuilder.newInstance( "stateful0", StatefulOperator0.class ).build();
        final OperatorDef stateful1 = OperatorDefBuilder.newInstance( "stateful1", StatefulOperator2.class ).build();
        final OperatorDef stateful2 = OperatorDefBuilder.newInstance( "stateful2", StatefulOperator2.class ).build();
        final OperatorDef stateless = OperatorDefBuilder.newInstance( "stateless", StatelessOperator1.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( stateful0 )
                                                 .add( stateful1 )
                                                 .add( stateful2 )
                                                 .add( stateless )
                                                 .connect( "stateful0", "stateless" )
                                                 .connect( "stateless", "stateful1" )
                                                 .connect( "stateless", "stateful2" )
                                                 .build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );
        final Map<String, OperatorDef> operators = flow.getOperatorsMap();
        final Set<Entry<Port, Port>> connections = flow.getConnections();

        flowOptimizer.duplicateStatelessRegions( operators, connections, regions );
        flowOptimizer.mergeRegions( operators, connections, regions );

        assertEquals( 3, regions.size() );
        final RegionDef region1 = regions.get( 1 );
        assertEquals( STATEFUL, region1.getRegionType() );
        assertEquals( asList( StatelessOperator1.class, StatefulOperator2.class ),
                      region1.getOperators().stream().map( OperatorDef::getOperatorClazz ).collect( toList() ) );
        final RegionDef region2 = regions.get( 2 );
        assertEquals( STATEFUL, region2.getRegionType() );
        assertEquals( asList( StatelessOperator1.class, StatefulOperator2.class ),
                      region2.getOperators().stream().map( OperatorDef::getOperatorClazz ).collect( toList() ) );
    }

    @Test
    public void shouldMergeMultipleStatefulAndStatelessRegions ()
    {
        final OperatorDef stateful0 = OperatorDefBuilder.newInstance( "stateful0", StatefulOperator0.class ).build();
        final OperatorDef stateful1 = OperatorDefBuilder.newInstance( "stateful1", StatefulOperator2.class ).build();
        final OperatorDef stateful2 = OperatorDefBuilder.newInstance( "stateful2", StatefulOperator2.class ).build();
        final OperatorDef stateful3 = OperatorDefBuilder.newInstance( "stateful3", StatefulOperator2.class ).build();
        final OperatorDef stateless1 = OperatorDefBuilder.newInstance( "stateless1", StatelessOperator1.class ).build();
        final OperatorDef stateless2 = OperatorDefBuilder.newInstance( "stateless2", StatelessOperator1.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( stateful0 )
                                                 .add( stateful1 )
                                                 .add( stateful2 )
                                                 .add( stateful3 )
                                                 .add( stateless1 )
                                                 .add( stateless2 )
                                                 .connect( "stateful0", "stateful1" )
                                                 .connect( "stateful1", "stateless1" )
                                                 .connect( "stateless1", "stateful2" )
                                                 .connect( "stateful2", "stateless2" )
                                                 .connect( "stateless2", "stateful3" )
                                                 .build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );

        flowOptimizer.mergeRegions( flow.getOperatorsMap(), flow.getConnections(), regions );

        assertEquals( 2, regions.size() );
        final RegionDef region = regions.get( 1 );
        assertEquals( STATEFUL, region.getRegionType() );
        assertEquals( asList( stateful1, stateless1, stateful2, stateless2, stateful3 ), region.getOperators() );
    }

    @Test
    public void shouldMergeStatefulRegions ()
    {
        final OperatorDef stateful0 = OperatorDefBuilder.newInstance( "stateful0", StatefulOperator0.class ).build();
        final OperatorDef stateful1 = OperatorDefBuilder.newInstance( "stateful1", StatefulOperator2.class ).build();
        final OperatorDef stateful2 = OperatorDefBuilder.newInstance( "stateful2", StatefulOperator2.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( stateful0 )
                                                 .add( stateful1 )
                                                 .add( stateful2 )
                                                 .connect( "stateful0", "stateful1" )
                                                 .connect( "stateful1", "stateful2" )
                                                 .build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );

        flowOptimizer.mergeRegions( flow.getOperatorsMap(), flow.getConnections(), regions );

        assertEquals( 2, regions.size() );
        final RegionDef region = regions.get( 1 );
        assertEquals( STATEFUL, region.getRegionType() );
        assertEquals( asList( stateful1, stateful2 ), region.getOperators() );
    }

    @Test
    public void shouldMergeStatelessRegions ()
    {
        final OperatorDef stateful = OperatorDefBuilder.newInstance( "stateful", StatefulOperator0.class ).build();
        final OperatorDef stateless1 = OperatorDefBuilder.newInstance( "stateless1", StatelessOperator1.class ).build();
        final OperatorDef stateless2 = OperatorDefBuilder.newInstance( "stateless2", StatelessOperator1.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( stateful )
                                                 .add( stateless1 )
                                                 .add( stateless2 )
                                                 .connect( "stateful", "stateless1" )
                                                 .connect( "stateless1", "stateless2" )
                                                 .build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );

        flowOptimizer.mergeRegions( flow.getOperatorsMap(), flow.getConnections(), regions );

        assertEquals( 2, regions.size() );
        final RegionDef region = regions.get( 1 );
        assertEquals( STATELESS, region.getRegionType() );
        assertEquals( asList( stateless1, stateless2 ), region.getOperators() );
    }

    @Test
    public void shouldNotMergePartitionedStatefulAndStatelessRegionsWhenPartitionFieldNamesAreNotPresentInStatelessRegion ()
    {
        final OperatorDef stateful = OperatorDefBuilder.newInstance( "stateful", StatefulOperator0.class )
                                                       .setExtendingSchema( schema0 )
                                                       .build();

        final OperatorDef partitionedStateful = OperatorDefBuilder.newInstance( "partitionedStateful", PartitionedStatefulOperator.class )
                                                                  .setExtendingSchema( schema1 )
                                                                  .setPartitionFieldNames( partitionFieldNames )
                                                                  .build();

        final OperatorRuntimeSchemaBuilder statelessSchema = new OperatorRuntimeSchemaBuilder( 1, 1 );

        final RuntimeSchemaField inputField = schema1.getInputPortSchemaBuilder( 0 ).getFields().iterator().next();
        final RuntimeSchemaField outputField = schema1.getOutputPortSchemaBuilder( 0 ).getFields().iterator().next();

        statelessSchema.addInputField( 0, inputField.getName(), inputField.getType() );
        statelessSchema.addOutputField( 0, outputField.getName(), outputField.getType() );

        final OperatorDef stateless = OperatorDefBuilder.newInstance( "stateless", StatelessOperator1.class )
                                                        .setExtendingSchema( statelessSchema )
                                                        .build();

        final FlowDef flow = new FlowDefBuilder().add( stateful )
                                                 .add( partitionedStateful )
                                                 .add( stateless )
                                                 .connect( "stateful", "partitionedStateful" )
                                                 .connect( "partitionedStateful", "stateless" )
                                                 .build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );

        flowOptimizer.mergeRegions( flow.getOperatorsMap(), flow.getConnections(), regions );

        assertEquals( 3, regions.size() );
    }

    @Test
    public void shouldMergePartitionedStatefulAndStatelessRegionsAfterDuplication ()
    {
        final OperatorDef stateful = OperatorDefBuilder.newInstance( "stateful", StatefulOperator0.class )
                                                       .setExtendingSchema( schema0 )
                                                       .build();

        final OperatorDef partitionedStateful1 = OperatorDefBuilder.newInstance( "partitionedStateful1", PartitionedStatefulOperator.class )
                                                                   .setExtendingSchema( schema1 )
                                                                   .setPartitionFieldNames( partitionFieldNames )
                                                                   .build();

        final OperatorDef partitionedStateful2 = OperatorDefBuilder.newInstance( "partitionedStateful2", PartitionedStatefulOperator.class )
                                                                   .setExtendingSchema( schema1 )
                                                                   .setPartitionFieldNames( partitionFieldNames )
                                                                   .build();

        final OperatorDef stateless = OperatorDefBuilder.newInstance( "stateless", StatelessOperator1.class ).setExtendingSchema( schema1 )
                                                        .build();

        final FlowDef flow = new FlowDefBuilder().add( stateful )
                                                 .add( partitionedStateful1 )
                                                 .add( partitionedStateful2 )
                                                 .add( stateless )
                                                 .connect( "stateful", "partitionedStateful1" )
                                                 .connect( "stateful", "partitionedStateful2" )
                                                 .connect( "partitionedStateful1", "stateless" )
                                                 .connect( "partitionedStateful2", "stateless" )
                                                 .build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );
        final Map<String, OperatorDef> operators = flow.getOperatorsMap();
        final Set<Entry<Port, Port>> connections = flow.getConnections();

        flowOptimizer.duplicateStatelessRegions( operators, connections, regions );
        flowOptimizer.mergeRegions( operators, connections, regions );

        assertEquals( 3, regions.size() );
        final RegionDef region1 = regions.get( 1 );
        assertEquals( PARTITIONED_STATEFUL, region1.getRegionType() );
        assertEquals( asList( PartitionedStatefulOperator.class, StatelessOperator1.class ),
                      region1.getOperators().stream().map( OperatorDef::getOperatorClazz ).collect( toList() ) );
        final RegionDef region2 = regions.get( 2 );
        assertEquals( PARTITIONED_STATEFUL, region2.getRegionType() );
        assertEquals( asList( PartitionedStatefulOperator.class, StatelessOperator1.class ),
                      region2.getOperators().stream().map( OperatorDef::getOperatorClazz ).collect( toList() ) );
    }

    @Test
    public void shouldDuplicateStatelessRegionWithMultipleUpstreamRegions ()
    {
        final OperatorDef stateful1 = OperatorDefBuilder.newInstance( "stateful1", StatefulOperator0.class ).build();
        final OperatorDef stateful2 = OperatorDefBuilder.newInstance( "stateful2", StatefulOperator0.class ).build();
        final OperatorDef stateless1 = OperatorDefBuilder.newInstance( "stateless1", StatelessOperator1.class ).build();
        final OperatorDef stateless2 = OperatorDefBuilder.newInstance( "stateless2", StatelessOperator1.class ).build();
        final OperatorDef stateless3 = OperatorDefBuilder.newInstance( "stateless3", StatelessOperator1.class ).build();
        final OperatorDef stateful3 = OperatorDefBuilder.newInstance( "stateful3", StatefulOperator2.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( stateful1 )
                                                 .add( stateful2 )
                                                 .add( stateless1 )
                                                 .add( stateless2 )
                                                 .add( stateless3 )
                                                 .add( stateful3 )
                                                 .connect( "stateful1", "stateless1" )
                                                 .connect( "stateful2", "stateless1" )
                                                 .connect( "stateless1", "stateless2" )
                                                 .connect( "stateless2", "stateless3" )
                                                 .connect( "stateless3", "stateful3", 0 )
                                                 .connect( "stateless3", "stateful3", 1 )
                                                 .build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );
        final Set<Entry<Port, Port>> connections = flow.getConnections();
        final Map<String, OperatorDef> operators = flow.getOperatorsMap();

        flowOptimizer.duplicateStatelessRegions( operators, connections, regions );

        assertNotNull( operators.get( toDuplicateOperatorId( stateless1, 0 ) ) );
        assertNotNull( operators.get( toDuplicateOperatorId( stateless1, 1 ) ) );
        assertNotNull( operators.get( toDuplicateOperatorId( stateless2, 0 ) ) );
        assertNotNull( operators.get( toDuplicateOperatorId( stateless2, 1 ) ) );
        assertNotNull( operators.get( toDuplicateOperatorId( stateless3, 0 ) ) );
        assertNotNull( operators.get( toDuplicateOperatorId( stateless3, 1 ) ) );
        assertTrue( connections.contains( new SimpleEntry<>( new Port( stateful1.getId(), 0 ),
                                                             new Port( toDuplicateOperatorId( stateless1, 0 ), 0 ) ) ) );
        assertTrue( connections.contains( new SimpleEntry<>( new Port( stateful2.getId(), 0 ),
                                                             new Port( toDuplicateOperatorId( stateless1, 1 ), 0 ) ) ) );
        assertTrue( connections.contains( new SimpleEntry<>( new Port( toDuplicateOperatorId( stateless3, 0 ), 0 ),
                                                             new Port( stateful3.getId(), 0 ) ) ) );
        assertTrue( connections.contains( new SimpleEntry<>( new Port( toDuplicateOperatorId( stateless3, 0 ), 0 ),
                                                             new Port( stateful3.getId(), 1 ) ) ) );
        assertTrue( connections.contains( new SimpleEntry<>( new Port( toDuplicateOperatorId( stateless3, 1 ), 0 ),
                                                             new Port( stateful3.getId(), 0 ) ) ) );
        assertTrue( connections.contains( new SimpleEntry<>( new Port( toDuplicateOperatorId( stateless3, 1 ), 0 ),
                                                             new Port( stateful3.getId(), 1 ) ) ) );

        assertTrue( connections.contains( new SimpleEntry<>( new Port( toDuplicateOperatorId( stateless1, 0 ), 0 ),
                                                             new Port( toDuplicateOperatorId( stateless2, 0 ), 0 ) ) ) );
        assertTrue( connections.contains( new SimpleEntry<>( new Port( toDuplicateOperatorId( stateless2, 0 ), 0 ),
                                                             new Port( toDuplicateOperatorId( stateless3, 0 ), 0 ) ) ) );
        assertTrue( connections.contains( new SimpleEntry<>( new Port( toDuplicateOperatorId( stateless1, 1 ), 0 ),
                                                             new Port( toDuplicateOperatorId( stateless2, 1 ), 0 ) ) ) );
        assertTrue( connections.contains( new SimpleEntry<>( new Port( toDuplicateOperatorId( stateless2, 1 ), 0 ),
                                                             new Port( toDuplicateOperatorId( stateless3, 1 ), 0 ) ) ) );
    }

    @Test
    public void shouldDuplicateStatelessRegionWithMultipleDownstreamRegions ()
    {
        final OperatorDef stateful1 = OperatorDefBuilder.newInstance( "stateful1", StatefulOperator0.class ).build();
        final OperatorDef stateless1 = OperatorDefBuilder.newInstance( "stateless1", StatelessOperator1.class ).build();
        final OperatorDef stateless2 = OperatorDefBuilder.newInstance( "stateless2", StatelessOperator1.class ).build();
        final OperatorDef stateless3 = OperatorDefBuilder.newInstance( "stateless3", StatelessOperator1.class ).build();
        final OperatorDef stateful2 = OperatorDefBuilder.newInstance( "stateful2", StatefulOperator2.class ).build();
        final OperatorDef stateful3 = OperatorDefBuilder.newInstance( "stateful3", StatefulOperator2.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( stateful1 )
                                                 .add( stateless1 )
                                                 .add( stateless2 )
                                                 .add( stateless3 )
                                                 .add( stateful2 )
                                                 .add( stateful3 )
                                                 .connect( "stateful1", 0, "stateless1" )
                                                 .connect( "stateful1", 1, "stateless1" )
                                                 .connect( "stateless1", "stateless2" )
                                                 .connect( "stateless2", "stateless3" )
                                                 .connect( "stateless3", "stateful2" )
                                                 .connect( "stateless3", "stateful3" )
                                                 .build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );
        final Set<Entry<Port, Port>> connections = flow.getConnections();
        final Map<String, OperatorDef> operators = flow.getOperatorsMap();

        flowOptimizer.duplicateStatelessRegions( operators, connections, regions );

        assertNotNull( operators.get( toDuplicateOperatorId( stateless1, 0 ) ) );
        assertNotNull( operators.get( toDuplicateOperatorId( stateless1, 1 ) ) );
        assertNotNull( operators.get( toDuplicateOperatorId( stateless2, 0 ) ) );
        assertNotNull( operators.get( toDuplicateOperatorId( stateless2, 1 ) ) );
        assertNotNull( operators.get( toDuplicateOperatorId( stateless3, 0 ) ) );
        assertNotNull( operators.get( toDuplicateOperatorId( stateless3, 1 ) ) );
        assertTrue( connections.contains( new SimpleEntry<>( new Port( stateful1.getId(), 0 ),
                                                             new Port( toDuplicateOperatorId( stateless1, 0 ), 0 ) ) ) );
        assertTrue( connections.contains( new SimpleEntry<>( new Port( stateful1.getId(), 1 ),
                                                             new Port( toDuplicateOperatorId( stateless1, 0 ), 0 ) ) ) );
        assertTrue( connections.contains( new SimpleEntry<>( new Port( toDuplicateOperatorId( stateless3, 0 ), 0 ),
                                                             new Port( stateful2.getId(), 0 ) ) ) );
        assertTrue( connections.contains( new SimpleEntry<>( new Port( toDuplicateOperatorId( stateless3, 1 ), 0 ),
                                                             new Port( stateful3.getId(), 0 ) ) ) );
        assertTrue( connections.contains( new SimpleEntry<>( new Port( toDuplicateOperatorId( stateless1, 0 ), 0 ),
                                                             new Port( toDuplicateOperatorId( stateless2, 0 ), 0 ) ) ) );
        assertTrue( connections.contains( new SimpleEntry<>( new Port( toDuplicateOperatorId( stateless2, 0 ), 0 ),
                                                             new Port( toDuplicateOperatorId( stateless3, 0 ), 0 ) ) ) );
        assertTrue( connections.contains( new SimpleEntry<>( new Port( toDuplicateOperatorId( stateless1, 1 ), 0 ),
                                                             new Port( toDuplicateOperatorId( stateless2, 1 ), 0 ) ) ) );
        assertTrue( connections.contains( new SimpleEntry<>( new Port( toDuplicateOperatorId( stateless2, 1 ), 0 ),
                                                             new Port( toDuplicateOperatorId( stateless3, 1 ), 0 ) ) ) );
    }

    @Test
    public void shouldDuplicateStatelessRegionWithMultipleUpstreamAndDownstreamRegions ()
    {
        final OperatorDef stateful1 = OperatorDefBuilder.newInstance( "stateful1", StatefulOperator0.class ).build();
        final OperatorDef stateful2 = OperatorDefBuilder.newInstance( "stateful2", StatefulOperator0.class ).build();
        final OperatorDef stateless1 = OperatorDefBuilder.newInstance( "stateless1", StatelessOperator1.class ).build();
        final OperatorDef stateless2 = OperatorDefBuilder.newInstance( "stateless2", StatelessOperator1.class ).build();
        final OperatorDef stateless3 = OperatorDefBuilder.newInstance( "stateless3", StatelessOperator1.class ).build();
        final OperatorDef stateful3 = OperatorDefBuilder.newInstance( "stateful3", StatefulOperator2.class ).build();
        final OperatorDef stateful4 = OperatorDefBuilder.newInstance( "stateful4", StatefulOperator2.class ).build();

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
        final Set<Entry<Port, Port>> connections = flow.getConnections();
        final Map<String, OperatorDef> operators = flow.getOperatorsMap();

        flowOptimizer.duplicateStatelessRegions( operators, connections, regions );

        assertNotNull( operators.get( toDuplicateOperatorId( stateless1, 0, 0 ) ) );
        assertNotNull( operators.get( toDuplicateOperatorId( stateless1, 0, 1 ) ) );
        assertNotNull( operators.get( toDuplicateOperatorId( stateless1, 1, 0 ) ) );
        assertNotNull( operators.get( toDuplicateOperatorId( stateless1, 1, 1 ) ) );
        assertNotNull( operators.get( toDuplicateOperatorId( stateless2, 0, 0 ) ) );
        assertNotNull( operators.get( toDuplicateOperatorId( stateless2, 0, 1 ) ) );
        assertNotNull( operators.get( toDuplicateOperatorId( stateless2, 1, 0 ) ) );
        assertNotNull( operators.get( toDuplicateOperatorId( stateless2, 1, 1 ) ) );
        assertNotNull( operators.get( toDuplicateOperatorId( stateless3, 0, 0 ) ) );
        assertNotNull( operators.get( toDuplicateOperatorId( stateless3, 0, 1 ) ) );
        assertNotNull( operators.get( toDuplicateOperatorId( stateless3, 1, 0 ) ) );
        assertNotNull( operators.get( toDuplicateOperatorId( stateless3, 1, 1 ) ) );

        assertTrue( connections.contains( new SimpleEntry<>( new Port( stateful1.getId(), 0 ),
                                                             new Port( toDuplicateOperatorId( stateless1, 0, 0 ), 0 ) ) ) );
        assertTrue( connections.contains( new SimpleEntry<>( new Port( stateful1.getId(), 0 ),
                                                             new Port( toDuplicateOperatorId( stateless1, 0, 1 ), 0 ) ) ) );
        assertTrue( connections.contains( new SimpleEntry<>( new Port( stateful2.getId(), 0 ),
                                                             new Port( toDuplicateOperatorId( stateless1, 1, 0 ), 0 ) ) ) );
        assertTrue( connections.contains( new SimpleEntry<>( new Port( stateful2.getId(), 0 ),
                                                             new Port( toDuplicateOperatorId( stateless1, 1, 1 ), 0 ) ) ) );
        assertTrue( connections.contains( new SimpleEntry<>( new Port( toDuplicateOperatorId( stateless3, 0, 0 ), 0 ),
                                                             new Port( stateful3.getId(), 0 ) ) ) );
        assertTrue( connections.contains( new SimpleEntry<>( new Port( toDuplicateOperatorId( stateless3, 0, 1 ), 0 ),
                                                             new Port( stateful4.getId(), 0 ) ) ) );
        assertTrue( connections.contains( new SimpleEntry<>( new Port( toDuplicateOperatorId( stateless3, 1, 0 ), 0 ),
                                                             new Port( stateful3.getId(), 0 ) ) ) );
        assertTrue( connections.contains( new SimpleEntry<>( new Port( toDuplicateOperatorId( stateless3, 1, 1 ), 0 ),
                                                             new Port( stateful4.getId(), 0 ) ) ) );
    }

    @Test
    public void shouldDuplicateMultipleStatelessRegions ()
    {
        final OperatorDef stateless1 = OperatorDefBuilder.newInstance( "stateless1", StatelessOperator0.class ).build();
        final OperatorDef stateless2 = OperatorDefBuilder.newInstance( "stateless2", StatelessOperator1.class ).build();
        final OperatorDef stateless3 = OperatorDefBuilder.newInstance( "stateless3", StatelessOperator1.class ).build();
        final OperatorDef stateful1 = OperatorDefBuilder.newInstance( "stateful1", StatefulOperator2.class ).build();
        final OperatorDef stateful2 = OperatorDefBuilder.newInstance( "stateful2", StatefulOperator2.class ).build();
        final OperatorDef stateless4 = OperatorDefBuilder.newInstance( "stateless4", StatelessOperator1.class ).build();
        final OperatorDef stateless5 = OperatorDefBuilder.newInstance( "stateless5", StatelessOperator1.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( stateless1 )
                                                 .add( stateless2 )
                                                 .add( stateless3 )
                                                 .add( stateful1 )
                                                 .add( stateful2 )
                                                 .add( stateless4 )
                                                 .add( stateless5 )
                                                 .connect( "stateless1", "stateless2" )
                                                 .connect( "stateless2", "stateless3" )
                                                 .connect( "stateless3", "stateful1" )
                                                 .connect( "stateless3", "stateful2" )
                                                 .connect( "stateful1", "stateless4" )
                                                 .connect( "stateful2", "stateless4" )
                                                 .connect( "stateless4", "stateless5" )
                                                 .build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );
        final Set<Entry<Port, Port>> connections = flow.getConnections();
        final Map<String, OperatorDef> operators = flow.getOperatorsMap();

        flowOptimizer.duplicateStatelessRegions( operators, connections, regions );

        assertNotNull( operators.get( toDuplicateOperatorId( stateless1, 0 ) ) );
        assertNotNull( operators.get( toDuplicateOperatorId( stateless1, 1 ) ) );
        assertNotNull( operators.get( toDuplicateOperatorId( stateless2, 0 ) ) );
        assertNotNull( operators.get( toDuplicateOperatorId( stateless2, 1 ) ) );
        assertNotNull( operators.get( toDuplicateOperatorId( stateless3, 0 ) ) );
        assertNotNull( operators.get( toDuplicateOperatorId( stateless3, 1 ) ) );
        assertNotNull( operators.get( toDuplicateOperatorId( stateless4, 0 ) ) );
        assertNotNull( operators.get( toDuplicateOperatorId( stateless4, 1 ) ) );
        assertNotNull( operators.get( toDuplicateOperatorId( stateless5, 0 ) ) );
        assertNotNull( operators.get( toDuplicateOperatorId( stateless5, 1 ) ) );

        assertTrue( connections.contains( new SimpleEntry<>( new Port( toDuplicateOperatorId( stateless3, 0 ), 0 ),
                                                             new Port( stateful1.getId(), 0 ) ) ) );
        assertTrue( connections.contains( new SimpleEntry<>( new Port( toDuplicateOperatorId( stateless3, 1 ), 0 ),
                                                             new Port( stateful2.getId(), 0 ) ) ) );
        assertTrue( connections.contains( new SimpleEntry<>( new Port( stateful1.getId(), 0 ),
                                                             new Port( toDuplicateOperatorId( stateless4, 0 ), 0 ) ) ) );
        assertTrue( connections.contains( new SimpleEntry<>( new Port( stateful2.getId(), 0 ),
                                                             new Port( toDuplicateOperatorId( stateless4, 1 ), 0 ) ) ) );
        assertTrue( connections.contains( new SimpleEntry<>( new Port( toDuplicateOperatorId( stateless1, 0 ), 0 ),
                                                             new Port( toDuplicateOperatorId( stateless2, 0 ), 0 ) ) ) );
        assertTrue( connections.contains( new SimpleEntry<>( new Port( toDuplicateOperatorId( stateless2, 0 ), 0 ),
                                                             new Port( toDuplicateOperatorId( stateless3, 0 ), 0 ) ) ) );
        assertTrue( connections.contains( new SimpleEntry<>( new Port( toDuplicateOperatorId( stateless1, 1 ), 0 ),
                                                             new Port( toDuplicateOperatorId( stateless2, 1 ), 0 ) ) ) );
        assertTrue( connections.contains( new SimpleEntry<>( new Port( toDuplicateOperatorId( stateless2, 1 ), 0 ),
                                                             new Port( toDuplicateOperatorId( stateless3, 1 ), 0 ) ) ) );
        assertTrue( connections.contains( new SimpleEntry<>( new Port( toDuplicateOperatorId( stateless4, 0 ), 0 ),
                                                             new Port( toDuplicateOperatorId( stateless5, 0 ), 0 ) ) ) );
        assertTrue( connections.contains( new SimpleEntry<>( new Port( toDuplicateOperatorId( stateless4, 1 ), 0 ),
                                                             new Port( toDuplicateOperatorId( stateless5, 1 ), 0 ) ) ) );
    }

    @OperatorSpec( inputPortCount = 0, outputPortCount = 1, type = STATELESS )
    private static class StatelessOperator0 implements Operator
    {

        @Override
        public SchedulingStrategy init ( final InitializationContext ctx )
        {
            return null;
        }

        @Override
        public void invoke ( final InvocationContext ctx )
        {
        }

    }


    @OperatorSpec( inputPortCount = 1, outputPortCount = 1, type = STATELESS )
    private static class StatelessOperator1 implements Operator
    {

        @Override
        public SchedulingStrategy init ( final InitializationContext ctx )
        {
            return null;
        }

        @Override
        public void invoke ( final InvocationContext ctx )
        {
        }

    }


    @OperatorSpec( inputPortCount = 0, outputPortCount = 2, type = STATEFUL )
    private static class StatefulOperator0 implements Operator
    {

        @Override
        public SchedulingStrategy init ( final InitializationContext ctx )
        {
            return null;
        }

        @Override
        public void invoke ( final InvocationContext ctx )
        {
        }

    }


    @OperatorSpec( inputPortCount = 2, outputPortCount = 2, type = STATEFUL )
    private static class StatefulOperator2 implements Operator
    {

        @Override
        public SchedulingStrategy init ( final InitializationContext ctx )
        {
            return null;
        }

        @Override
        public void invoke ( final InvocationContext ctx )
        {
        }

    }


    @OperatorSpec( inputPortCount = 1, outputPortCount = 1, type = PARTITIONED_STATEFUL )
    private static class PartitionedStatefulOperator implements Operator
    {

        @Override
        public SchedulingStrategy init ( final InitializationContext ctx )
        {
            return null;
        }

        @Override
        public void invoke ( final InvocationContext ctx )
        {
        }

    }

}
