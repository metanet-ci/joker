package cs.bilkent.zanza.engine.pipeline.impl;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import com.google.inject.Guice;
import com.google.inject.Injector;

import cs.bilkent.zanza.ZanzaModule;
import cs.bilkent.zanza.engine.config.ZanzaConfig;
import static cs.bilkent.zanza.engine.pipeline.OperatorReplicaStatus.INITIAL;
import cs.bilkent.zanza.engine.pipeline.PipelineRuntimeManager;
import cs.bilkent.zanza.engine.pipeline.PipelineRuntimeState;
import cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus;
import static cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus.ACTIVE;
import static cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus.NO_CONNECTION;
import cs.bilkent.zanza.engine.pipeline.UpstreamContext;
import cs.bilkent.zanza.engine.region.RegionDef;
import cs.bilkent.zanza.engine.region.RegionDefFormer;
import cs.bilkent.zanza.engine.region.RegionRuntimeConfig;
import cs.bilkent.zanza.engine.supervisor.Supervisor;
import cs.bilkent.zanza.flow.FlowDef;
import cs.bilkent.zanza.flow.FlowDefBuilder;
import cs.bilkent.zanza.flow.OperatorDef;
import cs.bilkent.zanza.flow.OperatorDefBuilder;
import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import cs.bilkent.zanza.operator.schema.annotation.OperatorSchema;
import cs.bilkent.zanza.operator.schema.annotation.PortSchema;
import static cs.bilkent.zanza.operator.schema.annotation.PortSchemaScope.EXACT_FIELD_SET;
import cs.bilkent.zanza.operator.schema.annotation.SchemaField;
import cs.bilkent.zanza.operator.spec.OperatorSpec;
import static cs.bilkent.zanza.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.zanza.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.zanza.operator.spec.OperatorType.STATELESS;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class PipelineRuntimeManagerImplTest
{

    private final ZanzaConfig zanzaConfig = new ZanzaConfig();

    private RegionDefFormer regionDefFormer;

    private PipelineRuntimeManager pipelineRuntimeManager;

    private Supervisor supervisor;

    @Before
    public void init ()
    {
        final Injector injector = Guice.createInjector( new ZanzaModule( zanzaConfig ) );
        regionDefFormer = injector.getInstance( RegionDefFormer.class );
        pipelineRuntimeManager = injector.getInstance( PipelineRuntimeManager.class );
        supervisor = injector.getInstance( Supervisor.class );
    }

    @Test
    public void test_singleStatefulRegion ()
    {
        final OperatorDef operatorDef = OperatorDefBuilder.newInstance( "op1", StatefulOperatorInput0Output1.class ).build();
        final FlowDef flow = new FlowDefBuilder().add( operatorDef ).build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );
        final RegionRuntimeConfig regionRuntimeConfig = new RegionRuntimeConfig( 0, regions.get( 0 ), 1, singletonList( 0 ) );

        final List<PipelineRuntimeState> pipelineRuntimeStates = pipelineRuntimeManager.createPipelineRuntimeStates( supervisor,
                                                                                                                     flow,
                                                                                                                     singletonList(
                                                                                                                             regionRuntimeConfig ) );

        assertEquals( 1, pipelineRuntimeStates.size() );

        final PipelineRuntimeState pipelineRuntimeState = pipelineRuntimeStates.get( 0 );
        assertEquals( 0, pipelineRuntimeState.getOperatorIndex( operatorDef ) );
        assertEquals( INITIAL, pipelineRuntimeState.getPipelineStatus() );

        assertNotNull( pipelineRuntimeState.getPipelineReplica( 0 ) );
        assertNotNull( pipelineRuntimeState.getPipelineReplicaRunner( 0 ) );
        assertEquals( new UpstreamContext( 0, new UpstreamConnectionStatus[] {} ), pipelineRuntimeState.getUpstreamContext() );
        assertNotNull( pipelineRuntimeState.getDownstreamTupleSender( 0 ) );
    }

    @Test
    public void test_statefulRegion_partitionedStatefulRegionWithPartitionedStatefulAndStatelessOperators ()
    {
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", StatefulOperatorInput0Output1.class ).build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", PartitionedStatefulOperatorInput2Output2
                                                                                                      .class )
                                                           .setPartitionFieldNames( singletonList( "field1" ) )
                                                           .build();
        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", StatelessOperatorInput1Output1.class ).build();
        final FlowDef flow = new FlowDefBuilder().add( operatorDef1 )
                                                 .add( operatorDef2 )
                                                 .add( operatorDef3 )
                                                 .connect( "op1", "op2" )
                                                 .connect( "op2", "op3" )
                                                 .build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );
        assertEquals( 2, regions.size() );
        final RegionDef statefulRegionDef = regions.stream().filter( regionDef -> regionDef.getRegionType() == STATEFUL ).findFirst().get();
        final RegionDef partitionedStatefulRegionDef = regions.stream()
                                                              .filter( regionDef -> regionDef.getRegionType()
                                                                                                  == PARTITIONED_STATEFUL )
                                                              .findFirst()
                                                              .get();
        final RegionRuntimeConfig regionRuntimeConfig1 = new RegionRuntimeConfig( 0, statefulRegionDef, 1, singletonList( 0 ) );
        final RegionRuntimeConfig regionRuntimeConfig2 = new RegionRuntimeConfig( 1, partitionedStatefulRegionDef, 2, asList( 0, 1 ) );

        final List<PipelineRuntimeState> pipelineRuntimeStates = pipelineRuntimeManager.createPipelineRuntimeStates( supervisor,
                                                                                                                     flow,
                                                                                                                     asList( regionRuntimeConfig1,
                                                                                                                             regionRuntimeConfig2 ) );

        assertEquals( 3, pipelineRuntimeStates.size() );

        final PipelineRuntimeState pipelineRuntimeState1 = pipelineRuntimeStates.get( 0 );
        assertEquals( statefulRegionDef, pipelineRuntimeState1.getRegionDef() );
        assertEquals( 0, pipelineRuntimeState1.getOperatorIndex( operatorDef1 ) );

        final PipelineRuntimeState pipelineRuntimeState2 = pipelineRuntimeStates.get( 1 );
        assertEquals( new UpstreamContext( 0, new UpstreamConnectionStatus[] { ACTIVE, NO_CONNECTION } ),
                      pipelineRuntimeState2.getUpstreamContext() );
        assertEquals( partitionedStatefulRegionDef, pipelineRuntimeState2.getRegionDef() );
        assertEquals( 0, pipelineRuntimeState2.getOperatorIndex( operatorDef2 ) );
        assertFalse( pipelineRuntimeState2.getPipelineReplica( 0 ) == pipelineRuntimeState2.getPipelineReplica( 1 ) );

        final PipelineRuntimeState pipelineRuntimeState3 = pipelineRuntimeStates.get( 2 );
        assertEquals( new UpstreamContext( 0, new UpstreamConnectionStatus[] { ACTIVE } ), pipelineRuntimeState3.getUpstreamContext() );
        assertEquals( partitionedStatefulRegionDef, pipelineRuntimeState3.getRegionDef() );
        assertEquals( 0, pipelineRuntimeState3.getOperatorIndex( operatorDef3 ) );
        assertFalse( pipelineRuntimeState3.getPipelineReplica( 0 ) == pipelineRuntimeState3.getPipelineReplica( 1 ) );
    }

    @Test
    public void test_statefulRegion_partitionedStatefulRegionWithStatelessAndPartitionedStatefulOperators ()
    {
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", StatefulOperatorInput0Output1.class ).build();

        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatelessOperatorInput1Output1.class ).build();
        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", PartitionedStatefulOperatorInput2Output2
                                                                                                      .class )
                                                           .setPartitionFieldNames( singletonList( "field1" ) )
                                                           .build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef1 )
                                                 .add( operatorDef2 )
                                                 .add( operatorDef3 )
                                                 .connect( "op1", "op2" )
                                                 .connect( "op2", "op3" )
                                                 .build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );
        assertEquals( 2, regions.size() );

        final RegionDef statefulRegionDef = regions.stream().filter( regionDef -> regionDef.getRegionType() == STATEFUL ).findFirst().get();
        final RegionDef partitionedStatefulRegionDef = regions.stream()
                                                              .filter( regionDef -> regionDef.getRegionType()
                                                                                                  == PARTITIONED_STATEFUL )
                                                              .findFirst()
                                                              .get();
        final RegionRuntimeConfig regionRuntimeConfig1 = new RegionRuntimeConfig( 0, statefulRegionDef, 1, singletonList( 0 ) );
        final RegionRuntimeConfig regionRuntimeConfig2 = new RegionRuntimeConfig( 1, partitionedStatefulRegionDef, 2, asList( 0, 1 ) );

        final List<PipelineRuntimeState> pipelineRuntimeStates = pipelineRuntimeManager.createPipelineRuntimeStates( supervisor,
                                                                                                                     flow,
                                                                                                                     asList( regionRuntimeConfig1,
                                                                                                                             regionRuntimeConfig2 ) );

        assertEquals( 3, pipelineRuntimeStates.size() );

        final PipelineRuntimeState pipelineRuntimeState1 = pipelineRuntimeStates.get( 0 );
        assertEquals( statefulRegionDef, pipelineRuntimeState1.getRegionDef() );
        assertEquals( 0, pipelineRuntimeState1.getOperatorIndex( operatorDef1 ) );

        final PipelineRuntimeState pipelineRuntimeState2 = pipelineRuntimeStates.get( 1 );
        assertEquals( new UpstreamContext( 0, new UpstreamConnectionStatus[] { ACTIVE } ), pipelineRuntimeState2.getUpstreamContext() );
        assertEquals( partitionedStatefulRegionDef, pipelineRuntimeState2.getRegionDef() );
        assertEquals( 0, pipelineRuntimeState2.getOperatorIndex( operatorDef2 ) );
        assertFalse( pipelineRuntimeState2.getPipelineReplica( 0 ) == pipelineRuntimeState2.getPipelineReplica( 1 ) );

        final PipelineRuntimeState pipelineRuntimeState3 = pipelineRuntimeStates.get( 2 );
        assertEquals( new UpstreamContext( 0, new UpstreamConnectionStatus[] { ACTIVE, NO_CONNECTION } ),
                      pipelineRuntimeState3.getUpstreamContext() );
        assertEquals( partitionedStatefulRegionDef, pipelineRuntimeState3.getRegionDef() );
        assertEquals( 0, pipelineRuntimeState3.getOperatorIndex( operatorDef3 ) );
        assertFalse( pipelineRuntimeState3.getPipelineReplica( 0 ) == pipelineRuntimeState3.getPipelineReplica( 1 ) );
    }

    @Test
    public void test_twoRegions_connectedToSingleRegion ()
    {
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", StatefulOperatorInput0Output1.class ).build();

        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatefulOperatorInput0Output1.class ).build();

        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", StatelessOperatorInput1Output1.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef1 )
                                                 .add( operatorDef2 )
                                                 .add( operatorDef3 )
                                                 .connect( "op1", "op3" )
                                                 .connect( "op2", "op3" )
                                                 .build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );
        assertEquals( 3, regions.size() );

        final RegionDef statelessRegionDef = regions.stream()
                                                    .filter( regionDef -> regionDef.getRegionType() == STATELESS )
                                                    .findFirst()
                                                    .get();

        final List<RegionDef> statefulRegionDefs = regions.stream()
                                                          .filter( regionDef -> regionDef.getRegionType() == STATEFUL )
                                                          .collect( Collectors.toList() );

        final RegionDef statefulRegionDef1 = statefulRegionDefs.get( 0 ), statefulRegionDef2 = statefulRegionDefs.get( 1 );

        final RegionRuntimeConfig regionRuntimeConfig1 = new RegionRuntimeConfig( 0, statefulRegionDef1, 1, singletonList( 0 ) );
        final RegionRuntimeConfig regionRuntimeConfig2 = new RegionRuntimeConfig( 1, statefulRegionDef2, 1, singletonList( 0 ) );
        final RegionRuntimeConfig regionRuntimeConfig3 = new RegionRuntimeConfig( 2, statelessRegionDef, 1, singletonList( 0 ) );

        final List<PipelineRuntimeState> pipelineRuntimeStates = pipelineRuntimeManager.createPipelineRuntimeStates( supervisor,
                                                                                                                     flow,
                                                                                                                     asList( regionRuntimeConfig1,
                                                                                                                             regionRuntimeConfig2,
                                                                                                                             regionRuntimeConfig3 ) );

        final PipelineRuntimeState pipelineRuntimeState1 = pipelineRuntimeStates.get( 0 );
        assertEquals( statefulRegionDef1, pipelineRuntimeState1.getRegionDef() );
        assertEquals( 0, pipelineRuntimeState1.getOperatorIndex( statefulRegionDef1.getOperators().get( 0 ) ) );

        final PipelineRuntimeState pipelineRuntimeState2 = pipelineRuntimeStates.get( 1 );
        assertEquals( statefulRegionDef2, pipelineRuntimeState2.getRegionDef() );
        assertEquals( 0, pipelineRuntimeState2.getOperatorIndex( statefulRegionDef2.getOperators().get( 0 ) ) );

        final PipelineRuntimeState pipelineRuntimeState3 = pipelineRuntimeStates.get( 2 );
        assertEquals( new UpstreamContext( 0, new UpstreamConnectionStatus[] { ACTIVE } ), pipelineRuntimeState3.getUpstreamContext() );
        assertEquals( statelessRegionDef, pipelineRuntimeState3.getRegionDef() );
        assertEquals( 0, pipelineRuntimeState3.getOperatorIndex( operatorDef3 ) );
    }

    @Test
    public void test_singleRegion_connectedToTwoRegions ()
    {
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", StatefulOperatorInput0Output1.class ).build();

        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatelessOperatorInput1Output1.class ).build();

        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", PartitionedStatefulOperatorInput2Output2
                                                                                                      .class )
                                                           .setPartitionFieldNames( singletonList( "field1" ) )
                                                           .build();

        final FlowDef flow = new FlowDefBuilder().add( operatorDef1 )
                                                 .add( operatorDef2 )
                                                 .add( operatorDef3 )
                                                 .connect( "op1", "op2" )
                                                 .connect( "op1", 0, "op3", 0 )
                                                 .connect( "op1", 0, "op3", 1 )
                                                 .build();

        final List<RegionDef> regions = regionDefFormer.createRegions( flow );
        assertEquals( 3, regions.size() );

        final RegionDef statefulRegionDef = regions.stream().filter( regionDef -> regionDef.getRegionType() == STATEFUL ).findFirst().get();

        final RegionDef statelessRegionDef = regions.stream()
                                                    .filter( regionDef -> regionDef.getRegionType() == STATELESS )
                                                    .findFirst()
                                                    .get();

        final RegionDef partitionedStatefulRegionDef = regions.stream()
                                                              .filter( regionDef -> regionDef.getRegionType()
                                                                                                  == PARTITIONED_STATEFUL )
                                                              .findFirst()
                                                              .get();

        final RegionRuntimeConfig regionRuntimeConfig1 = new RegionRuntimeConfig( 0, statefulRegionDef, 1, singletonList( 0 ) );
        final RegionRuntimeConfig regionRuntimeConfig2 = new RegionRuntimeConfig( 1, statelessRegionDef, 1, singletonList( 0 ) );
        final RegionRuntimeConfig regionRuntimeConfig3 = new RegionRuntimeConfig( 2, partitionedStatefulRegionDef, 1, singletonList( 0 ) );

        final List<PipelineRuntimeState> pipelineRuntimeStates = pipelineRuntimeManager.createPipelineRuntimeStates( supervisor,
                                                                                                                     flow,
                                                                                                                     asList( regionRuntimeConfig1,
                                                                                                                             regionRuntimeConfig2,
                                                                                                                             regionRuntimeConfig3 ) );

        final PipelineRuntimeState pipelineRuntimeState1 = pipelineRuntimeStates.get( 0 );
        assertEquals( statefulRegionDef, pipelineRuntimeState1.getRegionDef() );
        assertEquals( 0, pipelineRuntimeState1.getOperatorIndex( statefulRegionDef.getOperators().get( 0 ) ) );

        final PipelineRuntimeState pipelineRuntimeState2 = pipelineRuntimeStates.get( 1 );
        assertEquals( statelessRegionDef, pipelineRuntimeState2.getRegionDef() );
        assertEquals( new UpstreamContext( 0, new UpstreamConnectionStatus[] { ACTIVE } ), pipelineRuntimeState2.getUpstreamContext() );
        assertEquals( 0, pipelineRuntimeState2.getOperatorIndex( statelessRegionDef.getOperators().get( 0 ) ) );

        final PipelineRuntimeState pipelineRuntimeState3 = pipelineRuntimeStates.get( 2 );
        assertEquals( new UpstreamContext( 0, new UpstreamConnectionStatus[] { ACTIVE, ACTIVE } ),
                      pipelineRuntimeState3.getUpstreamContext() );
        assertEquals( partitionedStatefulRegionDef, pipelineRuntimeState3.getRegionDef() );
        assertEquals( 0, pipelineRuntimeState3.getOperatorIndex( operatorDef3 ) );
    }

    @OperatorSpec( type = STATEFUL, inputPortCount = 0, outputPortCount = 1 )
    @OperatorSchema( inputs = {},
            outputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field1", type = Integer.class ) } ) } )
    private static class StatefulOperatorInput0Output1 implements Operator
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


    @OperatorSpec( type = STATELESS, inputPortCount = 1, outputPortCount = 1 )
    @OperatorSchema( inputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field1", type = Integer.class ) } ) },
            outputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field1", type = Integer.class ) } ) } )
    private static class StatelessOperatorInput1Output1 implements Operator
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


    @OperatorSpec( type = PARTITIONED_STATEFUL, inputPortCount = 2, outputPortCount = 2 )
    @OperatorSchema( inputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field1", type = Integer.class ) } ),
                                @PortSchema( portIndex = 1, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field1", type = Integer.class ) } ) },
            outputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field1", type = Integer.class ) } ) } )
    private static class PartitionedStatefulOperatorInput2Output2 implements Operator
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
