package cs.bilkent.zanza.engine.supervisor.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Before;
import org.junit.Test;
import com.google.inject.Guice;
import com.google.inject.Injector;

import cs.bilkent.zanza.ZanzaModule;
import cs.bilkent.zanza.engine.config.ZanzaConfig;
import cs.bilkent.zanza.engine.exception.InitializationException;
import cs.bilkent.zanza.engine.pipeline.OperatorReplicaStatus;
import cs.bilkent.zanza.engine.pipeline.Pipeline;
import cs.bilkent.zanza.engine.pipeline.PipelineManager;
import cs.bilkent.zanza.engine.pipeline.PipelineReplica;
import cs.bilkent.zanza.engine.pipeline.impl.PipelineManagerImpl;
import cs.bilkent.zanza.engine.pipeline.impl.PipelineManagerImplTest.PartitionedStatefulOperatorInput2Output2;
import cs.bilkent.zanza.engine.pipeline.impl.PipelineManagerImplTest.StatefulOperatorInput0Output1;
import cs.bilkent.zanza.engine.pipeline.impl.PipelineManagerImplTest.StatefulOperatorInput1Output1;
import cs.bilkent.zanza.engine.region.RegionConfig;
import cs.bilkent.zanza.engine.region.RegionDef;
import cs.bilkent.zanza.engine.region.RegionDefFormer;
import cs.bilkent.zanza.engine.supervisor.impl.SupervisorImpl.FlowStatus;
import cs.bilkent.zanza.flow.FlowDef;
import cs.bilkent.zanza.flow.FlowDefBuilder;
import cs.bilkent.zanza.flow.OperatorDef;
import cs.bilkent.zanza.flow.OperatorDefBuilder;
import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenAvailable;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import cs.bilkent.zanza.operator.schema.annotation.OperatorSchema;
import cs.bilkent.zanza.operator.schema.annotation.PortSchema;
import static cs.bilkent.zanza.operator.schema.annotation.PortSchemaScope.EXACT_FIELD_SET;
import cs.bilkent.zanza.operator.schema.annotation.SchemaField;
import cs.bilkent.zanza.operator.spec.OperatorSpec;
import static cs.bilkent.zanza.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.zanza.operator.spec.OperatorType.STATEFUL;
import cs.bilkent.zanza.testutils.ZanzaAbstractTest;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SupervisorImplFlowLifecycleTest extends ZanzaAbstractTest
{

    private SupervisorImpl supervisor;

    private RegionDefFormer regionDefFormer;

    private PipelineManagerImpl pipelineManager;

    @Before
    public void init ()
    {
        final Injector injector = Guice.createInjector( new ZanzaModule( new ZanzaConfig() ) );
        supervisor = injector.getInstance( SupervisorImpl.class );
        regionDefFormer = injector.getInstance( RegionDefFormer.class );
        pipelineManager = (PipelineManagerImpl) injector.getInstance( PipelineManager.class );
    }

    @Test
    public void testFlowDeploymentWithTwoSubsequentRegions () throws ExecutionException, InterruptedException, TimeoutException
    {
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", StatefulOperatorInput0Output1.class ).build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatefulOperatorInput1Output1.class ).build();

        final FlowDef flowDef = new FlowDefBuilder().add( operatorDef1 ).add( operatorDef2 ).connect( "op1", "op2" ).build();
        final List<RegionDef> regions = regionDefFormer.createRegions( flowDef );
        final RegionConfig regionConfig1 = new RegionConfig( 0, regions.get( 0 ), singletonList( 0 ), 1 );
        final RegionConfig regionConfig2 = new RegionConfig( 1, regions.get( 1 ), singletonList( 0 ), 1 );

        supervisor.start( flowDef, Arrays.asList( regionConfig1, regionConfig2 ) );

        for ( Pipeline pipeline : pipelineManager.getPipelines().values() )
        {
            for ( int replicaIndex = 0; replicaIndex < pipeline.getReplicaCount(); replicaIndex++ )
            {
                final PipelineReplica pipelineReplica = pipeline.getPipelineReplica( replicaIndex );
                for ( int operatorIndex = 0; operatorIndex < pipelineReplica.getOperatorCount(); operatorIndex++ )
                {
                    assertEquals( OperatorReplicaStatus.RUNNING, pipelineReplica.getOperator( operatorIndex ).getStatus() );
                }
            }
        }

        supervisor.shutdown().get( 30, TimeUnit.SECONDS );
    }

    @Test
    public void testFlowDeploymentWithRegionWithMultipleUpstreamRegions () throws ExecutionException, InterruptedException, TimeoutException
    {
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", StatefulOperatorInput0Output1.class ).build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatefulOperatorInput0Output1.class ).build();
        final OperatorDef operatorDef3 = OperatorDefBuilder.newInstance( "op3", PartitionedStatefulOperatorInput2Output2.class )
                                                           .setPartitionFieldNames( Collections.singletonList( "field1" ) )
                                                           .build();

        final FlowDef flowDef = new FlowDefBuilder().add( operatorDef1 )
                                                    .add( operatorDef2 )
                                                    .add( operatorDef3 )
                                                    .connect( "op1", 0, "op3", 0 )
                                                    .connect( "op2", 0, "op3", 1 )
                                                    .build();
        final List<RegionDef> regions = regionDefFormer.createRegions( flowDef );
        final List<RegionConfig> regionConfigs = new ArrayList<>();
        int i = 0;
        for ( RegionDef region : regions )
        {
            final int replicaCount = region.getRegionType() == PARTITIONED_STATEFUL ? 4 : 1;
            regionConfigs.add( new RegionConfig( i++, region, singletonList( 0 ), replicaCount ) );
        }

        supervisor.start( flowDef, regionConfigs );

        supervisor.shutdown().get( 30, TimeUnit.SECONDS );
    }

    @Test( expected = IllegalStateException.class )
    public void testFlowDeploymentFailed () throws ExecutionException, InterruptedException, TimeoutException
    {
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", StatefulOperatorInput0Output1.class ).build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", FailingOnInitializationStatefulOperatorInput1Output1.class )
                                                           .build();

        final FlowDef flowDef = new FlowDefBuilder().add( operatorDef1 ).add( operatorDef2 ).connect( "op1", "op2" ).build();
        final List<RegionDef> regions = regionDefFormer.createRegions( flowDef );
        final RegionConfig regionConfig1 = new RegionConfig( 0, regions.get( 0 ), singletonList( 0 ), 1 );
        final RegionConfig regionConfig2 = new RegionConfig( 1, regions.get( 1 ), singletonList( 0 ), 1 );

        try
        {
            supervisor.start( flowDef, Arrays.asList( regionConfig1, regionConfig2 ) );
            fail();
        }
        catch ( InitializationException e )
        {
            assertTrue( pipelineManager.getPipelines().isEmpty() );
        }

        supervisor.shutdown().get( 30, TimeUnit.SECONDS );
    }

    @Test( expected = ExecutionException.class )
    public void testFlowFailedDuringExecution () throws ExecutionException, InterruptedException, TimeoutException
    {
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", TupleProducingStatefulOperatorInput0Output1.class ).build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", FailingOnInvocationStatefulOperatorInput1Output1.class )
                                                           .build();

        final FlowDef flowDef = new FlowDefBuilder().add( operatorDef1 ).add( operatorDef2 ).connect( "op1", "op2" ).build();
        final List<RegionDef> regions = regionDefFormer.createRegions( flowDef );
        final RegionConfig regionConfig1 = new RegionConfig( 0, regions.get( 0 ), singletonList( 0 ), 1 );
        final RegionConfig regionConfig2 = new RegionConfig( 1, regions.get( 1 ), singletonList( 0 ), 1 );

        supervisor.start( flowDef, Arrays.asList( regionConfig1, regionConfig2 ) );
        assertTrueEventually( () -> assertEquals( FlowStatus.SHUT_DOWN, supervisor.getStatus() ) );

        supervisor.shutdown().get( 30, TimeUnit.SECONDS );
    }

    @Test
    public void testFlowFailedDuringShutdown () throws ExecutionException, InterruptedException, TimeoutException
    {
        final OperatorDef operatorDef1 = OperatorDefBuilder.newInstance( "op1", FailingOnShutdownStatefulOperatorInput0Output1.class )
                                                           .build();
        final OperatorDef operatorDef2 = OperatorDefBuilder.newInstance( "op2", StatefulOperatorInput1Output1.class ).build();

        final FlowDef flowDef = new FlowDefBuilder().add( operatorDef1 ).add( operatorDef2 ).connect( "op1", "op2" ).build();
        final List<RegionDef> regions = regionDefFormer.createRegions( flowDef );
        final RegionConfig regionConfig1 = new RegionConfig( 0, regions.get( 0 ), singletonList( 0 ), 1 );
        final RegionConfig regionConfig2 = new RegionConfig( 1, regions.get( 1 ), singletonList( 0 ), 1 );

        supervisor.start( flowDef, Arrays.asList( regionConfig1, regionConfig2 ) );

        supervisor.shutdown().get( 30, TimeUnit.SECONDS );
    }

    @OperatorSpec( type = STATEFUL, inputPortCount = 1, outputPortCount = 1 )
    @OperatorSchema( inputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field1", type = Integer.class ) } ) }, outputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field1", type = Integer.class ) } ) } )
    public static class FailingOnInitializationStatefulOperatorInput1Output1 implements Operator
    {

        @Override
        public SchedulingStrategy init ( final InitializationContext context )
        {
            throw new RuntimeException( "expected" );
        }

        @Override
        public void invoke ( final InvocationContext invocationContext )
        {

        }

    }


    @OperatorSpec( type = STATEFUL, inputPortCount = 0, outputPortCount = 1 )
    @OperatorSchema( inputs = {}, outputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field1", type = Integer.class ) } ) } )
    public static class TupleProducingStatefulOperatorInput0Output1 implements Operator
    {

        @Override
        public SchedulingStrategy init ( final InitializationContext context )
        {
            return ScheduleWhenAvailable.INSTANCE;
        }

        @Override
        public void invoke ( final InvocationContext invocationContext )
        {
            invocationContext.getOutput().add( new Tuple() );
        }

    }


    @OperatorSpec( type = STATEFUL, inputPortCount = 1, outputPortCount = 1 )
    @OperatorSchema( inputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field1", type = Integer.class ) } ) }, outputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field1", type = Integer.class ) } ) } )
    public static class FailingOnInvocationStatefulOperatorInput1Output1 implements Operator
    {

        private int invocationCount = 0;

        @Override
        public SchedulingStrategy init ( final InitializationContext context )
        {
            return scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        }

        @Override
        public void invoke ( final InvocationContext invocationContext )
        {
            invocationCount++;
            if ( invocationCount == 10000 )
            {
                throw new RuntimeException( "expected" );
            }
        }

    }


    @OperatorSpec( type = STATEFUL, inputPortCount = 0, outputPortCount = 1 )
    @OperatorSchema( inputs = {}, outputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field1", type = Integer.class ) } ) } )
    public static class FailingOnShutdownStatefulOperatorInput0Output1 implements Operator
    {

        @Override
        public SchedulingStrategy init ( final InitializationContext context )
        {
            return ScheduleWhenAvailable.INSTANCE;
        }

        @Override
        public void invoke ( final InvocationContext invocationContext )
        {

        }

        @Override
        public void shutdown ()
        {
            throw new RuntimeException( "expected" );
        }

    }

}
