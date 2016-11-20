package cs.bilkent.joker.engine.supervisor.impl;

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

import cs.bilkent.joker.JokerModule;
import cs.bilkent.joker.engine.FlowStatus;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.exception.InitializationException;
import cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus;
import cs.bilkent.joker.engine.pipeline.Pipeline;
import cs.bilkent.joker.engine.pipeline.PipelineManager;
import cs.bilkent.joker.engine.pipeline.PipelineReplica;
import cs.bilkent.joker.engine.pipeline.impl.PipelineManagerImpl;
import cs.bilkent.joker.engine.pipeline.impl.PipelineManagerImplTest.PartitionedStatefulOperatorInput2Output2;
import cs.bilkent.joker.engine.pipeline.impl.PipelineManagerImplTest.StatefulOperatorInput0Output1;
import cs.bilkent.joker.engine.pipeline.impl.PipelineManagerImplTest.StatefulOperatorInput1Output1;
import cs.bilkent.joker.engine.region.FlowDeploymentDef;
import cs.bilkent.joker.engine.region.RegionConfig;
import cs.bilkent.joker.engine.region.RegionDef;
import cs.bilkent.joker.engine.region.RegionDefFormer;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.operator.InitializationContext;
import cs.bilkent.joker.operator.InvocationContext;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenAvailable;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.annotation.OperatorSchema;
import cs.bilkent.joker.operator.schema.annotation.PortSchema;
import static cs.bilkent.joker.operator.schema.annotation.PortSchemaScope.EXACT_FIELD_SET;
import cs.bilkent.joker.operator.schema.annotation.SchemaField;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SupervisorImplFlowLifecycleTest extends AbstractJokerTest
{

    private SupervisorImpl supervisor;

    private RegionDefFormer regionDefFormer;

    private PipelineManagerImpl pipelineManager;

    @Before
    public void init ()
    {
        final Injector injector = Guice.createInjector( new JokerModule( new JokerConfig() ) );
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
        final RegionConfig regionConfig1 = new RegionConfig( regions.get( 0 ), singletonList( 0 ), 1 );
        final RegionConfig regionConfig2 = new RegionConfig( regions.get( 1 ), singletonList( 0 ), 1 );

        final FlowDeploymentDef flowDeployment = new FlowDeploymentDef( flowDef, regions );

        supervisor.start( flowDeployment, Arrays.asList( regionConfig1, regionConfig2 ) );

        for ( Pipeline pipeline : pipelineManager.getPipelines() )
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
        for ( RegionDef region : regions )
        {
            final int replicaCount = region.getRegionType() == PARTITIONED_STATEFUL ? 4 : 1;
            regionConfigs.add( new RegionConfig( region, singletonList( 0 ), replicaCount ) );
        }

        final FlowDeploymentDef flowDeployment = new FlowDeploymentDef( flowDef, regions );

        supervisor.start( flowDeployment, regionConfigs );

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
        final RegionConfig regionConfig1 = new RegionConfig( regions.get( 0 ), singletonList( 0 ), 1 );
        final RegionConfig regionConfig2 = new RegionConfig( regions.get( 1 ), singletonList( 0 ), 1 );

        final FlowDeploymentDef flowDeployment = new FlowDeploymentDef( flowDef, regions );

        try
        {
            supervisor.start( flowDeployment, Arrays.asList( regionConfig1, regionConfig2 ) );
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
        final RegionConfig regionConfig1 = new RegionConfig( regions.get( 0 ), singletonList( 0 ), 1 );
        final RegionConfig regionConfig2 = new RegionConfig( regions.get( 1 ), singletonList( 0 ), 1 );

        final FlowDeploymentDef flowDeployment = new FlowDeploymentDef( flowDef, regions );

        supervisor.start( flowDeployment, Arrays.asList( regionConfig1, regionConfig2 ) );
        assertTrueEventually( () -> assertEquals( FlowStatus.SHUT_DOWN, supervisor.getFlowStatus() ) );

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
        final RegionConfig regionConfig1 = new RegionConfig( regions.get( 0 ), singletonList( 0 ), 1 );
        final RegionConfig regionConfig2 = new RegionConfig( regions.get( 1 ), singletonList( 0 ), 1 );

        final FlowDeploymentDef flowDeployment = new FlowDeploymentDef( flowDef, regions );

        supervisor.start( flowDeployment, Arrays.asList( regionConfig1, regionConfig2 ) );

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
    @OperatorSchema( outputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field1", type = Integer.class ) } ) } )
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
    @OperatorSchema( outputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field1", type = Integer.class ) } ) } )
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
