package cs.bilkent.joker.experiment;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.typesafe.config.Config;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static com.typesafe.config.ConfigFactory.systemProperties;
import cs.bilkent.joker.Joker;
import cs.bilkent.joker.Joker.JokerBuilder;
import cs.bilkent.joker.engine.adaptation.impl.adaptationtracker.ExperimentalAdaptationTracker;
import cs.bilkent.joker.engine.adaptation.impl.adaptationtracker.FlowMetricsFileReporter;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.config.JokerConfigBuilder;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import cs.bilkent.joker.engine.region.impl.AbstractRegionExecPlanFactory;
import cs.bilkent.joker.flow.FlowDef;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;

public class ModelStaticConfigurationRunner
{

    public static void main ( String[] args ) throws ClassNotFoundException, IllegalAccessException, InstantiationException
    {
        final Config config = systemProperties();

        final int region1ReplicaCount = config.getInt( "region1ReplicaCount" );
        final List<Integer> region1PipelineStartIndices = Arrays.stream( config.getString( "region1PipelineStartIndices" ).split( "_" ) )
                                                                .map( Integer::valueOf )
                                                                .collect( toList() );

        final int region3ReplicaCount = config.getInt( "region3ReplicaCount" );
        final List<Integer> region3PipelineStartIndices = Arrays.stream( config.getString( "region3PipelineStartIndices" ).split( "_" ) )
                                                                .map( Integer::valueOf )
                                                                .collect( toList() );

        final JokerConfigBuilder configBuilder = new JokerConfigBuilder();
        configBuilder.getAdaptationConfigBuilder().disableAdaptation();
        configBuilder.getFlowDefOptimizerConfigBuilder().disableMergeRegions();

        final JokerConfig jokerConfig = configBuilder.build( config );

        final AbstractRegionExecPlanFactory staticRegionExecPlanFactory = new AbstractRegionExecPlanFactory( jokerConfig )
        {
            @Override
            protected RegionExecPlan createRegionExecPlan ( final RegionDef regionDef )
            {
                switch ( regionDef.getRegionId() )
                {
                    case 1:
                        return new RegionExecPlan( regionDef, region1PipelineStartIndices, region1ReplicaCount );
                    case 3:
                        return new RegionExecPlan( regionDef, region3PipelineStartIndices, region3ReplicaCount );
                    default:
                        return new RegionExecPlan( regionDef, Collections.singletonList( 0 ), 1 );
                }
            }
        };

        final String flowDefFactoryClassName = config.getString( "flowFactory" );
        final FlowDefFactory flowDefFactory = (FlowDefFactory) Class.forName( flowDefFactoryClassName ).newInstance();
        final FlowDef flow = flowDefFactory.createFlow( jokerConfig );

        final String reportDir = config.getString( "reportDir" );
        final FlowMetricsFileReporter reporter = new FlowMetricsFileReporter( jokerConfig, new File( reportDir ) );
        reporter.init();

        final ExperimentalAdaptationTracker adaptationTracker = new ExperimentalAdaptationTracker( jokerConfig, reporter );
        final Joker joker = new JokerBuilder().setJokerConfig( jokerConfig )
                                              .setRegionExecPlanFactory( staticRegionExecPlanFactory )
                                              .setAdaptationTracker( adaptationTracker )
                                              .build();

        joker.run( flow );

        sleepUninterruptibly( 1, MINUTES );

        joker.shutdown().join();
        System.exit( 0 );
    }

}
