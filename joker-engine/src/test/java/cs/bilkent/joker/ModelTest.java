package cs.bilkent.joker;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import cs.bilkent.joker.Joker.JokerBuilder;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.config.JokerConfigBuilder;
import cs.bilkent.joker.engine.flow.FlowExecPlan;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchema;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchemaBuilder;
import cs.bilkent.joker.operators.BeaconOperator;
import static cs.bilkent.joker.operators.BeaconOperator.TUPLE_POPULATOR_CONFIG_PARAMETER;
import cs.bilkent.joker.operators.MapperOperator;
import cs.bilkent.joker.operators.PartitionedMapperOperator;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Collections.shuffle;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ModelTest extends AbstractJokerTest
{
    private static final int JOKER_APPLICATION_RUNNING_TIME_IN_SECONDS = 50;
    private static final int JOKER_APPLICATION_WARM_UP_TIME_IN_SECONDS = 10;
    private static final String TEST_OUTPUT_FILE_PATH = String.format(
            "target/surefire-reports/%s-output.txt", ModelTest.class.getCanonicalName());
    private static final String THROUGHPUT_RETRIEVER_FILE_PATH = "src/test/resources/grepThroughput.sh";

    private static final int KEY_RANGE = 1000;
    private static final int MULTIPLICATION_COUNT = 100;
    private static final int MULTIPLIER_VALUE = 271;


    static class ValueGenerator implements Consumer<Tuple>
    {
        private final int[] vals;
        private int curr;

        ValueGenerator ( final int keyRange )
        {
            final List<Integer> v = new ArrayList<>();
            for ( int i = 0; i < 100; i++ )
            {
                for ( int key = 0; key < keyRange; key++ )
                {
                    v.add( key );
                }
            }
            for ( int i = 0; i < 10; i++ )
            {
                shuffle( v );
            }
            vals = new int[ v.size() ];
            for ( int i = 0; i < v.size(); i++ )
            {
                vals[ i ] = v.get( i );
            }
        }

        @Override
        public void accept ( final Tuple tuple )
        {
            final int key = vals[ curr++ ];
            final int value = key + 1;

            tuple.set( "key", key ).set( "value", value );
            if ( curr == vals.length )
            {
                curr = 0;
            }
        }
    }

    private static class TestExecutionHelper
    {
        private static final String PIPELINE_SPECIFICATION = "P[1][0][0]";

        private static int lastThroughputCount = 0;

        private final JokerConfig config;
        private final FlowDef flow;

        TestExecutionHelper ( final FlowDef flow )
        {
            this.flow = flow;

            final JokerConfigBuilder configBuilder = new JokerConfigBuilder();
            configBuilder.getTupleQueueDrainerConfigBuilder().setMaxBatchSize( 4096 );
            configBuilder.getMetricManagerConfigBuilder().setTickMask( 3 );
            configBuilder.getMetricManagerConfigBuilder().setPipelineMetricsScanningPeriodInMillis( 1000 );
            configBuilder.getFlowDefOptimizerConfigBuilder().disableMergeRegions();
            configBuilder.getPipelineReplicaRunnerConfigBuilder().enforceThreadAffinity( true );

            // disable latency tracking...
            configBuilder.getPipelineManagerConfigBuilder().setLatencyTickMask( 16383 );
            configBuilder.getPipelineManagerConfigBuilder().setLatencyStageTickMask( 16383 );
            configBuilder.getPipelineManagerConfigBuilder().setInterArrivalTimeTrackingPeriod( 100_000_000 );
            configBuilder.getPipelineManagerConfigBuilder().setInterArrivalTimeTrackingCount( 1 );

            config = configBuilder.build();
        }

        private double retrieveThroughput ()
        {
            final File outputFile;
            try
            {
                outputFile = File.createTempFile( "standardErrorAndOutput-", "txt" );
            }
            catch ( final IOException e )
            {
                throw new RuntimeException( "failed to create a temporary file for the standard error/output of the throughput retriever",
                                            e );
            }
            outputFile.deleteOnExit();
            final File throughputFile;
            try
            {
                throughputFile = File.createTempFile( "throughput-", "txt" );
            }
            catch ( final IOException e )
            {
                throw new RuntimeException( "failed to create a throughput file for the throughput retriever", e );
            }
            throughputFile.deleteOnExit();
            final Process process;
            try
            {
                process = new ProcessBuilder().command( THROUGHPUT_RETRIEVER_FILE_PATH,
                                                        TEST_OUTPUT_FILE_PATH,
                                                        PIPELINE_SPECIFICATION,
                                                        throughputFile.toString() )
                                              .inheritIO()
                                              .redirectErrorStream( true )
                                              .redirectOutput( outputFile )
                                              .start();
            }
            catch ( final IOException e )
            {
                throw new RuntimeException( "failed to launch the throughput retriever process", e );
            }
            final int exitValue;
            try
            {
                exitValue = process.waitFor();
            }
            catch ( final InterruptedException e )
            {
                Thread.currentThread().interrupt();
                throw new RuntimeException( "interrupted while waiting for the throughput retriever to complete", e );
            }
            if ( exitValue != 0 )
            {
                String outputString;
                try
                {
                    outputString = readFileContents( outputFile );
                }
                catch ( final IOException e )
                {
                    outputString = "<failed to retrieve the script output>";
                }
                throw new RuntimeException( String.format(
                        "failed to execute the throughput retriever process, the exit value was %d, the combined standard output/error "
                        + "was:\n%s",
                        exitValue,
                        outputString ) );
            }
            final String throughputOutputString;
            try
            {
                throughputOutputString = readFileContents( throughputFile );
            }
            catch ( final IOException e )
            {
                throw new RuntimeException( "failed to read the throughput file", e );
            }
            String[] throughputStrings = throughputOutputString.split( System.lineSeparator() );
            final int currentThroughputCount = throughputStrings.length;
            if (currentThroughputCount == lastThroughputCount)
            {
                throw new RuntimeException("failed to find new throughput values");
            }
            throughputStrings = Arrays.copyOfRange(throughputStrings, lastThroughputCount, currentThroughputCount);
            lastThroughputCount = currentThroughputCount;
            return Arrays.stream(throughputStrings).mapToDouble(Double::parseDouble).average().getAsDouble();
        }

        private String readFileContents ( final File file ) throws IOException
        {
            return new String( Files.readAllBytes( file.toPath() ), StandardCharsets.UTF_8 );
        }

        double runTestAndGetThroughput ()
        {
            return runTestAndGetThroughput( (joker, execPlan) ->  {} );
        }

        double runTestAndGetThroughput ( final BiConsumer<Joker, FlowExecPlan> testCustomizer )
        {
            final Joker joker = new JokerBuilder().setJokerConfig( config ).build();
            final FlowExecPlan execPlan = joker.run( flow );
            sleepUninterruptibly( JOKER_APPLICATION_WARM_UP_TIME_IN_SECONDS, SECONDS );
            testCustomizer.accept( joker, execPlan );
            sleepUninterruptibly( JOKER_APPLICATION_RUNNING_TIME_IN_SECONDS, SECONDS );
            joker.shutdown().join();
            return retrieveThroughput();
        }
    }

    private FlowDef buildStatelessTopology ()
    {
        final int emittedTupleCountPerSourceInvocation = 4;

        final ValueGenerator valueGenerator = new ValueGenerator( KEY_RANGE );
        final OperatorConfig sourceConfig = new OperatorConfig().set( TUPLE_POPULATOR_CONFIG_PARAMETER, valueGenerator )
                .set( BeaconOperator.TUPLE_COUNT_CONFIG_PARAMETER,
                        emittedTupleCountPerSourceInvocation );

        final OperatorRuntimeSchema sourceSchema = new OperatorRuntimeSchemaBuilder( 0, 1 ).addOutputField( 0, "key", Integer.class )
                .addOutputField( 0, "value", Integer.class )
                .build();

        final OperatorDef source = OperatorDefBuilder.newInstance( "src", BeaconOperator.class )
                .setConfig( sourceConfig )
                .setExtendingSchema( sourceSchema )
                .build();

        final OperatorRuntimeSchema multiplier1Schema = new OperatorRuntimeSchemaBuilder( 1, 1 ).addInputField( 0, "key", Integer.class )
                .addInputField( 0, "value", Integer.class )
                .addOutputField( 0, "key", Integer.class )
                .addOutputField( 0, "mult1", Integer.class )
                .build();

        final BiConsumer<Tuple, Tuple> multiplier1Func = ( input, output ) -> {
            int val = input.getInteger( "value" );
            for ( int i = 0; i < MULTIPLICATION_COUNT; i++ )
            {
                val = val * MULTIPLIER_VALUE;
            }
            val = val * MULTIPLIER_VALUE;
            output.set( "key", input.get( "key" ) ).set( "mult1", val );
        };

        final OperatorConfig multiplier1Config = new OperatorConfig().set( MapperOperator.MAPPER_CONFIG_PARAMETER, multiplier1Func );

        final OperatorDef multiplier1 = OperatorDefBuilder.newInstance( "mult1", MapperOperator.class )
                .setExtendingSchema( multiplier1Schema )
                .setConfig( multiplier1Config )
                .build();

        final OperatorRuntimeSchema multiplier2Schema = new OperatorRuntimeSchemaBuilder( 1, 1 ).addInputField( 0, "key", Integer.class )
                .addInputField( 0, "mult1", Integer.class )
                .addOutputField( 0, "key", Integer.class )
                .addOutputField( 0, "mult2", Integer.class )
                .build();

        final BiConsumer<Tuple, Tuple> multiplier2Func = ( input, output ) -> {
            int val = input.getInteger( "mult1" );
            for ( int i = 0; i < MULTIPLICATION_COUNT; i++ )
            {
                val = val * MULTIPLIER_VALUE;
            }
            val = val * MULTIPLIER_VALUE;
            output.set( "key", input.get( "key" ) ).set( "mult2", val );
        };

        final OperatorConfig multiplier2Config = new OperatorConfig().set( MapperOperator.MAPPER_CONFIG_PARAMETER, multiplier2Func );

        final OperatorDef multiplier2 = OperatorDefBuilder.newInstance( "mult2", MapperOperator.class )
                .setExtendingSchema( multiplier2Schema )
                .setConfig( multiplier2Config )
                .build();

        return new FlowDefBuilder().add( source )
                .add( multiplier1 )
                .add( multiplier2 )
                .connect( source.getId(), multiplier1.getId() )
                .connect( multiplier1.getId(), multiplier2.getId() )
                .build();
    }

    public FlowDef buildPartitionedStatefulTopology ()
    {
        final int emittedTupleCountPerSourceInvocation = 1;

        final ValueGenerator valueGenerator = new ValueGenerator( KEY_RANGE );
        final OperatorConfig sourceConfig = new OperatorConfig().set( TUPLE_POPULATOR_CONFIG_PARAMETER, valueGenerator )
                .set( BeaconOperator.TUPLE_COUNT_CONFIG_PARAMETER,
                        emittedTupleCountPerSourceInvocation );

        final OperatorRuntimeSchema sourceSchema = new OperatorRuntimeSchemaBuilder( 0, 1 ).addOutputField( 0, "key", Integer.class )
                .addOutputField( 0, "value", Integer.class )
                .build();

        final OperatorDef source = OperatorDefBuilder.newInstance( "src", BeaconOperator.class )
                .setConfig( sourceConfig )
                .setExtendingSchema( sourceSchema )
                .build();

        final OperatorRuntimeSchema multiplier1Schema = new OperatorRuntimeSchemaBuilder( 1, 1 ).addInputField( 0, "key", Integer.class )
                .addInputField( 0, "value", Integer.class )
                .addOutputField( 0, "key", Integer.class )
                .addOutputField( 0, "mult1", Integer.class )
                .build();

        final BiConsumer<Tuple, Tuple> multiplier1Func = ( input, output ) -> {
            int val = input.getInteger( "value" );
            for ( int i = 0; i < MULTIPLICATION_COUNT; i++ )
            {
                val = val * MULTIPLIER_VALUE;
            }
            val = val * MULTIPLIER_VALUE;
            output.set( "key", input.get( "key" ) ).set( "mult1", val );
        };

        final OperatorConfig multiplier1Config = new OperatorConfig().set( PartitionedMapperOperator.MAPPER_CONFIG_PARAMETER,
                multiplier1Func );

        final OperatorDef multiplier1 = OperatorDefBuilder.newInstance( "mult1", PartitionedMapperOperator.class )
                .setExtendingSchema( multiplier1Schema )
                .setConfig( multiplier1Config )
                .setPartitionFieldNames( singletonList( "key" ) )
                .build();

        final OperatorRuntimeSchema multiplier2Schema = new OperatorRuntimeSchemaBuilder( 1, 1 ).addInputField( 0, "key", Integer.class )
                .addInputField( 0, "mult1", Integer.class )
                .addOutputField( 0, "key", Integer.class )
                .addOutputField( 0, "mult2", Integer.class )
                .build();

        final BiConsumer<Tuple, Tuple> multiplier2Func = ( input, output ) -> {
            int val = input.getInteger( "mult1" );
            for ( int i = 0; i < MULTIPLICATION_COUNT; i++ )
            {
                val = val * MULTIPLIER_VALUE;
            }
            val = val * MULTIPLIER_VALUE;
            output.set( "key", input.get( "key" ) ).set( "mult2", val );
        };

        final OperatorConfig multiplier2Config = new OperatorConfig().set( PartitionedMapperOperator.MAPPER_CONFIG_PARAMETER,
                multiplier2Func );

        final OperatorDef multiplier2 = OperatorDefBuilder.newInstance( "mult2", PartitionedMapperOperator.class )
                .setExtendingSchema( multiplier2Schema )
                .setConfig( multiplier2Config )
                .setPartitionFieldNames( singletonList( "key" ) )
                .build();

        return new FlowDefBuilder().add( source )
                .add( multiplier1 )
                .add( multiplier2 )
                .connect( source.getId(), multiplier1.getId() )
                .connect( multiplier1.getId(), multiplier2.getId() )
                .build();
    }

    @Test
    public void test_discover_thread_switching_overhead ()
    {
        TestExecutionHelper testExecutionHelper = new TestExecutionHelper( buildStatelessTopology() );
        final double sequentialThroughput = testExecutionHelper.runTestAndGetThroughput();
        testExecutionHelper = new TestExecutionHelper( buildStatelessTopology() );
        final double parallelThroughput = testExecutionHelper.runTestAndGetThroughput((joker, execPlan) -> {
                final RegionExecPlan partitionedStatefulRegionExecPlan = getProcessingRegion( execPlan );
                // the partitioned stateful region has a single pipeline, which contains 2 operators.
                // splits the pipeline from the 2nd operator (operatorIndex=1), which is the last parameter
                joker.splitPipeline( execPlan.getVersion(), partitionedStatefulRegionExecPlan.getPipelineId( 0 ), singletonList( 1 ) )
                        .join();
        });
        System.out.println( String.format( "Sequential throughput is %.2f", sequentialThroughput ) );
        System.out.println( String.format( "Parallel throughput is %.2f", parallelThroughput ) );
        // Computed based on Eq 17 from the earlier JPDC paper
        final double threadSwitchingOverhead = 1.0 / parallelThroughput - 0.5 / sequentialThroughput;
        System.out.println( String.format( "Multiplication cost is %.2e", 1.0 / (MULTIPLICATION_COUNT * sequentialThroughput ) ) );
        System.out.println( String.format( "Thread switching overhead is %.2e", threadSwitchingOverhead ) );
    }

    @Test
    public void test_discover_replication_cost_factor ()
    {
        final int numReplicas = 4;
        TestExecutionHelper testExecutionHelper = new TestExecutionHelper( buildPartitionedStatefulTopology() );
        final double sequentialThroughput = testExecutionHelper.runTestAndGetThroughput();
        testExecutionHelper = new TestExecutionHelper( buildPartitionedStatefulTopology() );
        final double parallelThroughput = testExecutionHelper.runTestAndGetThroughput((joker, execPlan) -> {
            joker.rebalanceRegion( execPlan.getVersion(), getProcessingRegion( execPlan ).getRegionId(), numReplicas);
        });
        System.out.println( String.format( "Sequential throughput is %.2f", sequentialThroughput ) );
        System.out.println( String.format( "Parallel throughput is %.2f", parallelThroughput ) );
        final Function<Double, Double> log2 = value -> Math.log(value) / Math.log(2.0);
        // Computed based on Eq 18 from the earlier JPDC paper
        final double replicationCostFactor = (1.0 / (numReplicas * sequentialThroughput) - 1.0 / parallelThroughput) /
                (log2.apply(1.0) / numReplicas - log2.apply((double) numReplicas));
        System.out.println( String.format( "Replication cost factor is %.2e", replicationCostFactor ) );
    }

    @NotNull
    private RegionExecPlan getProcessingRegion ( final FlowExecPlan execPlan )
    {
        return execPlan.getRegionExecPlans()
                       .stream()
                       .filter( r -> !r.getRegionDef().isSource() )
                       .findFirst()
                       .orElseThrow( IllegalStateException::new );
    }
}
