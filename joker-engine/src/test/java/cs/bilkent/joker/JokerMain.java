package cs.bilkent.joker;

import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchemaBuilder;
import cs.bilkent.joker.operators.BeaconOperator;
import static cs.bilkent.joker.operators.BeaconOperator.TUPLE_POPULATOR_CONFIG_PARAMETER;
import cs.bilkent.joker.operators.ConsoleAppenderOperator;
import cs.bilkent.joker.operators.MapperOperator;
import static cs.bilkent.joker.operators.MapperOperator.MAPPER_CONFIG_PARAMETER;
import cs.bilkent.joker.operators.TupleCountBasedWindowReducerOperator;
import static cs.bilkent.joker.operators.TupleCountBasedWindowReducerOperator.ACCUMULATOR_INITIALIZER_CONFIG_PARAMETER;
import static cs.bilkent.joker.operators.TupleCountBasedWindowReducerOperator.REDUCER_CONFIG_PARAMETER;
import static cs.bilkent.joker.operators.TupleCountBasedWindowReducerOperator.TUPLE_COUNT_CONFIG_PARAMETER;
import static java.util.Collections.singletonList;

public class JokerMain
{

    public static void main ( String[] args )
    {
        final Random random = new Random();
        final Joker joker = new Joker();

        final OperatorConfig beaconConfig = new OperatorConfig();
        beaconConfig.set( BeaconOperator.TUPLE_COUNT_CONFIG_PARAMETER, 10 );
        beaconConfig.set( TUPLE_POPULATOR_CONFIG_PARAMETER, (Consumer<Tuple>) tuple -> {
            sleepUninterruptibly( 250 + random.nextInt( 100 ), TimeUnit.MILLISECONDS );
            tuple.set( "field1", random.nextInt( 10 ) );
        } );
        final OperatorRuntimeSchemaBuilder beaconSchema = new OperatorRuntimeSchemaBuilder( 0, 1 );
        beaconSchema.addOutputField( 0, "field1", Integer.class );

        final OperatorDef beacon = OperatorDefBuilder.newInstance( "beacon", BeaconOperator.class )
                                                     .setConfig( beaconConfig )
                                                     .setExtendingSchema( beaconSchema )
                                                     .build();

        final OperatorConfig mapperConfig = new OperatorConfig();
        mapperConfig.set( MAPPER_CONFIG_PARAMETER,
                          (BiConsumer<Tuple, Tuple>) ( input, output ) -> output.set( "field1", input.get( "field1" ) ) );

        final OperatorRuntimeSchemaBuilder mapperSchema = new OperatorRuntimeSchemaBuilder( 1, 1 );
        mapperSchema.addInputField( 0, "field1", Integer.class ).addOutputField( 0, "field1", Integer.class );

        final OperatorDef mapper = OperatorDefBuilder.newInstance( "mapper", MapperOperator.class )
                                                     .setConfig( mapperConfig )
                                                     .setExtendingSchema( mapperSchema )
                                                     .build();

        final OperatorConfig windowConfig = new OperatorConfig();
        windowConfig.set( ACCUMULATOR_INITIALIZER_CONFIG_PARAMETER, (Consumer<Tuple>) tuple -> tuple.set( "field1", 0 ) );
        windowConfig.set( TUPLE_COUNT_CONFIG_PARAMETER, 1 );
        windowConfig.set( REDUCER_CONFIG_PARAMETER,
                          (BiConsumer<Tuple, Tuple>) ( acc, val ) -> acc.set( "field1",
                                                                              acc.getInteger( "field1" ) + val.getInteger( "field1" ) ) );

        final OperatorRuntimeSchemaBuilder windowSchema = new OperatorRuntimeSchemaBuilder( 1, 1 );
        windowSchema.addInputField( 0, "field1", Integer.class );

        final OperatorDef window = OperatorDefBuilder.newInstance( "window", TupleCountBasedWindowReducerOperator.class )
                                                     .setConfig( windowConfig )
                                                     .setExtendingSchema( windowSchema )
                                                     .setPartitionFieldNames( singletonList( "field1" ) )
                                                     .build();

        final OperatorRuntimeSchemaBuilder consoleAppenderSchema = new OperatorRuntimeSchemaBuilder( 1, 1 );
        consoleAppenderSchema.addInputField( 0, "field1", Integer.class );
        consoleAppenderSchema.addOutputField( 0, "field1", Integer.class );

        final OperatorDef consoleAppender = OperatorDefBuilder.newInstance( "consoleAppender", ConsoleAppenderOperator.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( beacon )
                                                 .add( mapper )
                                                 .add( window )
                                                 .add( consoleAppender )
                                                 .connect( "beacon", "mapper" )
                                                 .connect( "mapper", "window" )
                                                 .connect( "window", "consoleAppender" )
                                                 .build();

        joker.run( flow );

        sleepUninterruptibly( 30, TimeUnit.SECONDS );

        try
        {
            joker.shutdown().get( 30, TimeUnit.MINUTES );
            System.out.println( "JOKER FLOW COMPLETED" );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            System.out.println( "JOKER FLOW INTERRUPTED" );
            e.printStackTrace();
            System.exit( -1 );
        }
        catch ( ExecutionException e )
        {
            System.out.println( "JOKER FLOW FAILED" );
            e.printStackTrace();
            System.exit( -1 );
        }
        catch ( TimeoutException e )
        {
            System.out.println( "JOKER FLOW SHUTDOWN FAILED" );
            e.printStackTrace();
            System.exit( -1 );
        }
    }

}
