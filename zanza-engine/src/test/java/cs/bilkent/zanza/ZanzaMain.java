package cs.bilkent.zanza;

import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.google.common.util.concurrent.Uninterruptibles;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import cs.bilkent.zanza.flow.FlowDef;
import cs.bilkent.zanza.flow.FlowDefBuilder;
import cs.bilkent.zanza.flow.OperatorDef;
import cs.bilkent.zanza.flow.OperatorDefBuilder;
import cs.bilkent.zanza.flow.OperatorRuntimeSchemaBuilder;
import cs.bilkent.zanza.operator.OperatorConfig;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operators.BeaconOperator;
import cs.bilkent.zanza.operators.ConsoleAppenderOperator;
import cs.bilkent.zanza.operators.MapperOperator;
import cs.bilkent.zanza.operators.TupleCountBasedWindowReducerOperator;
import static java.util.Collections.singletonList;

public class ZanzaMain
{

    public static void main ( String[] args )
    {

        final Zanza zanza = new Zanza();

        final OperatorConfig beaconConfig = new OperatorConfig();
        beaconConfig.set( BeaconOperator.TUPLE_COUNT_CONFIG_PARAMETER, 10 );
        beaconConfig.set( BeaconOperator.TUPLE_GENERATOR_CONFIG_PARAMETER, (Function<Random, Tuple>) random ->
        {
            sleepUninterruptibly( 1 + random.nextInt( 100 ), TimeUnit.MILLISECONDS );
            return new Tuple( "field1", random.nextInt( 10 ) );
        } );
        final OperatorRuntimeSchemaBuilder beaconSchema = new OperatorRuntimeSchemaBuilder( 0, 1 );
        beaconSchema.getOutputPortSchemaBuilder( 0 ).addField( "field1", Integer.class );

        final OperatorDef beacon = OperatorDefBuilder.newInstance( "beacon", BeaconOperator.class )
                                                     .setConfig( beaconConfig )
                                                     .setExtendingSchema( beaconSchema )
                                                     .build();

        final OperatorConfig mapperConfig = new OperatorConfig();
        mapperConfig.set( MapperOperator.MAPPER_CONFIG_PARAMETER,
                          (Function<Tuple, Tuple>) tuple -> new Tuple( "field1", tuple.get( "field1" ) ) );

        final OperatorRuntimeSchemaBuilder mapperSchema = new OperatorRuntimeSchemaBuilder( 1, 1 );
        mapperSchema.addInputField( 0, "field1", Integer.class ).addOutputField( 0, "field1", Integer.class );

        final OperatorDef mapper = OperatorDefBuilder.newInstance( "mapper", MapperOperator.class )
                                                     .setConfig( mapperConfig )
                                                     .setExtendingSchema( mapperSchema )
                                                     .build();

        final OperatorConfig windowConfig = new OperatorConfig();
        windowConfig.set( TupleCountBasedWindowReducerOperator.INITIAL_VALUE_CONFIG_PARAMETER, new Tuple( "field1", 0 ) );
        windowConfig.set( TupleCountBasedWindowReducerOperator.TUPLE_COUNT_CONFIG_PARAMETER, 1 );
        windowConfig.set( TupleCountBasedWindowReducerOperator.REDUCER_CONFIG_PARAMETER,
                          (BiFunction<Tuple, Tuple, Tuple>) ( tuple1, tuple2 ) -> new Tuple( "field1",
                                                                                             tuple1.getInteger( "field1" )
                                                                                             + tuple2.getInteger( "field1" ) ) );

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

        zanza.start( flow );

        Uninterruptibles.sleepUninterruptibly( 30, TimeUnit.SECONDS );

        try
        {
            zanza.shutdown().get();
            System.out.println( "ZANZA FLOW COMPLETED" );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            System.out.println( "ZANZA FLOW INTERRUPTED" );
            e.printStackTrace();
        }
        catch ( ExecutionException e )
        {
            System.out.println( "ZANZA FLOW FAILED" );
            e.printStackTrace();
        }
    }

}
