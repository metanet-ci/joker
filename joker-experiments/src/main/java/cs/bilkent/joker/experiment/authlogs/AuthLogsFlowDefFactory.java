package cs.bilkent.joker.experiment.authlogs;

import java.util.Collections;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.typesafe.config.Config;

import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.experiment.FlowDefFactory;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchemaBuilder;
import cs.bilkent.joker.operators.FilterOperator;
import cs.bilkent.joker.operators.MapperOperator;
import cs.bilkent.joker.operators.TupleCountBasedWindowReducerOperator;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class AuthLogsFlowDefFactory implements FlowDefFactory
{

    private static final String UID_FIELD_NAME = "uid";
    private static final String EUID_FIELD_NAME = "euid";
    private static final String TTY_FIELD_NAME = "tty";
    private static final String RHOST_FIELD_NAME = "rhost";
    private static final String USER_FIELD_NAME = "user";

    private static final String MAX_TIMESTAMP_FIELD_NAME = "maxTimestamp";
    private static final String MIN_TIMESTAMP_FIELD_NAME = "minTimestamp";
    private static final String LAST_FAILURE_FIELD_NAME = "last";
    private static final String DIFF_FIELD_NAME = "diff";

    private static final int FAILURE_WINDOW_TUPLE_COUNT = 5;

    @Override
    public FlowDef createFlow ( final JokerConfig jokerConfig )
    {
        final Config config = jokerConfig.getRootConfig();
        final int failureWindowDurationInSeconds = config.getInt( "failureWindowDurationInSeconds" );

        final OperatorConfig logBeaconConfig = new OperatorConfig();
        logBeaconConfig.set( "filePath", config.getString( "filePath" ) );
        logBeaconConfig.set( "batchSize", config.getInt( "batchSize" ) );
        logBeaconConfig.set( "uidRange", config.getInt( "uidRange" ) );
        logBeaconConfig.set( "euidRange", config.getInt( "euidRange" ) );
        logBeaconConfig.set( "rhostCount", config.getInt( "rhostCount" ) );
        logBeaconConfig.set( "userCount", config.getInt( "userCount" ) );
        logBeaconConfig.set( "authFailureRatio", config.getDouble( "authFailureRatio" ) );
        logBeaconConfig.set( "logsPerSecond", config.getInt( "logsPerSecond" ) );
        logBeaconConfig.set( "tuplesPerInvocation", config.getInt( "tuplesPerInvocation" ) );

        final OperatorDef logBeacon = OperatorDefBuilder.newInstance( "logBeacon", LogBeaconOperator.class )
                                                        .setConfig( logBeaconConfig )
                                                        .build();

        final OperatorRuntimeSchemaBuilder authFailureFilterSchema = new OperatorRuntimeSchemaBuilder( 1, 1 );
        authFailureFilterSchema.addInputField( 0, LogBeaconOperator.TIMESTAMP_FIELD_NAME, Long.class )
                               .addInputField( 0, LogBeaconOperator.HOST_FIELD_NAME, String.class )
                               .addInputField( 0, LogBeaconOperator.SERVICE_FIELD_NAME, String.class )
                               .addInputField( 0, LogBeaconOperator.MESSAGE_FIELD_NAME, String.class )
                               .addOutputField( 0, LogBeaconOperator.TIMESTAMP_FIELD_NAME, Long.class )
                               .addOutputField( 0, LogBeaconOperator.HOST_FIELD_NAME, String.class )
                               .addOutputField( 0, LogBeaconOperator.SERVICE_FIELD_NAME, String.class )
                               .addOutputField( 0, LogBeaconOperator.MESSAGE_FIELD_NAME, String.class );

        final Predicate<Tuple> authFailureFilterPredicate = tuple -> ( tuple.getString( LogBeaconOperator.SERVICE_FIELD_NAME )
                                                                            .contains( "sshd" )
                                                                       && tuple.getString( LogBeaconOperator.MESSAGE_FIELD_NAME )
                                                                               .contains( "authentication failure;" ) );

        final OperatorConfig authFailureFilterConfig = new OperatorConfig();
        authFailureFilterConfig.set( FilterOperator.PREDICATE_CONFIG_PARAMETER, authFailureFilterPredicate );

        final OperatorDef authFailureFilter = OperatorDefBuilder.newInstance( "authFailureFilter", FilterOperator.class )
                                                                .setExtendingSchema( authFailureFilterSchema )
                                                                .setConfig( authFailureFilterConfig )
                                                                .build();

        final OperatorRuntimeSchemaBuilder failureParserSchema = new OperatorRuntimeSchemaBuilder( 1, 1 );
        failureParserSchema.addInputField( 0, LogBeaconOperator.TIMESTAMP_FIELD_NAME, Long.class )
                           .addInputField( 0, LogBeaconOperator.HOST_FIELD_NAME, String.class )
                           .addInputField( 0, LogBeaconOperator.SERVICE_FIELD_NAME, String.class )
                           .addInputField( 0, LogBeaconOperator.MESSAGE_FIELD_NAME, String.class )
                           .addOutputField( 0, LogBeaconOperator.TIMESTAMP_FIELD_NAME, Long.class )
                           .addOutputField( 0, UID_FIELD_NAME, String.class )
                           .addOutputField( 0, EUID_FIELD_NAME, String.class )
                           .addOutputField( 0, TTY_FIELD_NAME, String.class )
                           .addOutputField( 0, RHOST_FIELD_NAME, String.class )
                           .addOutputField( 0, USER_FIELD_NAME, String.class );

        final BiConsumer<Tuple, Tuple> failureParserFunc = ( input, output ) -> {
            output.set( LogBeaconOperator.TIMESTAMP_FIELD_NAME, input.getLong( LogBeaconOperator.TIMESTAMP_FIELD_NAME ) );
            final String message = input.getString( LogBeaconOperator.MESSAGE_FIELD_NAME );
            final String[] tokens = message.split( " " );
            output.set( UID_FIELD_NAME, tokens[ 4 ].split( "=" )[ 1 ] );
            output.set( EUID_FIELD_NAME, tokens[ 5 ].split( "=" )[ 1 ] );
            output.set( TTY_FIELD_NAME, tokens[ 6 ].split( "=" )[ 1 ] );
            output.set( RHOST_FIELD_NAME, tokens[ 8 ].split( "=" )[ 1 ] );
            output.set( USER_FIELD_NAME, tokens[ 9 ].split( "=" )[ 1 ] );
        };

        final OperatorConfig failureParserConfig = new OperatorConfig();
        failureParserConfig.set( MapperOperator.MAPPER_CONFIG_PARAMETER, failureParserFunc );

        final OperatorDef failureParser = OperatorDefBuilder.newInstance( "failureParser", MapperOperator.class )
                                                            .setExtendingSchema( failureParserSchema )
                                                            .setConfig( failureParserConfig )
                                                            .build();

        final OperatorRuntimeSchemaBuilder failureWindowSchema = new OperatorRuntimeSchemaBuilder( 1, 1 );
        failureWindowSchema.addInputField( 0, LogBeaconOperator.TIMESTAMP_FIELD_NAME, Long.class )
                           .addInputField( 0, UID_FIELD_NAME, String.class )
                           .addInputField( 0, EUID_FIELD_NAME, String.class )
                           .addInputField( 0, TTY_FIELD_NAME, String.class )
                           .addInputField( 0, RHOST_FIELD_NAME, String.class )
                           .addInputField( 0, USER_FIELD_NAME, String.class )
                           .addOutputField( 0, RHOST_FIELD_NAME, String.class )
                           .addOutputField( 0, MAX_TIMESTAMP_FIELD_NAME, Long.class )
                           .addOutputField( 0, MIN_TIMESTAMP_FIELD_NAME, Long.class )
                           .addOutputField( 0, USER_FIELD_NAME, String.class );

        final BiConsumer<Tuple, Tuple> windowReducerFunc = ( accumulator, input ) -> {
            final long maxTimestamp = accumulator.getLongOrDefault( MAX_TIMESTAMP_FIELD_NAME, Long.MIN_VALUE );
            final long minTimestamp = accumulator.getLongOrDefault( MIN_TIMESTAMP_FIELD_NAME, Long.MAX_VALUE );

            accumulator.set( MAX_TIMESTAMP_FIELD_NAME, max( maxTimestamp, input.getLong( LogBeaconOperator.TIMESTAMP_FIELD_NAME ) ) );
            accumulator.set( MIN_TIMESTAMP_FIELD_NAME, min( minTimestamp, input.getLong( LogBeaconOperator.TIMESTAMP_FIELD_NAME ) ) );
            accumulator.set( RHOST_FIELD_NAME, input.get( RHOST_FIELD_NAME ) );
            accumulator.set( USER_FIELD_NAME, input.get( USER_FIELD_NAME ) );
        };

        final OperatorConfig failureWindowConfig = new OperatorConfig();
        failureWindowConfig.set( TupleCountBasedWindowReducerOperator.REDUCER_CONFIG_PARAMETER, windowReducerFunc );
        failureWindowConfig.set( TupleCountBasedWindowReducerOperator.ACCUMULATOR_INITIALIZER_CONFIG_PARAMETER, (Consumer<Tuple>) t -> {
        } );
        failureWindowConfig.set( TupleCountBasedWindowReducerOperator.TUPLE_COUNT_CONFIG_PARAMETER, FAILURE_WINDOW_TUPLE_COUNT );

        final OperatorDef failureWindow = OperatorDefBuilder.newInstance( "failureWindow", TupleCountBasedWindowReducerOperator.class )
                                                            .setExtendingSchema( failureWindowSchema )
                                                            .setPartitionFieldNames( Collections.singletonList( RHOST_FIELD_NAME ) )
                                                            .setConfig( failureWindowConfig )
                                                            .build();

        final OperatorRuntimeSchemaBuilder cutOffSchema = new OperatorRuntimeSchemaBuilder( 1, 1 );
        cutOffSchema.addInputField( 0, RHOST_FIELD_NAME, String.class )
                    .addInputField( 0, MAX_TIMESTAMP_FIELD_NAME, Long.class )
                    .addInputField( 0, MIN_TIMESTAMP_FIELD_NAME, Long.class )
                    .addInputField( 0, USER_FIELD_NAME, String.class )
                    .addOutputField( 0, RHOST_FIELD_NAME, String.class )
                    .addOutputField( 0, MAX_TIMESTAMP_FIELD_NAME, Long.class )
                    .addOutputField( 0, MIN_TIMESTAMP_FIELD_NAME, Long.class )
                    .addOutputField( 0, USER_FIELD_NAME, String.class );

        final Predicate<Tuple> cutOffPredicate = input -> ( input.getLong( MAX_TIMESTAMP_FIELD_NAME ) - SECONDS.toMillis(
                failureWindowDurationInSeconds ) ) <= input.getLong( MIN_TIMESTAMP_FIELD_NAME );

        final OperatorConfig cutOffConfig = new OperatorConfig();
        cutOffConfig.set( FilterOperator.PREDICATE_CONFIG_PARAMETER, cutOffPredicate );

        final OperatorDef cutOff = OperatorDefBuilder.newInstance( "cutOff", FilterOperator.class )
                                                     .setExtendingSchema( cutOffSchema )
                                                     .setConfig( cutOffConfig )
                                                     .build();

        final OperatorRuntimeSchemaBuilder diffCalculatorSchema = new OperatorRuntimeSchemaBuilder( 1, 1 );

        diffCalculatorSchema.addInputField( 0, RHOST_FIELD_NAME, String.class )
                            .addInputField( 0, MAX_TIMESTAMP_FIELD_NAME, Long.class )
                            .addInputField( 0, MIN_TIMESTAMP_FIELD_NAME, Long.class )
                            .addInputField( 0, USER_FIELD_NAME, String.class )
                            .addOutputField( 0, RHOST_FIELD_NAME, String.class )
                            .addOutputField( 0, LAST_FAILURE_FIELD_NAME, Long.class )
                            .addOutputField( 0, DIFF_FIELD_NAME, Long.class );

        final BiConsumer<Tuple, Tuple> diffCalculatorFunc = ( input, output ) -> {
            output.set( RHOST_FIELD_NAME, input.get( RHOST_FIELD_NAME ) );
            output.set( LAST_FAILURE_FIELD_NAME, input.get( MAX_TIMESTAMP_FIELD_NAME ) );
            final long timeDiff = input.getLong( MAX_TIMESTAMP_FIELD_NAME ) - input.getLong( MIN_TIMESTAMP_FIELD_NAME );
            output.set( DIFF_FIELD_NAME, MILLISECONDS.toSeconds( timeDiff ) );
        };

        final OperatorConfig diffCalculatorConfig = new OperatorConfig();
        diffCalculatorConfig.set( MapperOperator.MAPPER_CONFIG_PARAMETER, diffCalculatorFunc );

        final OperatorDef diffCalculator = OperatorDefBuilder.newInstance( "diffCalculator", MapperOperator.class )
                                                             .setExtendingSchema( diffCalculatorSchema )
                                                             .setConfig( diffCalculatorConfig )
                                                             .build();

        return new FlowDefBuilder().add( logBeacon )
                                   .add( authFailureFilter )
                                   .add( failureParser )
                                   .add( failureWindow )
                                   .add( cutOff )
                                   .add( diffCalculator ).connect( logBeacon.getId(), authFailureFilter.getId() )
                                   .connect( authFailureFilter.getId(), failureParser.getId() )
                                   .connect( failureParser.getId(), failureWindow.getId() )
                                   .connect( failureWindow.getId(), cutOff.getId() )
                                   .connect( cutOff.getId(), diffCalculator.getId() )
                                   .build();
    }

}
