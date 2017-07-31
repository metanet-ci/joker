package cs.bilkent.joker.experiment.wordcount;

import com.typesafe.config.Config;

import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.experiment.FlowDefFactory;
import static cs.bilkent.joker.experiment.wordcount.SentenceBeaconOperator.MAX_PARTITION_INDEX_PARAM;
import static cs.bilkent.joker.experiment.wordcount.SentenceBeaconOperator.MAX_SENTENCE_LENGTH_PARAM;
import static cs.bilkent.joker.experiment.wordcount.SentenceBeaconOperator.MAX_WORD_LENGTH_PARAM;
import static cs.bilkent.joker.experiment.wordcount.SentenceBeaconOperator.MIN_SENTENCE_LENGTH_PARAM;
import static cs.bilkent.joker.experiment.wordcount.SentenceBeaconOperator.MIN_WORD_LENGTH_PARAM;
import static cs.bilkent.joker.experiment.wordcount.SentenceBeaconOperator.PARTITION_INDEX_FIELD;
import static cs.bilkent.joker.experiment.wordcount.SentenceBeaconOperator.SENTENCE_COUNT_PER_INVOCATION_PARAM;
import static cs.bilkent.joker.experiment.wordcount.SentenceBeaconOperator.SENTENCE_COUNT_PER_LENGTH_PARAM;
import static cs.bilkent.joker.experiment.wordcount.SentenceBeaconOperator.SENTENCE_FIELD;
import static cs.bilkent.joker.experiment.wordcount.SentenceBeaconOperator.SHUFFLED_SENTENCE_COUNT_PARAM;
import static cs.bilkent.joker.experiment.wordcount.SentenceBeaconOperator.WORD_COUNT_PER_LENGTH_PARAM;
import static cs.bilkent.joker.experiment.wordcount.SentenceSplitterFunction.WORD_FIELD;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchemaBuilder;
import cs.bilkent.joker.operators.FlatMapperOperator;
import static java.util.Collections.singletonList;

public class WordCountFlowDefFactory2 implements FlowDefFactory
{

    @Override
    public FlowDef createFlow ( final JokerConfig jokerConfig )
    {
        final Config config = jokerConfig.getRootConfig();
        final int minWordLength = config.getInt( MIN_WORD_LENGTH_PARAM );
        final int maxWordLength = config.getInt( MAX_WORD_LENGTH_PARAM );
        final int wordCountPerLength = config.getInt( WORD_COUNT_PER_LENGTH_PARAM );
        final int minSentenceLength = config.getInt( MIN_SENTENCE_LENGTH_PARAM );
        final int maxSentenceLength = config.getInt( MAX_SENTENCE_LENGTH_PARAM );
        final int sentenceCountPerLength = config.getInt( SENTENCE_COUNT_PER_LENGTH_PARAM );
        final int shuffledSentenceCount = config.getInt( SHUFFLED_SENTENCE_COUNT_PARAM );
        final int maxPartitionIndex = config.getInt( MAX_PARTITION_INDEX_PARAM );
        final int sentenceCountPerInvocation = config.getInt( SENTENCE_COUNT_PER_INVOCATION_PARAM );

        final OperatorConfig sentenceBeaconConfig = new OperatorConfig();
        sentenceBeaconConfig.set( MIN_WORD_LENGTH_PARAM, minWordLength );
        sentenceBeaconConfig.set( MAX_WORD_LENGTH_PARAM, maxWordLength );
        sentenceBeaconConfig.set( WORD_COUNT_PER_LENGTH_PARAM, wordCountPerLength );
        sentenceBeaconConfig.set( MIN_SENTENCE_LENGTH_PARAM, minSentenceLength );
        sentenceBeaconConfig.set( MAX_SENTENCE_LENGTH_PARAM, maxSentenceLength );
        sentenceBeaconConfig.set( SENTENCE_COUNT_PER_LENGTH_PARAM, sentenceCountPerLength );
        sentenceBeaconConfig.set( SHUFFLED_SENTENCE_COUNT_PARAM, shuffledSentenceCount );
        sentenceBeaconConfig.set( MAX_PARTITION_INDEX_PARAM, maxPartitionIndex );
        sentenceBeaconConfig.set( SENTENCE_COUNT_PER_INVOCATION_PARAM, sentenceCountPerInvocation );

        final OperatorDef sentenceBeaconOp = OperatorDefBuilder.newInstance( "sb", SentenceBeaconOperator.class )
                                                               .setConfig( sentenceBeaconConfig )
                                                               .build();

        final OperatorConfig flatMapperConfig = new OperatorConfig();
        flatMapperConfig.set( FlatMapperOperator.FLAT_MAPPER_CONFIG_PARAMETER, new SentenceSplitterFunction() );

        final OperatorRuntimeSchemaBuilder flatMapperSchemaBuilder = new OperatorRuntimeSchemaBuilder( 1, 1 );
        flatMapperSchemaBuilder.addInputField( 0, SENTENCE_FIELD, String.class )
                               .addInputField( 0, PARTITION_INDEX_FIELD, Integer.class )
                               .addOutputField( 0, WORD_FIELD, String.class );

        final OperatorDef sentenceSplitterOp = OperatorDefBuilder.newInstance( "ss", FlatMapperOperator.class )
                                                                 .setConfig( flatMapperConfig )
                                                                 .setExtendingSchema( flatMapperSchemaBuilder )
                                                                 .build();

        final OperatorRuntimeSchemaBuilder wordCounterSchemaBuilder = new OperatorRuntimeSchemaBuilder( 1, 1 );
        wordCounterSchemaBuilder.addInputField( 0, WORD_FIELD, String.class )
                                .addOutputField( 0, WORD_FIELD, String.class )
                                .addOutputField( 0, CounterOperator.COUNT_FIELD, Integer.class );

        final OperatorDef wordCounterOp = OperatorDefBuilder.newInstance( "wc", CounterOperator.class )
                                                            .setExtendingSchema( wordCounterSchemaBuilder )
                                                            .setPartitionFieldNames( singletonList( WORD_FIELD ) )
                                                            .build();

        return new FlowDefBuilder().add( sentenceBeaconOp )
                                   .add( sentenceSplitterOp )
                                   .add( wordCounterOp )
                                   .connect( sentenceBeaconOp.getId(), sentenceSplitterOp.getId() )
                                   .connect( sentenceSplitterOp.getId(), wordCounterOp.getId() )
                                   .build();
    }

}
