package cs.bilkent.joker.experiment.wordcount;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import cs.bilkent.joker.operator.InitCtx;
import cs.bilkent.joker.operator.InvocationCtx;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenAvailable;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.annotation.OperatorSchema;
import cs.bilkent.joker.operator.schema.annotation.PortSchema;
import static cs.bilkent.joker.operator.schema.annotation.PortSchemaScope.EXACT_FIELD_SET;
import cs.bilkent.joker.operator.schema.annotation.SchemaField;
import cs.bilkent.joker.operator.schema.runtime.TupleSchema;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import static java.util.Collections.shuffle;

@OperatorSpec( type = STATEFUL, inputPortCount = 0, outputPortCount = 1 )
@OperatorSchema( outputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = SentenceBeaconOperator3.SENTENCE_FIELD, type = String.class ) } ) } )
public class SentenceBeaconOperator3 implements Operator
{

    public static final String MIN_WORD_LENGTH_PARAM = "minWordLength";

    public static final String MAX_WORD_LENGTH_PARAM = "maxWordLength";

    public static final String WORD_COUNT_PER_LENGTH_PARAM = "wordCountPerLength";

    public static final String MIN_SENTENCE_LENGTH_PARAM = "minSentenceLength";

    public static final String MAX_SENTENCE_LENGTH_PARAM = "maxSentenceLength";

    public static final String SENTENCE_COUNT_PER_LENGTH_PARAM = "sentenceCountPerLength";

    public static final String SENTENCE_COUNT_PER_INVOCATION_PARAM = "sentenceCountPerInvocation";

    public static final String SENTENCE_FIELD = "sentence";


    private int minWordLength, maxWordLength, wordCountPerLength;

    private int minSentenceLength, maxSentenceLength, sentenceCountPerLength;

    private List<String> sentences = new ArrayList<>();

    private TupleSchema outputSchema;

    private int sentenceCountPerInvocation;

    private int sentenceIndex;

    @Override
    public SchedulingStrategy init ( final InitCtx ctx )
    {
        this.outputSchema = ctx.getOutputPortSchema( 0 );

        final OperatorConfig config = ctx.getConfig();
        this.sentenceCountPerInvocation = config.getInteger( SENTENCE_COUNT_PER_INVOCATION_PARAM );
        this.minWordLength = config.getInteger( MIN_WORD_LENGTH_PARAM );
        this.maxWordLength = config.getInteger( MAX_WORD_LENGTH_PARAM );
        this.wordCountPerLength = config.getInteger( WORD_COUNT_PER_LENGTH_PARAM );
        this.minSentenceLength = config.getInteger( MIN_SENTENCE_LENGTH_PARAM );
        this.maxSentenceLength = config.getInteger( MAX_SENTENCE_LENGTH_PARAM );
        this.sentenceCountPerLength = config.getInteger( SENTENCE_COUNT_PER_LENGTH_PARAM );

        initSentences();

        return ScheduleWhenAvailable.INSTANCE;
    }

    @Override
    public void invoke ( final InvocationCtx ctx )
    {
        for ( int i = 0; i < sentenceCountPerInvocation; i++ )
        {
            final Tuple result = Tuple.of( outputSchema, SENTENCE_FIELD, sentences.get( sentenceIndex++ ) );

            if ( sentenceIndex == sentences.size() )
            {
                sentenceIndex = 0;
            }

            ctx.output( result );
        }
    }

    private void initSentences ()
    {
        //        char[] letters = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
        //        'u', 'v',
        //                           'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
        //                           'Q', 'R',
        //                           'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };
        char[] letters = { 'a',
                           'b',
                           'c',
                           'd',
                           'e',
                           'f',
                           'g',
                           'h',
                           'i',
                           'j',
                           'k',
                           'l',
                           'm',
                           'n',
                           'o',
                           'p',
                           'q',
                           'r',
                           's',
                           't',
                           'u',
                           'v',
                           'w',
                           'x',
                           'y',
                           'z' };
        Random random = new Random();
        List<String> words = new ArrayList<>();
        for ( int len = minWordLength; len <= maxWordLength; len++ )
        {
            Set<String> w = new HashSet<>();

            while ( w.size() < wordCountPerLength )
            {
                StringBuilder sb = new StringBuilder();
                for ( int i = 0; i < len; i++ )
                {
                    sb.append( letters[ random.nextInt( letters.length ) ] );
                }

                w.add( sb.toString() );
            }

            words.addAll( w );
        }

        shuffle( words );

        List<Integer> sentenceLengths = new ArrayList<>();
        for ( int len = minSentenceLength; len <= maxSentenceLength; len++ )
        {
            for ( int j = 0; j < sentenceCountPerLength; j++ )
            {
                sentenceLengths.add( len );
            }
        }

        shuffle( sentenceLengths );

        for ( int i = 0, wordIdx = 0; i < sentenceLengths.size(); i++ )
        {
            final int sentenceLength = sentenceLengths.get( i++ );
            final StringBuilder sb = new StringBuilder();
            for ( int j = 0; j < sentenceLength; j++ )
            {
                final String word = words.get( wordIdx++ );
                if ( wordIdx == words.size() )
                {
                    wordIdx = 0;
                }

                sb.append( word );

                if ( j < ( sentenceLength - 1 ) )
                {
                    sb.append( " " );
                }
            }

            sentences.add( sb.toString().trim() );
        }
    }

}
