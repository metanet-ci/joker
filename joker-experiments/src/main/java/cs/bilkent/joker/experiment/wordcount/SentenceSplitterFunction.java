package cs.bilkent.joker.experiment.wordcount;

import java.util.function.Consumer;
import java.util.function.Supplier;

import static cs.bilkent.joker.experiment.wordcount.SentenceBeaconOperator.SENTENCE_FIELD;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operators.FlatMapperOperator.FlatMapperConsumer;

public class SentenceSplitterFunction implements FlatMapperConsumer
{

    public static final String WORD_FIELD = "word";


    @Override
    public void accept ( final Tuple input, final Supplier<Tuple> outputTupleSupplier, final Consumer<Tuple> outputCollector )
    {
        final String sentence = input.getString( SENTENCE_FIELD );
        for ( String word : sentence.trim().split( " " ) )
        {
            final Tuple output = outputTupleSupplier.get();
            output.set( WORD_FIELD, word );

            outputCollector.accept( output );
        }
    }

}
