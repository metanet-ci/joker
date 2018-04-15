package cs.bilkent.joker.engine.pipeline.impl.invocation;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkNotNull;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import static cs.bilkent.joker.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.TupleAccessor;
import cs.bilkent.joker.operator.impl.InternalInvocationCtx;
import cs.bilkent.joker.operator.impl.OutputCollector;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.kvstore.KVStore;
import cs.bilkent.joker.partition.impl.PartitionKey;
import gnu.trove.map.hash.TObjectIntHashMap;
import static java.util.Arrays.copyOf;

public class FusedPartitionedInvocationCtx implements InternalInvocationCtx, OutputCollector
{

    private static final int NA = -1;


    private final int inputPortCount;

    private final Function<PartitionKey, KVStore> kvStoreSupplier;

    private final List<TuplesImpl> inputs = new ArrayList<>();

    private final List<PartitionKey> partitionKeys = new ArrayList<>();

    private final PartitionKeyExtractor partitionKeyExtractor;

    private final TObjectIntHashMap<PartitionKey> partitionKeyInputIndices = new TObjectIntHashMap<>( 16, 0.75f, NA );

    private final OutputCollector outputCollector;

    private int inputCount;

    private int currentInput = 0;

    private InvocationReason reason;

    private boolean[] upstreamConnectionStatuses;

    public FusedPartitionedInvocationCtx ( final int inputPortCount,
                                           final Function<PartitionKey, KVStore> kvStoreSupplier,
                                           final PartitionKeyExtractor partitionKeyExtractor,
                                           final OutputCollector outputCollector )
    {
        this.inputPortCount = inputPortCount;
        this.kvStoreSupplier = kvStoreSupplier;
        this.partitionKeyExtractor = partitionKeyExtractor;
        this.outputCollector = outputCollector;
    }

    // InternalInvocationContext methods begin

    @Override
    public void setInvocationReason ( final InvocationReason reason )
    {
        checkNotNull( reason );
        this.reason = reason;
    }

    @Override
    public void reset ()
    {
        this.reason = null;
        for ( TuplesImpl input : inputs )
        {
            input.clear();
        }
        partitionKeys.clear();
        outputCollector.clear();
        partitionKeyInputIndices.clear();
        inputCount = 0;
        currentInput = 0;
    }

    @Override
    public int getInputCount ()
    {
        return inputCount;
    }

    @Override
    public boolean nextInput ()
    {
        return ( ++currentInput < inputCount );
    }

    @Override
    public void setUpstreamConnectionStatuses ( final boolean[] upstreamConnectionStatuses )
    {
        this.upstreamConnectionStatuses = copyOf( upstreamConnectionStatuses, upstreamConnectionStatuses.length );
    }

    @Override
    public List<TuplesImpl> getInputs ()
    {
        return inputs;
    }

    @Override
    public OutputCollector getOutputCollector ()
    {
        return outputCollector;
    }

    // InternalInvocationContext methods end

    // InvocationContext methods begin

    @Override
    public List<Tuple> getInputTuples ( final int portIndex )
    {
        return getInput().getTuples( portIndex );
    }

    @Override
    public Tuple getInputTupleOrNull ( final int portIndex, final int tupleIndex )
    {
        return getInput().getTupleOrNull( portIndex, tupleIndex );
    }

    @Override
    public int getInputTupleCount ( final int portIndex )
    {
        return getInput().getTupleCount( portIndex );
    }

    @Override
    public void output ( final Tuple tuple )
    {
        outputCollector.add( tuple );
    }

    @Override
    public void output ( final List<Tuple> tuples )
    {
        for ( int i = 0, j = tuples.size(); i < j; i++ )
        {
            outputCollector.add( tuples.get( i ) );
        }
    }

    @Override
    public void output ( final int portIndex, final Tuple tuple )
    {
        outputCollector.add( portIndex, tuple );
    }

    @Override
    public void output ( final int portIndex, final List<Tuple> tuples )
    {
        for ( int i = 0, j = tuples.size(); i < j; i++ )
        {
            outputCollector.add( portIndex, tuples.get( i ) );
        }
    }

    @Override
    public InvocationReason getReason ()
    {
        return reason;
    }

    @Override
    public boolean isInputPortOpen ( final int portIndex )
    {
        return upstreamConnectionStatuses[ portIndex ];
    }

    @Override
    public PartitionKey getPartitionKey ()
    {
        return partitionKeys.get( currentInput );
    }

    @Override
    public KVStore getKVStore ()
    {
        return kvStoreSupplier.apply( getPartitionKey() );
    }

    public TuplesImpl getInput ()
    {
        return inputs.get( currentInput );
    }

    // InvocationContext methods end

    // OutputTuplesSupplier methods begin

    @Override
    public void add ( final Tuple tuple )
    {
        addOutputTuple( DEFAULT_PORT_INDEX, tuple );
    }

    @Override
    public void add ( final int portIndex, final Tuple tuple )
    {
        addOutputTuple( portIndex, tuple );
    }

    @Override
    public void recordInvocationLatency ( final String operatorId, final long latency )
    {
        for ( int i = 0; i < inputCount; i++ )
        {
            final TuplesImpl input = inputs.get( i );
            for ( int j = 0; j < input.getPortCount(); j++ )
            {
                final List<Tuple> tuples = input.getTuplesModifiable( j );
                for ( int k = 0; k < tuples.size(); k++ )
                {
                    TupleAccessor.recordInvocationLatency( tuples.get( k ), operatorId, latency );
                }
            }
        }
    }

    @Override
    public TuplesImpl getOutputTuples ()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear ()
    {
        // no need to implement
    }

    private void addOutputTuple ( final int portIndex, final Tuple tuple )
    {
        final PartitionKey partitionKey = partitionKeyExtractor.getPartitionKey( tuple );
        final int idx = partitionKeyInputIndices.get( partitionKey );
        if ( idx == NA )
        {
            partitionKeyInputIndices.put( partitionKey, partitionKeyInputIndices.size() );
            createOutputTuples( partitionKey ).add( portIndex, tuple );
            return;
        }

        inputs.get( idx ).add( portIndex, tuple );
    }

    private TuplesImpl createOutputTuples ( final PartitionKey partitionKey )
    {
        partitionKeys.add( partitionKey );
        if ( inputs.size() <= inputCount )
        {
            inputs.add( new TuplesImpl( inputPortCount ) );
        }

        return inputs.get( inputCount++ );
    }

    // OutputTuplesSupplier methods end
}
