package cs.bilkent.joker.operator.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkNotNull;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.kvstore.KVStore;
import cs.bilkent.joker.partition.impl.PartitionKey;
import static java.util.Arrays.copyOf;


public class DefaultInvocationCtx implements InternalInvocationCtx
{

    private static final Tuple TRACK_OUTPUT_TUPLE_MARKER = new Tuple();


    private final int inputPortCount;

    private final Function<PartitionKey, KVStore> kvStoreSupplier;

    private final List<TuplesImpl> inputs = new ArrayList<>();

    private final List<PartitionKey> partitionKeys = new ArrayList<>();

    private final OutputCollector outputCollector;

    private InvocationReason reason;

    private boolean[] upstreamConnectionStatuses;

    private int inputCount;

    private int currentInput = 0;

    private Tuple trackedOutputTuple;

    public DefaultInvocationCtx ( final int inputPortCount, final Function<PartitionKey, KVStore> kvStoreSupplier, final TuplesImpl output )
    {
        this( inputPortCount, kvStoreSupplier, new OutputCollector()
        {
            @Override
            public void add ( final Tuple tuple )
            {
                output.add( tuple );
            }

            @Override
            public void add ( final int portIndex, final Tuple tuple )
            {
                output.add( portIndex, tuple );
            }

            @Override
            public TuplesImpl getOutputTuples ()
            {
                return output;
            }

            @Override
            public void clear ()
            {
                output.clear();
            }
        } );
    }

    public DefaultInvocationCtx ( final int inputPortCount,
                                  final Function<PartitionKey, KVStore> kvStoreSupplier,
                                  final OutputCollector outputCollector )
    {
        this.inputPortCount = inputPortCount;
        this.kvStoreSupplier = kvStoreSupplier;
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
        reason = null;
        for ( TuplesImpl input : inputs )
        {
            input.clear();
        }
        partitionKeys.clear();
        outputCollector.clear();
        inputCount = 0;
        currentInput = 0;
        trackedOutputTuple = null;
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
    public TuplesImpl getOutput ()
    {
        return outputCollector.getOutputTuples();
    }

    @Override
    public void setUpstreamConnectionStatuses ( final boolean[] upstreamConnectionStatuses )
    {
        this.upstreamConnectionStatuses = copyOf( upstreamConnectionStatuses, upstreamConnectionStatuses.length );
    }

    @Override
    public void trackOutputTuple ()
    {
        trackedOutputTuple = TRACK_OUTPUT_TUPLE_MARKER;
    }

    @Override
    public Tuple getTrackedOutputTuple ()
    {
        return trackedOutputTuple != TRACK_OUTPUT_TUPLE_MARKER ? trackedOutputTuple : null;
    }

    // InternalInvocationContext methods end

    // InvocationContext methods begin

    @Override
    public List<Tuple> getInputTuples ( int portIndex )
    {
        return getInput().getTuples( portIndex );
    }

    @Override
    public Tuple getInputTupleOrNull ( int portIndex, int tupleIndex )
    {
        return getInput().getTupleOrNull( portIndex, tupleIndex );
    }

    @Override
    public int getInputTupleCount ( int portIndex )
    {
        return getInput().getTupleCount( portIndex );
    }

    @Override
    public void output ( final Tuple tuple )
    {
        if ( trackedOutputTuple == TRACK_OUTPUT_TUPLE_MARKER )
        {
            trackedOutputTuple = tuple;
        }

        outputCollector.add( tuple );
    }

    @Override
    public void output ( final int portIndex, final Tuple tuple )
    {
        if ( trackedOutputTuple == TRACK_OUTPUT_TUPLE_MARKER )
        {
            trackedOutputTuple = tuple;
        }

        outputCollector.add( portIndex, tuple );
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

    public TuplesImpl createInputTuples ( final PartitionKey partitionKey )
    {
        partitionKeys.add( partitionKey );
        if ( inputs.size() <= inputCount )
        {
            inputs.add( new TuplesImpl( inputPortCount ) );
        }

        return inputs.get( inputCount++ );
    }

    public TuplesImpl getInput ()
    {
        return inputs.get( currentInput );
    }

    public List<TuplesImpl> getInputs ()
    {
        return inputs;
    }

    // InvocationContext methods end

}
