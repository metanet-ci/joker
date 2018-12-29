package cs.bilkent.joker.engine.pipeline.impl.invocation;

import java.util.List;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;

import static com.google.common.base.Preconditions.checkNotNull;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.InternalInvocationCtx;
import cs.bilkent.joker.operator.impl.OutputCollector;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.kvstore.KVStore;
import cs.bilkent.joker.partition.impl.PartitionKey;
import static java.util.Arrays.copyOf;

public class FusedInvocationCtx implements InternalInvocationCtx, OutputCollector
{

    private static final Tuple TRACK_OUTPUT_TUPLE_MARKER = new Tuple();


    private final Function<PartitionKey, KVStore> kvStoreSupplier;

    private final TuplesImpl input;

    private final List<TuplesImpl> inputs;

    private final OutputCollector outputCollector;

    private InvocationReason reason;

    private boolean[] upstreamConnectionStatuses;

    private Tuple trackedOutputTuple;

    public FusedInvocationCtx ( final int inputPortCount,
                                final Function<PartitionKey, KVStore> kvStoreSupplier,
                                final OutputCollector outputCollector )
    {
        this.input = new TuplesImpl( inputPortCount );
        this.inputs = ImmutableList.of( input );
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
        input.clear();
        outputCollector.clear();
        trackedOutputTuple = null;
    }

    @Override
    public int getInputCount ()
    {
        return input.isNonEmpty() ? 1 : 0;
    }

    @Override
    public boolean nextInput ()
    {
        return false;
    }

    @Override
    public List<TuplesImpl> getInputs ()
    {
        return inputs;
    }

    @Override
    public void setUpstreamConnectionStatuses ( final boolean[] upstreamConnectionStatuses )
    {
        this.upstreamConnectionStatuses = copyOf( upstreamConnectionStatuses, upstreamConnectionStatuses.length );
    }

    @Override
    public TuplesImpl getOutput ()
    {
        return outputCollector.getOutputTuples();
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
    public List<Tuple> getInputTuples ( final int portIndex )
    {
        return input.getTuples( portIndex );
    }

    @Override
    public Tuple getInputTupleOrNull ( final int portIndex, final int tupleIndex )
    {
        return input.getTupleOrNull( portIndex, tupleIndex );
    }

    @Override
    public int getInputTupleCount ( final int portIndex )
    {
        return input.getTupleCount( portIndex );
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
    public List<Object> getPartitionKey ()
    {
        return null;
    }

    @Override
    public KVStore getKVStore ()
    {
        return kvStoreSupplier.apply( null );
    }

    // InvocationContext methods end

    // OutputTuplesSupplier methods begin

    @Override
    public void add ( final Tuple tuple )
    {
        input.add( tuple );
    }

    @Override
    public void add ( final int portIndex, final Tuple tuple )
    {
        input.add( portIndex, tuple );
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

    // OutputTuplesSupplier methods end

}
