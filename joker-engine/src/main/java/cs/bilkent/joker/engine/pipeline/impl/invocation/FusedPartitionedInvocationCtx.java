package cs.bilkent.joker.engine.pipeline.impl.invocation;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkNotNull;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import static cs.bilkent.joker.flow.Port.DEFAULT_PORT_INDEX;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkArgument;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.Tuple.LatencyRecord;
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

    private LatencyRecord latencyRec;

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
        reason = null;
        for ( TuplesImpl input : inputs )
        {
            input.clear();
        }
        partitionKeys.clear();
        outputCollector.clear();
        partitionKeyInputIndices.clear();
        inputCount = 0;
        currentInput = 0;
        latencyRec = null;
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
    public TuplesImpl getOutput ()
    {
        return outputCollector.getOutputTuples();
    }

    @Override
    public void setInvocationLatencyRecord ( final LatencyRecord latencyRec )
    {
        checkArgument( latencyRec != null );
        checkState( this.latencyRec == null );
        this.latencyRec = latencyRec;
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
        if ( latencyRec != null )
        {
            tuple.addInvocationLatencyRecord( latencyRec );
        }

        outputCollector.add( tuple );
    }

    @Override
    public void output ( final int portIndex, final Tuple tuple )
    {
        if ( latencyRec != null )
        {
            tuple.addInvocationLatencyRecord( latencyRec );
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
