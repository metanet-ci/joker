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

    private final int inputPortCount;

    private final Function<PartitionKey, KVStore> kvStoreSupplier;

    private final List<TuplesImpl> inputs = new ArrayList<>();

    private final List<PartitionKey> partitionKeys = new ArrayList<>();

    private final OutputCollector outputCollector;

    private InvocationReason reason;

    private boolean[] upstreamConnectionStatuses;

    private int inputCount;

    private int currentInput = 0;

    public DefaultInvocationCtx ( final int inputPortCount, final Function<PartitionKey, KVStore> kvStoreSupplier, final TuplesImpl output )
    {
        this( inputPortCount, kvStoreSupplier, new DefaultOutputCollector( output ) );
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
        this.reason = null;
        for ( TuplesImpl input : inputs )
        {
            input.clear();
        }
        partitionKeys.clear();
        outputCollector.clear();
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
    public TuplesImpl getOutput ()
    {
        return outputCollector.getOutputTuples();
    }

    @Override
    public void setUpstreamConnectionStatuses ( final boolean[] upstreamConnectionStatuses )
    {
        this.upstreamConnectionStatuses = copyOf( upstreamConnectionStatuses, upstreamConnectionStatuses.length );
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
