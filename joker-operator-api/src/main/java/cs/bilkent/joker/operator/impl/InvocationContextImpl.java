package cs.bilkent.joker.operator.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkNotNull;
import cs.bilkent.joker.operator.InvocationContext;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.kvstore.KVStore;
import cs.bilkent.joker.partition.impl.PartitionKey;
import static java.util.Arrays.copyOf;


public class InvocationContextImpl implements InvocationContext
{

    private final int inputPortCount;

    private final Function<PartitionKey, KVStore> kvStoreSupplier;

    private final List<TuplesImpl> inputs = new ArrayList<>();

    private final List<PartitionKey> partitionKeys = new ArrayList<>();

    private final TuplesImpl output;

    private InvocationReason reason;

    private boolean[] upstreamConnectionStatuses;

    private int inputCount;

    private int currentInput = 0;

    public InvocationContextImpl ( final int inputPortCount,
                                   final Function<PartitionKey, KVStore> kvStoreSupplier,
                                   final TuplesImpl output )
    {
        this.inputPortCount = inputPortCount;
        this.kvStoreSupplier = kvStoreSupplier;
        this.output = output;
    }

    public void setInvocationReason ( final InvocationReason reason )
    {
        checkNotNull( reason );
        this.reason = reason;
    }

    public void reset ()
    {
        this.reason = null;
        for ( TuplesImpl input : inputs )
        {
            input.clear();
        }
        partitionKeys.clear();
        output.clear();
        inputCount = 0;
        currentInput = 0;
    }

    public void setUpstreamConnectionStatuses ( final boolean[] upstreamConnectionStatuses )
    {
        this.upstreamConnectionStatuses = copyOf( upstreamConnectionStatuses, upstreamConnectionStatuses.length );
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

    public int getInputCount ()
    {
        return inputCount;
    }

    public boolean nextInput ()
    {
        return ( ++currentInput < inputCount );
    }

    public TuplesImpl getInput ()
    {
        return inputs.get( currentInput );
    }

    public List<TuplesImpl> getInputs ()
    {
        return inputs;
    }

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
        output.add( tuple );
    }

    @Override
    public void output ( final List<Tuple> tuples )
    {
        output.addAll( tuples );
    }

    @Override
    public void output ( final int portIndex, final Tuple tuple )
    {
        output.add( portIndex, tuple );
    }

    @Override
    public void output ( final int portIndex, final List<Tuple> tuples )
    {
        output.addAll( portIndex, tuples );
    }

    public TuplesImpl getOutput ()
    {
        return output;
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
    public boolean isInputPortClosed ( final int portIndex )
    {
        return !upstreamConnectionStatuses[ portIndex ];
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

}
