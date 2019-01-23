package cs.bilkent.joker.operator.schema.runtime;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkArgument;
import static cs.bilkent.joker.operator.schema.runtime.TupleSchema.FIELD_NOT_FOUND;
import gnu.trove.TIntCollection;
import gnu.trove.function.TIntFunction;
import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.procedure.TIntProcedure;
import gnu.trove.procedure.TObjectIntProcedure;
import gnu.trove.procedure.TObjectProcedure;

class TObjectIntMap1 implements TObjectIntMap<String>
{

    private final String key;

    TObjectIntMap1 ( final List<RuntimeSchemaField> fields )
    {
        checkArgument( fields.size() == 1 );
        this.key = fields.get( 0 ).name;
    }

    @Override
    public int getNoEntryValue ()
    {
        return FIELD_NOT_FOUND;
    }

    @Override
    public int size ()
    {
        return 1;
    }

    @Override
    public boolean isEmpty ()
    {
        return false;
    }

    @Override
    public boolean containsKey ( final Object key )
    {
        return this.key.equals( key );
    }

    @Override
    public boolean containsValue ( final int value )
    {
        return value == 0;
    }

    @Override
    public int get ( final Object key )
    {
        return this.key.equals( key ) ? 0 : FIELD_NOT_FOUND;
    }

    @Override
    public int put ( final String key, final int value )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int putIfAbsent ( final String key, final int value )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int remove ( final Object key )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll ( final Map<? extends String, ? extends Integer> m )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll ( final TObjectIntMap<? extends String> map )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear ()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> keySet ()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] keys ()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String[] keys ( final String[] array )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TIntCollection valueCollection ()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int[] values ()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int[] values ( final int[] array )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TObjectIntIterator<String> iterator ()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean increment ( final String key )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean adjustValue ( final String key, final int amount )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int adjustOrPutValue ( final String key, final int adjust_amount, final int put_amount )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean forEachKey ( final TObjectProcedure<? super String> procedure )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean forEachValue ( final TIntProcedure procedure )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean forEachEntry ( final TObjectIntProcedure<? super String> procedure )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void transformValues ( final TIntFunction function )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainEntries ( final TObjectIntProcedure<? super String> procedure )
    {
        throw new UnsupportedOperationException();
    }
}
