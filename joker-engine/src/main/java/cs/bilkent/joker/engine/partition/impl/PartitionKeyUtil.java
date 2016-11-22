package cs.bilkent.joker.engine.partition.impl;

/*
 * hashcode calculation:
 *
 * int hashCode = 1;
 * for (E e : list)
 *     hashCode = 31*hashCode + (e==null ? 0 : e.hashCode());
 *
 *
 */
public final class PartitionKeyUtil
{
    private PartitionKeyUtil ()
    {

    }

    static void rangeCheck ( final int index, final int size )
    {
        if ( index >= size )
        {
            throw new IndexOutOfBoundsException( "Index: " + index + ", Size: " + size );
        }
    }

    public static int hashHead ( final Object val )
    {
        return 31 + val.hashCode();
    }

    public static int hashTail ( final int headHashCode, final Object val )
    {
        return 31 * headHashCode + val.hashCode();
    }

}
