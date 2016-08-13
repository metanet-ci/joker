package cs.bilkent.joker.examples.bargaindiscovery;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import cs.bilkent.joker.operator.Tuple;

class MergedTupleListsIterator implements Iterator<Tuple>
{

    private final Iterator<Tuple> leftIt;

    private final Iterator<Tuple> rightIt;

    private final Comparator<Tuple> comparator;

    private Tuple next;

    private Tuple left;

    private Tuple right;

    public MergedTupleListsIterator ( final List<Tuple> left, final List<Tuple> right, final Comparator<Tuple> comparator )
    {
        this.leftIt = left.iterator();
        this.rightIt = right.iterator();
        this.comparator = comparator;
    }

    @Override
    public boolean hasNext ()
    {
        if ( next != null )
        {
            return true;
        }

        if ( left == null && leftIt.hasNext() )
        {
            left = leftIt.next();
        }

        if ( right == null && rightIt.hasNext() )
        {
            right = rightIt.next();
        }

        if ( left != null && right != null )
        {
            if ( comparator.compare( left, right ) <= 0 )
            {
                next = left;
                left = null;
            }
            else
            {
                next = right;
                right = null;
            }
        }
        else if ( left != null )
        {
            next = left;
            left = null;
        }
        else if ( right != null )
        {
            next = right;
            right = null;
        }
        else
        {
            return false;
        }

        return true;
    }

    @Override
    public Tuple next ()
    {
        Tuple next = this.next;
        if ( next == null )
        {
            if ( hasNext() )
            {
                next = this.next;
            }
            else
            {
                throw new NoSuchElementException();
            }
        }

        this.next = null;
        return next;
    }
}
