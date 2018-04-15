package cs.bilkent.joker.operator.utils;


public class Triple<T1, T2, T3>
{
    public final T1 _1;

    public final T2 _2;

    public final T3 _3;

    public static <T1, T2, T3> Triple<T1, T2, T3> of ( final T1 _1, final T2 _2, final T3 _3 )
    {
        return new Triple<>( _1, _2, _3 );
    }

    public Triple ( final T1 _1, final T2 _2, final T3 _3 )
    {
        this._1 = _1;
        this._2 = _2;
        this._3 = _3;
    }

    public int productArity ()
    {
        return 3;
    }

    public Object productElement ( int n ) throws IndexOutOfBoundsException
    {
        if ( n == 0 )
        {
            return this._1;
        }
        else if ( n == 1 )
        {
            return this._2;
        }
        else if ( n == 2 )
        {
            return this._3;
        }
        else
        {
            throw new IndexOutOfBoundsException();
        }
    }

    public T1 firstElement ()
    {
        return _1;
    }

    public T2 secondElement ()
    {
        return _2;
    }

    public T3 thirdElement ()
    {
        return _3;
    }

    @Override
    public boolean equals ( final Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        final Triple<?, ?, ?> triple = (Triple<?, ?, ?>) o;

        if ( _1 != null ? !_1.equals( triple._1 ) : triple._1 != null )
        {
            return false;
        }
        if ( _2 != null ? !_2.equals( triple._2 ) : triple._2 != null )
        {
            return false;
        }

        return !( _3 != null ? !_3.equals( triple._3 ) : triple._3 != null );
    }

    @Override
    public int hashCode ()
    {
        int result = _1 != null ? _1.hashCode() : 0;
        result = 31 * result + ( _2 != null ? _2.hashCode() : 0 );
        result = 31 * result + ( _3 != null ? _3.hashCode() : 0 );
        return result;
    }

    @Override
    public String toString ()
    {
        return "Triple{" + _1 + ", " + _2 + ", " + _3 + '}';
    }
}
