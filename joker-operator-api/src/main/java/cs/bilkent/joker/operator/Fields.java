package cs.bilkent.joker.operator;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;


/**
 * A utility interface that provides some methods for key-value accesses
 *
 * @param <K>
 *         type of the key
 */
public interface Fields<K>
{
    /**
     * Returns the value associated with the given key if it exists, {@code NULL} otherwise
     *
     * @param key
     *         the key to retrieve the associated value
     * @param <T>
     *         type of the associated value
     *
     * @return the value associated with the given key if it exists, {@code NULL} otherwise
     */
    <T> T get ( K key );

    /**
     * Checks if any value is value associated with the given key or not
     *
     * @param key
     *         the key to check the associated value
     * @param <T>
     *         type of the value
     *
     * @return if there exists a value associated with the given key, false otherwise
     */
    boolean contains ( K key );

    /**
     * Associates a value with the given key.
     * {@code NULL} is forbidden for both {@code key} and {@code value}.
     *
     * @param key
     *         the key to associate the value
     * @param value
     *         the value to associate with the given key.
     * @param <T>
     *         type of the given value
     */
    <T> void set ( K key, T value );

    /**
     * Deletes the value associated with the given key if it exists, and returns the value or {@code NULL} if it doesn't exist.
     * {@code NULL} is forbidden for {@code key}
     *
     * @param key
     *         the key to delete the associated value
     * @param <T>
     *         type of the associated value
     *
     * @return the deleted value associated with the given key
     */
    <T> T remove ( K key );

    /**
     * Deletes the value associated with the given key and returns true if any value exists for the given key before deletion.
     *
     * @param key
     *         the key to delete the associated value
     *
     * @return true if any value is associated with the given key before deletion
     */
    boolean delete ( K key );

    /**
     * Deletes all the keys and associated values.
     * Optional to implement. May throw {@code java.lang.UnsupportedOperationException}
     */
    void clear ();

    /**
     * Returns number of the keys
     *
     * @return number of the keys
     */
    int size ();

    /**
     * Returns the associated value of the given key, or throws {@link IllegalArgumentException} if the key is not present
     *
     * @param key
     *         key to get the associated value
     * @param <T>
     *         type of the associated value
     *
     * @return the associated value
     *
     * @throws IllegalArgumentException
     */
    default <T> T getOrFail ( K key )
    {
        T val = get( key );
        if ( val != null )
        {
            return val;
        }

        throw new IllegalArgumentException( key + " is not present!" );
    }

    /**
     * Returns the associated value of the given key, or throws a {@link RuntimeException} which is created by the provided function
     *
     * @param key
     *         key to get the associated value
     * @param exceptionFunc
     *         the function to create the exception to be thrown
     * @param <T>
     *         type of the associated value
     *
     * @return the associated value
     *
     * @throws RuntimeException
     *         craeted by the provided function
     */
    default <T> T getOrFail ( K key, Function<K, ? extends RuntimeException> exceptionFunc )
    {
        T val = get( key );
        if ( val != null )
        {
            return val;
        }

        throw exceptionFunc.apply( key );
    }

    /**
     * Returns the associated value of the given key, or returns the provided value if the key is not present
     *
     * @param key
     *         key to get the associated value
     * @param defaultVal
     *         value to return if the key is not present
     * @param <T>
     *         type of the associated value
     *
     * @return the value associated with the key or the provided value if the key is not present
     */
    default <T> T getOrDefault ( K key, T defaultVal )
    {
        T val = get( key );
        return val != null ? val : defaultVal;
    }

    /**
     * Returns the associated value of the given key, or returns the value that is created by the provided function, if the key is not
     * present
     *
     * @param key
     *         key to get the associated value
     * @param supplier
     *         the function to create the value to be returned if the key is not present
     * @param <T>
     *         type of the associated value
     *
     * @return the associated value of the given key, or returns the value that is created by the provided function, if the key is not
     * present
     */
    default <T> T getOrDefault ( K key, Supplier<T> supplier )
    {
        T val = get( key );
        return val != null ? val : supplier.get();
    }

    default Object getObject ( final K key )
    {
        return get( key );
    }

    default Object getObjectOrDefault ( final K key, final Object defaultVal )
    {
        return getOrDefault( key, defaultVal );
    }

    default String getString ( final K key )
    {
        return get( key );
    }

    default String getStringOrDefault ( final K key, final String defaultVal )
    {
        return getOrDefault( key, defaultVal );
    }

    default Integer getInteger ( final K key )
    {
        final Number number = get( key );
        if ( number != null )
        {
            return number instanceof Integer ? (Integer) number : number.intValue();
        }

        return null;
    }

    default Integer getIntegerOrDefault ( final K key, final Integer defaultVal )
    {
        final Number number = get( key );
        if ( number != null )
        {
            return number instanceof Integer ? (Integer) number : number.intValue();
        }

        return defaultVal;
    }

    default int getIntegerValueOrDefault ( final K key, final int defaultVal )
    {
        final Number number = get( key );
        return number != null ? number.intValue() : defaultVal;
    }

    default Long getLong ( final K key )
    {
        final Number number = get( key );
        if ( number != null )
        {
            return number instanceof Long ? (Long) number : number.longValue();
        }

        return null;
    }

    default Long getLongOrDefault ( final K key, final Long defaultVal )
    {
        final Number number = get( key );
        if ( number != null )
        {
            return number instanceof Long ? (Long) number : number.longValue();
        }

        return defaultVal;
    }

    default long getLongValueOrDefault ( final K key, final long defaultVal )
    {
        final Number number = get( key );
        return number != null ? number.longValue() : defaultVal;
    }

    default Boolean getBoolean ( final K key )
    {
        return get( key );
    }

    default Boolean getBooleanOrDefault ( final K key, final Boolean defaultVal )
    {
        return getOrDefault( key, defaultVal );
    }

    default Short getShort ( final K key )
    {
        final Number number = get( key );
        if ( number != null )
        {
            return number instanceof Short ? (Short) number : number.shortValue();
        }

        return null;
    }

    default Short getShortOrDefault ( final K key, final Short defaultVal )
    {
        final Number number = get( key );
        if ( number != null )
        {
            return number instanceof Short ? (Short) number : number.shortValue();
        }

        return defaultVal;
    }

    default short getShortValueOrDefault ( final K key, final short defaultVal )
    {
        final Number number = get( key );
        return number != null ? number.shortValue() : defaultVal;
    }

    default Byte getByte ( final K key )
    {
        final Number number = get( key );
        if ( number != null )
        {
            return number instanceof Byte ? (Byte) number : number.byteValue();
        }

        return null;
    }

    default Byte getByteOrDefault ( final K key, final Byte defaultVal )
    {
        final Number number = get( key );
        if ( number != null )
        {
            return number instanceof Byte ? (Byte) number : number.byteValue();
        }

        return defaultVal;
    }

    default byte getByteValueOrDefault ( final K key, final byte defaultVal )
    {
        final Number number = get( key );
        return number != null ? number.byteValue() : defaultVal;
    }

    default Double getDouble ( final K key )
    {
        final Number number = get( key );
        if ( number != null )
        {
            return number instanceof Double ? (Double) number : number.doubleValue();
        }

        return null;
    }

    default Double getDoubleOrDefault ( final K key, final Double defaultVal )
    {
        final Number number = get( key );
        if ( number != null )
        {
            return number instanceof Double ? (Double) number : number.doubleValue();
        }

        return defaultVal;
    }

    default double getDoubleValueOrDefault ( final K key, final double defaultVal )
    {
        final Number number = get( key );
        return number != null ? number.doubleValue() : defaultVal;
    }

    default Float getFloat ( final K key )
    {
        final Number number = get( key );
        if ( number != null )
        {
            return number instanceof Float ? (Float) number : number.floatValue();
        }

        return null;
    }

    default Float getFloatOrDefault ( final K key, final Float defaultVal )
    {
        final Number number = get( key );
        if ( number != null )
        {
            return number instanceof Float ? (Float) number : number.floatValue();
        }

        return defaultVal;
    }

    default float getFloatValueOrDefault ( final K key, final float defaultVal )
    {
        final Number number = get( key );
        return number != null ? number.floatValue() : defaultVal;
    }

    default byte[] getBinary ( final K key )
    {
        return get( key );
    }

    default byte[] getBinaryOrDefault ( final K key, final byte[] defaultVal )
    {
        return getOrDefault( key, defaultVal );
    }

    default <T> Collection<T> getCollection ( final K key )
    {
        return get( key );
    }

    default <T> Collection<T> getCollectionOrDefault ( final K key, final Collection<T> defaultVal )
    {
        return getOrDefault( key, defaultVal );
    }

    default <T> Collection<T> getCollectionOrEmpty ( final K key )
    {
        return getOrDefault( key, Collections.emptyList() );
    }

    default <T> List<T> getList ( final K key )
    {
        return get( key );
    }

    default <T> List<T> getListOrDefault ( final K key, final List<T> defaultVal )
    {
        return getOrDefault( key, defaultVal );
    }

    default <T> List<T> getListOrEmpty ( final K key )
    {
        final List<T> list = getList( key );
        return list != null ? list : new ArrayList<>();
    }

    default <T> Set<T> getSet ( final K key )
    {
        return get( key );
    }

    default <T> Set<T> getSetOrDefault ( final K key, final Set<T> defaultVal )
    {
        return getOrDefault( key, defaultVal );
    }

    default <T> Set<T> getSetOrEmpty ( final K key )
    {
        final Set<T> set = getSet( key );
        return set != null ? set : new HashSet<>();
    }

    default <K2, V> Map<K2, V> getMap ( final K key )
    {
        return get( key );
    }

    default <K2, V> Map<K2, V> getMapOrDefault ( final K key, final Map<K2, V> defaultVal )
    {
        return getOrDefault( key, defaultVal );
    }

    default <K2, V> Map<K2, V> getMapOrEmpty ( final K key )
    {
        final Map<K2, V> map = getMap( key );
        return map != null ? map : new HashMap<>();
    }

}
