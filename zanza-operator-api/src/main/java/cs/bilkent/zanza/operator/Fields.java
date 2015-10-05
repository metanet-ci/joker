package cs.bilkent.zanza.operator;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface Fields
{
    <T> T get ( String key );

    <T> T getOrDefault ( String key, T defaultVal );

    boolean contains ( String key );

    void set ( String key, Object value );

    <T> T put ( String key, T value );

    Object remove ( String key );

    boolean delete ( String key );

    void clear();

    int size();

    /**
     * Returns immutable collection of key names
     *
     * @return immutable collection of key names
     */
    Collection<String> keys ();

    default Object getObject ( final String key )
    {
        return get( key );
    }

    default Object getObjectOrDefault ( final String key, final Object defaultVal )
    {
        return getOrDefault( key, defaultVal );
    }

    default String getString ( final String key )
    {
        return get( key );
    }

    default String getStringOrDefault ( final String key, final String defaultVal )
    {
        return getOrDefault( key, defaultVal );
    }

    default Integer getInteger ( final String key )
    {
        return get( key );
    }

    default Integer getIntegerOrDefault ( final String key, final Integer defaultVal )
    {
        return getOrDefault( key, defaultVal );
    }

    default Long getLong ( final String key )
    {
        return get( key );
    }

    default Long getLongOrDefault ( final String key, final Long defaultVal )
    {
        return getOrDefault( key, defaultVal );
    }

    default Boolean getBoolean ( final String key )
    {
        return get( key );
    }

    default Boolean getBooleanOrDefault ( final String key, final Boolean defaultVal )
    {
        return getOrDefault( key, defaultVal );
    }

    default Short getShort ( final String key )
    {
        return get( key );
    }

    default Short getShortOrDefault ( final String key, final Short defaultVal )
    {
        return getOrDefault( key, defaultVal );
    }

    default Byte getByte ( final String key )
    {
        return get( key );
    }

    default Byte getByteOrDefault ( final String key, final Byte defaultVal )
    {
        return getOrDefault( key, defaultVal );
    }

    default Double getDouble ( final String key )
    {
        return get( key );
    }

    default Double getDoubleOrDefault ( final String key, final Double defaultVal )
    {
        return getOrDefault( key, defaultVal );
    }

    default Float getFloat ( final String key )
    {
        return get( key );
    }

    default Float getFloatOrDefault ( final String key, final Float defaultVal )
    {
        return getOrDefault( key, defaultVal );
    }

    default byte[] getBinary ( final String key )
    {
        return get( key );
    }

    default byte[] getBinaryOrDefault ( final String key, final byte[] defaultVal )
    {
        return getOrDefault( key, defaultVal );
    }

    default <T> Collection<T> getCollection ( final String key )
    {
        return get( key );
    }

    default <T> Collection<T> getCollectionOrDefault ( final String key,
                                                      final Collection<T> defaultVal )
    {
        return getOrDefault( key, defaultVal );
    }

    default <T> Collection<T> getCollectionOrEmpty ( final String key )
    {
        return getOrDefault( key, Collections.emptyList() );
    }

    default <T> List<T> getList ( final String key )
    {
        return get( key );
    }

    default <T> List<T> getListOrDefault ( final String key, final List<T> defaultVal )
    {
        return getOrDefault( key, defaultVal );
    }

    default <T> List<T> getListOrEmpty ( final String key )
    {
        return getOrDefault( key, Collections.emptyList() );
    }

    default <T> Set<T> getSet ( final String key )
    {
        return get( key );
    }

    default <T> Set<T> getSetOrDefault ( final String key, final Set<T> defaultVal )
    {
        return getOrDefault( key, defaultVal );
    }

    default <T> Set<T> getSetOrEmpty ( final String key )
    {
        return getOrDefault( key, Collections.emptySet() );
    }

    default <K, V> Map<K, V> getMap ( final String key )
    {
        return get( key );
    }

    default <K, V> Map<K, V> getMapOrDefault ( final String key, final Map<K, V> defaultVal )
    {
        return getOrDefault( key, defaultVal );
    }

    default <K, V> Map<K, V> getMapOrEmpty ( final String key )
    {
        return getOrDefault( key, Collections.emptyMap() );
    }
}
