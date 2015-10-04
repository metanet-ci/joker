package cs.bilkent.zanza.operator;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface Fields
{
	<T> T get(String field);

	<T> T getOrDefault(String field, T defaultVal);

	void set(String field, Object value);

	<T> T put(String field, T value);

	Object remove(String field);

	boolean delete(String field);

	void clear();

	int size();

	/**
	 * Returns immutable collection of field names
	 *
	 * @return immutable collection of field names
	 */
	Collection<String> fieldNames();

	default boolean contains(final String field)
	{
		return getObject(field) != null;
	}

	default Object getObject(final String field)
	{
		return get(field);
	}

	default Object getObjectOrDefault(final String field, final Object defaultVal)
	{
		return getOrDefault(field, defaultVal);
	}

	default String getString(final String field)
	{
		return get(field);
	}

	default String getStringOrDefault(final String field, final String defaultVal)
	{
		return getOrDefault(field, defaultVal);
	}

	default Integer getInteger(final String field)
	{
		return get(field);
	}

	default Integer getIntegerOrDefault(final String field, final Integer defaultVal)
	{
		return getOrDefault(field, defaultVal);
	}

	default Long getLong(final String field)
	{
		return get(field);
	}

	default Long getLongOrDefault(final String field, final Long defaultVal)
	{
		return getOrDefault(field, defaultVal);
	}

	default Boolean getBoolean(final String field)
	{
		return get(field);
	}

	default Boolean getBooleanOrDefault(final String field, final Boolean defaultVal)
	{
		return getOrDefault(field, defaultVal);
	}

	default Short getShort(final String field)
	{
		return get(field);
	}

	default Short getShortOrDefault(final String field, final Short defaultVal)
	{
		return getOrDefault(field, defaultVal);
	}

	default Byte getByte(final String field)
	{
		return get(field);
	}

	default Byte getByteOrDefault(final String field, final Byte defaultVal)
	{
		return getOrDefault(field, defaultVal);
	}

	default Double getDouble(final String field)
	{
		return get(field);
	}

	default Double getDoubleOrDefault(final String field, final Double defaultVal)
	{
		return getOrDefault(field, defaultVal);
	}

	default Float getFloat(final String field)
	{
		return get(field);
	}

	default Float getFloatOrDefault(final String field, final Float defaultVal)
	{
		return getOrDefault(field, defaultVal);
	}

	default byte[] getBinary(final String field)
	{
		return get(field);
	}

	default byte[] getBinaryOrDefault(final String field, final byte[] defaultVal)
	{
		return getOrDefault(field, defaultVal);
	}

	default <T> Collection<T> getCollection(final String field)
	{
		return get(field);
	}

	default <T> Collection<T> getCollectionOrDefault(final String field, final Collection<T> defaultVal)
	{
		return getOrDefault(field, defaultVal);
	}

	default <T> Collection<T> getCollectionOrEmpty(final String field)
	{
		return getOrDefault(field, Collections.emptyList());
	}

	default <T> List<T> getList(final String field)
	{
		return get(field);
	}

	default <T> List<T> getListOrDefault(final String field, final List<T> defaultVal)
	{
		return getOrDefault(field, defaultVal);
	}

	default <T> List<T> getListOrEmpty(final String field)
	{
		return getOrDefault(field, Collections.emptyList());
	}

	default <T> Set<T> getSet(final String field)
	{
		return get(field);
	}

	default <T> Set<T> getSetOrDefault(final String field, final Set<T> defaultVal)
	{
		return getOrDefault(field, defaultVal);
	}

	default <T> Set<T> getSetOrEmpty(final String field)
	{
		return getOrDefault(field, Collections.emptySet());
	}

	default <K, V> Map<K, V> getMap(final String field)
	{
		return get(field);
	}

	default <K, V> Map<K, V> getMapOrDefault(final String field, final Map<K, V> defaultVal)
	{
		return getOrDefault(field, defaultVal);
	}

	default <K, V> Map<K, V> getMapOrEmpty(final String field)
	{
		return getOrDefault(field, Collections.emptyMap());
	}
}
