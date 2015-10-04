package cs.bilkent.zanza.operator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

public abstract class AbstractFieldsTest
{
	private Fields fields;

	@Before
	public final void createFieldsInstance()
	{
		fields = newFieldsInstance();
	}

	protected abstract Fields newFieldsInstance();

	@SuppressWarnings("unchecked")
	protected <T extends Fields> T getFields()
	{
		return (T) fields;
	}

	@Test
	public void shouldSet()
	{
		fields.set("field", "value");
		assertThat(fields.getObject("field"), equalTo("value"));
	}

	@Test
	public void shouldPut()
	{
		fields.set("field", "value");
		assertThat(fields.put("field", "value2"), equalTo("value"));
	}

	@Test
	public void shouldRemove()
	{
		fields.set("field", "value");
		assertNotNull(fields.remove("field"));
		assertNull(fields.getObject("field"));
	}

	@Test
	public void shouldNotRemoveNonExistingField()
	{
		assertNull(fields.remove("field"));
	}

	@Test
	public void shouldDelete()
	{
		fields.set("field", "value");
		assertTrue(fields.delete("field"));
		assertNull(fields.getObject("field"));
	}

	@Test
	public void shouldNotDeleteNonExistingField()
	{
		assertFalse(fields.delete("field"));
	}

	@Test
	public void shouldGetExisting()
	{
		fields.set("field", "value");
		assertThat(fields.<String> get("field"), equalTo("value"));
	}

	@Test
	public void shouldGetExistingOrDefault()
	{
		fields.set("field", "value");
		assertThat(fields.getOrDefault("field", "value2"), equalTo("value"));
	}

	@Test
	public void shouldGetNonExistingOrDefault()
	{
		assertThat(fields.getOrDefault("field", "value"), equalTo("value"));
	}

	@Test
	public void shouldContainExistingField()
	{
		fields.put("field", "value");
		assertTrue(fields.contains("field"));
	}

	@Test
	public void shouldNotContainNonExistingField()
	{
		assertFalse(fields.contains("field"));
	}

	@Test
	public void shouldGetExistingObject()
	{
		fields.put("field", "value");
		assertThat(fields.getObject("field"), equalTo("value"));
	}

	@Test
	public void shouldNotGetNonExistingObject()
	{
		assertNull(fields.getObject("field"));
	}

	@Test
	public void shouldGetExistingObjectOrDefault()
	{
		fields.put("field", "value");
		assertThat(fields.getObjectOrDefault("field", "value2"), equalTo("value"));
	}

	@Test
	public void shouldGetNonExistingObjectOrDefault()
	{
		assertThat(fields.getObjectOrDefault("field", "value2"), equalTo("value2"));
	}

	@Test
	public void shouldGetExistingString()
	{
		fields.put("field", "value");
		assertThat(fields.getString("field"), equalTo("value"));
	}

	@Test
	public void shouldNotGetExistingString()
	{
		assertNull(fields.getString("field"));
	}

	@Test
	public void shouldGetExistingStringOrDefault()
	{
		fields.put("field", "value");
		assertThat(fields.getStringOrDefault("field", "value2"), equalTo("value"));
	}

	@Test
	public void shouldGetNonExistingStringOrDefault()
	{
		assertThat(fields.getStringOrDefault("field", "value2"), equalTo("value2"));
	}

	@Test
	public void shouldGetExistingInteger()
	{
		fields.put("field", 1);
		assertThat(fields.getInteger("field"), equalTo(1));
	}

	@Test
	public void shouldNotGetNotExistingInteger()
	{
		assertNull(fields.getInteger("field"));
	}

	@Test
	public void shouldGetExistingIntegerOrDefault()
	{
		fields.put("field", 1);
		assertThat(fields.getIntegerOrDefault("field", 2), equalTo(1));
	}

	@Test
	public void shouldGetNonExistingIntegerOrDefault()
	{
		assertThat(fields.getIntegerOrDefault("field", 2), equalTo(2));
	}

	@Test
	public void shouldGetExistingLong()
	{
		fields.put("field", 1L);
		assertThat(fields.getLong("field"), equalTo(1L));
	}

	@Test
	public void shouldNotGetExistingLong()
	{
		assertNull(fields.getLong("field"));
	}

	@Test
	public void shouldGetExistingLongOrDefault()
	{
		fields.put("field", 1L);
		assertThat(fields.getLongOrDefault("field", 2L), equalTo(1L));
	}

	@Test
	public void shouldGetNonExistingLongOrDefault()
	{
		assertThat(fields.getLongOrDefault("field", 2L), equalTo(2L));
	}

	@Test
	public void shouldGetExistingBoolean()
	{
		fields.put("field", Boolean.TRUE);
		assertTrue(fields.getBoolean("field"));
	}

	@Test
	public void shouldNotGetNonExistingBoolean()
	{
		assertNull(fields.getBoolean("field"));
	}

	@Test
	public void shouldGetExistingBooleanOrDefault()
	{
		fields.put("field", Boolean.TRUE);
		assertTrue(fields.getBooleanOrDefault("field", Boolean.FALSE));
	}

	@Test
	public void shouldGetNonExistingBooleanOrDefault()
	{
		assertTrue(fields.getBooleanOrDefault("field", Boolean.TRUE));
	}

	@Test
	public void shouldGetGetExistingShort()
	{
		final short val = 1;
		fields.put("field", val);
		assertThat(fields.getShort("field"), equalTo(val));
	}

	@Test
	public void shouldNotGetNonExistingShort()
	{
		assertNull(fields.getShort("field"));
	}

	@Test
	public void shouldGetExistingShortOrDefault()
	{
		final short val = 1;
		final short other = 2;
		fields.put("field", val);
		assertThat(fields.getShortOrDefault("field", other), equalTo(val));
	}

	@Test
	public void shouldGetNonExistingShortOrDefault()
	{
		final short val = 1;
		assertThat(fields.getShortOrDefault("field", val), equalTo(val));
	}

	@Test
	public void shouldGetExistingByte()
	{
		final byte val = 1;
		fields.put("field", val);
		assertThat(fields.getByte("field"), equalTo(val));
	}

	@Test
	public void shouldNotGetNonExistingByte()
	{
		assertNull(fields.getByte("field"));
	}

	@Test
	public void shouldGetExistingByteOrDefault()
	{
		final byte val = 1;
		final byte other = 1;
		fields.put("field", val);
		assertThat(fields.getByteOrDefault("field", other), equalTo(val));
	}

	@Test
	public void shouldGetNonExistingByteOrDefault()
	{
		final byte val = 1;
		assertThat(fields.getByteOrDefault("field", val), equalTo(val));
	}

	@Test
	public void shouldGetExistingDouble()
	{
		fields.put("field", 1d);
		assertThat(fields.getDouble("field"), equalTo(1d));
	}

	@Test
	public void shouldNotGetNonExistingDouble()
	{
		assertNull(fields.getDouble("field"));
	}

	@Test
	public void shouldGetExistingDoubleOrDefault()
	{
		fields.put("field", 1d);
		assertThat(fields.getDoubleOrDefault("field", 2d), equalTo(1d));
	}

	@Test
	public void shouldGetNonExistingDoubleOrDefault()
	{
		assertThat(fields.getDoubleOrDefault("field", 1d), equalTo(1d));
	}

	@Test
	public void shouldGetExistingFloat()
	{
		fields.put("field", 1f);
		assertThat(fields.getFloat("field"), equalTo(1f));
	}

	@Test
	public void shouldNotGetNonExistingFloat()
	{
		assertNull(fields.getFloat("field"));
	}

	@Test
	public void shouldGetExistingFloatOrDefault()
	{
		fields.put("field", 1f);
		assertThat(fields.getFloatOrDefault("field", 2f), equalTo(1f));
	}

	@Test
	public void shouldGetNonExistingFloatOrDefault()
	{
		assertThat(fields.getFloatOrDefault("field", 1f), equalTo(1f));
	}

	@Test
	public void shouldGetExistingBinary()
	{
		final byte[] val = new byte[] { 1 };
		fields.put("field", val);
		assertThat(fields.getBinary("field"), equalTo(val));
	}

	@Test
	public void shouldGetExistingBinaryOrDefault()
	{
		final byte[] val = new byte[] { 1 };
		fields.put("field", val);
		assertThat(fields.getBinaryOrDefault("field", new byte[] {}), equalTo(val));
	}

	@Test
	public void shouldGetNonExistingBinaryOrDefault()
	{
		final byte[] val = new byte[] { 1 };
		assertThat(fields.getBinaryOrDefault("field", val), equalTo(val));
	}

	@SuppressWarnings("serial")
	private static List<Integer> makeArrayList()
	{
		return new ArrayList<Integer>()
		{
			{
				add(1);
			}
		};
	}

	@SuppressWarnings("serial")
	private static Set<Integer> makeHashSet()
	{
		return new HashSet<Integer>()
		{
			{
				add(1);
			}
		};
	}

	@SuppressWarnings("serial")
	private static Map<Integer, Integer> makeHashMap()
	{
		return new HashMap<Integer, Integer>()
		{
			{
				put(1, 1);
			}
		};
	}

	@Test
	public void shouldGetExistingCollection()
	{
		final Collection<Integer> value = makeArrayList();
		fields.put("field", value);
		assertThat(fields.getCollection("field"), equalTo(value));
	}

	@Test
	public void shouldNotGetNonExistingCollection()
	{
		assertNull(fields.getCollection("field"));
	}

	@Test
	public void shouldGetExistingCollectionOrDefault()
	{
		final Collection<Integer> value = makeArrayList();
		fields.put("field", value);
		assertThat(fields.getCollectionOrDefault("field", Collections.emptyList()), equalTo(value));
	}

	@Test
	public void shouldGetNonExistingCollectionOrDefault()
	{
		final Collection<Integer> value = makeArrayList();
		assertThat(fields.getCollectionOrDefault("field", value), equalTo(value));
	}

	@Test
	public void shouldGetExistingCollectionOrEmpty()
	{
		final Collection<Integer> value = makeArrayList();
		fields.put("field", value);
		assertThat(fields.getCollectionOrEmpty("field"), equalTo(value));
	}

	@Test
	public void shouldGetNonExistingCollectionOrEmpty()
	{
		assertThat(fields.getCollectionOrEmpty("field"), empty());
	}

	@Test
	public void shouldGetExistingList()
	{
		final List<Integer> value = makeArrayList();
		fields.put("field", value);
		assertThat(fields.getList("field"), equalTo(value));
	}

	@Test
	public void shouldNotGetNonExistingList()
	{
		assertNull(fields.getList("field"));
	}

	@Test
	public void shouldGetExistingListOrDefault()
	{
		final List<Integer> value = makeArrayList();
		fields.put("field", value);
		assertThat(fields.getListOrDefault("field", Collections.emptyList()), equalTo(value));
	}

	@Test
	public void shouldGetNonExistingListOrDefault()
	{
		final List<Integer> value = makeArrayList();
		assertThat(fields.getListOrDefault("field", value), equalTo(value));
	}

	@Test
	public void shouldGetExistingListOrEmpty()
	{
		final List<Integer> value = makeArrayList();
		fields.put("field", value);
		assertThat(fields.getListOrEmpty("field"), equalTo(value));
	}

	@Test
	public void shouldGetNonExistingListOrEmpty()
	{
		assertThat(fields.getListOrEmpty("field"), empty());
	}

	@Test
	public void shouldGetExistingSet()
	{
		final Set<Integer> value = makeHashSet();
		fields.put("field", value);
		assertThat(fields.getSet("field"), equalTo(value));
	}

	@Test
	public void shouldNotGetNonExistingSet()
	{
		assertNull(fields.getSet("field"));
	}

	@Test
	public void shouldGetExistingSetOrDefault()
	{
		final Set<Integer> value = makeHashSet();
		fields.put("field", value);
		assertThat(fields.getSetOrDefault("field", Collections.emptySet()), equalTo(value));
	}

	@Test
	public void shouldGetNonExistingSetOrDefault()
	{
		final Set<Integer> value = makeHashSet();
		assertThat(fields.getSetOrDefault("field", value), equalTo(value));
	}

	@Test
	public void shouldGetExistingSetOrEmpty()
	{
		final Set<Integer> value = makeHashSet();
		fields.put("field", value);
		assertThat(fields.getSetOrEmpty("field"), equalTo(value));
	}

	@Test
	public void shouldGetNonExistingSetOrEmpty()
	{
		assertThat(fields.getSetOrEmpty("field"), empty());
	}

	@Test
	public void shouldGetExistingMap()
	{
		final Map<Integer, Integer> value = makeHashMap();
		fields.put("field", value);
		assertThat(fields.getMap("field"), equalTo(value));
	}

	@Test
	public void shouldNotGetNonExistingMap()
	{
		assertNull(fields.getMap("field"));
	}

	@Test
	public void shouldGetExistingMapOrDefault()
	{
		final Map<Integer, Integer> value = makeHashMap();
		fields.put("field", value);
		assertThat(fields.getMapOrDefault("field", Collections.emptyMap()), equalTo(value));
	}

	@Test
	public void shouldGetNonExistingMapOrDefault()
	{
		final Map<Integer, Integer> value = makeHashMap();
		assertThat(fields.getMapOrDefault("field", value), equalTo(value));
	}

	@Test
	public void shouldGetExistingMapOrEmpty()
	{
		final Map<Integer, Integer> value = makeHashMap();
		fields.put("field", value);
		assertThat(fields.getMapOrEmpty("field"), equalTo(value));
	}

	@Test
	public void shouldGetNonExistingMapOrEmpty()
	{
		assertThat(fields.getMapOrEmpty("field").size(), equalTo(0));
	}
}
