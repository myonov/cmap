package cmap

import (
	"testing"
	"reflect"
	"fmt"
	"sort"
	"time"
)

func assertTrue(cond bool, t *testing.T, message string, args ...interface{}) {
	assertFalse(!cond, t, message, args...)
}

func assertFalse(cond bool, t *testing.T, message string, args ...interface{}) {
	if cond {
		t.Errorf(message, args...)
	}
}

func TestNewLinkedList(t *testing.T) {
	l := MakeLinkedList()

	l.Add(IntKey(1), "l1")
	l.Add(IntKey(2), "l2")
	l.Add(IntKey(3), "l3")
	l.Add(IntKey(4), "l4")
	l.Add(IntKey(5), "l5")
	l.Add(IntKey(6), "l6")
	l.Add(IntKey(7), "l7")

	var (
		s  interface{}
		ok bool
	)

	s, ok = l.Get(IntKey(1))
	assertTrue(ok && s.(string) == "l1", t, "Expected (\"l1\", true), received (%v, %v)", s, ok)

	s, ok = l.Get(IntKey(3))
	assertTrue(ok && s.(string) == "l3", t, "Expected (\"l3\", true), received (%v, %v)", s, ok)

	s, ok = l.Get(IntKey(15))
	assertFalse(ok, t, "Expected (nil, false), received (%v, %v)", s, ok)

	ok = l.Del(IntKey(1))
	assertTrue(ok, t, "Expected key 1 to be in the list")

	ok = l.Del(IntKey(1))
	assertFalse(ok, t, "Expected key 1 not to be in the list")

	s, ok = l.Get(IntKey(1))
	assertFalse(ok, t, "Expected (nil, false), received (%v, %v)", s, ok)

	l.Del(IntKey(5))
	l.Del(IntKey(7))
	l.Del(IntKey(3))

	expected := []KeyValue{
		{IntKey(2), "l2"},
		{IntKey(4), "l4"},
		{IntKey(6), "l6"},
	}

	values := l.Values()
	assertTrue(reflect.DeepEqual(expected, values), t, "Expected(%v) != Values(%v)\n", expected, values)
}

func MedianBucket(m *HashMap) (int, int, int, float64) {
	filled, l, maxBucket := 0, 0, 0
	for _, bucket := range m.buckets {
		if bucket != nil {
			filled++
			q := len(bucket.Values())
			if q > maxBucket {
				maxBucket = q
			}
			l += len(bucket.Values())
		}
	}
	return len(m.buckets), filled, maxBucket, float64(l) / float64(filled)
}

func TestHashMapBucketingInt(t *testing.T) {
	m := NewHashMap()
	var (
		numBuckets    int
		filled        int
		maxBucket     int
		averageBucket float64
	)

	l := 10000

	for i := 0; i < l; i++ {
		m.Set(IntKey(i), i)
	}

	t.Logf("IntKey")
	numBuckets, filled, maxBucket, averageBucket = MedianBucket(m)
	t.Logf("Number of buckets: %d; filled rows: %d; load: %f; maxBucket: %d; average items in bucket: %f",
		numBuckets, filled, m.load(), maxBucket, averageBucket)

	for i := l - 1; i >= 1000; i-- {
		m.Del(IntKey(i))
	}

	numBuckets, filled, maxBucket, averageBucket = MedianBucket(m)
	t.Logf("Number of buckets: %d; filled rows: %d; load: %f; maxBucket: %d; average items in bucket: %f",
		numBuckets, filled, m.load(), maxBucket, averageBucket)
}

func TestHashMapBucketingString(t *testing.T) {
	m := NewHashMap()
	var (
		numBuckets    int
		filled        int
		maxBucket     int
		averageBucket float64
	)

	l := 10000

	for i := 0; i < l; i++ {
		k := StringKey(fmt.Sprintf("%d", i))
		m.Set(k, i)
	}

	t.Logf("StringKey")
	numBuckets, filled, maxBucket, averageBucket = MedianBucket(m)
	t.Logf("Number of buckets: %d; filled rows: %d; load: %f; maxBucket: %d; average items in bucket: %f",
		numBuckets, filled, m.load(), maxBucket, averageBucket)

	for i := l - 1; i >= 1000; i-- {
		k := StringKey(fmt.Sprintf("%d", i))
		m.Del(k)
	}

	numBuckets, filled, maxBucket, averageBucket = MedianBucket(m)
	t.Logf("Number of buckets: %d; filled rows: %d; load: %f; maxBucket: %d; average items in bucket: %f",
		numBuckets, filled, m.load(), maxBucket, averageBucket)
}

func TestHashMapSetGetDel(t *testing.T) {
	var (
		v  interface{}
		ok bool
	)

	m := NewHashMap()

	l := 100
	for i := 0; i < l; i++ {
		m.Set(IntKey(i), i)
	}

	v, _ = m.Get(IntKey(1))
	assertTrue(v == 1, t, "Get 1")

	v, _ = m.Get(IntKey(38))
	assertTrue(v == 38, t, "Get 38")

	v, _ = m.Get(IntKey(99))
	assertTrue(v == 99, t, "Get 99")

	for i := 0; i < 10; i++ {
		m.Del(IntKey(i))
		_, ok := m.Get(IntKey(i))
		assertFalse(ok, t, "Item %d is deleted", i)
	}

	for i := 11; i < 100; i++ {
		_, ok := m.Get(IntKey(i))
		assertTrue(ok, t, "Expected item %d", i)
	}

	m.Set(IntKey(1), 100)
	v, ok = m.Get(IntKey(1))
	assertTrue(ok && v == 100, t, "Expected item to be 100; got %d", v)
	m.Set(IntKey(1), 99)
	v, ok = m.Get(IntKey(1))
	assertTrue(ok && v == 99, t, "Expected item to be 99; got %d", v)
}

func TestHashMapValues(t *testing.T) {
	m := NewHashMap()
	for i := 0; i < 100; i++ {
		m.Set(IntKey(i), i)
	}
	for i := 95; i < 100; i++ {
		m.Del(IntKey(i))
	}
	for i := 90; i >= 0; i-- {
		m.Set(IntKey(i), 100-i)
	}

	expected := make([]KeyValue, 95)
	for i := 0; i < 91; i++ {
		expected[i] = KeyValue{IntKey(i), 100 - i}
	}
	for i := 91; i < 95; i++ {
		expected[i] = KeyValue{IntKey(i), i}
	}

	values := m.Values()
	sort.Slice(values, func(i, j int) bool {
		v1 := values[i].k.(IntKey)
		v2 := values[j].k.(IntKey)
		return v1 < v2
	})
	sort.Slice(expected, func(i, j int) bool {
		v1 := expected[i].k.(IntKey)
		v2 := expected[j].k.(IntKey)
		return v1 < v2
	})
	assertTrue(reflect.DeepEqual(expected, values), t, "\nExpected(%d):\n%v\nGot(%d):\n%v\n",
		len(expected), expected, len(values), values)
}

func TestConcurrentMapBombard(t *testing.T) {
	m := NewConcurrentMap()
	defer m.Close()

	l := 1000
	expected := make([]KeyValue, l)

	for i := 0; i < l; i++ {
		go func(v int) {
			m.Set(IntKey(v), v)
		}(i)
		expected[i] = KeyValue{IntKey(i), i}
	}

	time.Sleep(1 * time.Second)
	values := m.Values()
	sort.Slice(values, func(i, j int) bool {
		v1 := values[i].k.(IntKey)
		v2 := values[j].k.(IntKey)
		return v1 < v2
	})
	assertTrue(reflect.DeepEqual(expected, values), t, "\nExpected(%d):\n%v\nGot(%d):\n%v\n",
		len(expected), expected, len(values), values)
}

func TestNewHashMap(t *testing.T) {
	m := NewHashMap()
	if m.size != 0 || len(m.buckets) != InitialSize {
		t.Errorf("Hash Map m is not initialized")
	}
}
