// Package cmap provides an implementation of HashMap and ConcurrentMap, implemented using channels

package cmap

const (
	LoadFactorExpand = 0.75
	LoadFactorShrink = 1 - LoadFactorExpand
	InitialSize      = 16
	BigPrime1        = 100043
	BigPrime2        = 100151
	BigPrime3        = 1000000000039
)

const (
	MapRequestGet = iota
	MapRequestSet
	MapRequestDel
	MapRequestValues
)

type Key interface {
	Eq(Key) bool
	Hash() uint64
}

type IntKey int
type StringKey string

func (a IntKey) Eq(b Key) bool {
	return a == b
}

func (a IntKey) Hash() uint64 {
	var h uint64
	b := uint64(a)
	for ; b > 0; b /= 10 {
		h += ((b%10)*BigPrime1 + b*BigPrime2) % BigPrime3
	}
	return h
}

func (a StringKey) Eq(b Key) bool {
	return a == b
}

func (a StringKey) Hash() uint64 {
	var h uint64
	for _, c := range a {
		h += (uint64(c) + (h*BigPrime2)%BigPrime1) % BigPrime3
	}
	return h
}

type LinkedList struct {
	k    Key
	v    interface{}
	tail *LinkedList
}

func MakeLinkedList() *LinkedList {
	return &LinkedList{}
}

// getPrevElem returns the previous element which contains k in its tail.
// It returns (last element of the list, false) if k is not in the list
func (l *LinkedList) getPrevElem(k Key) (*LinkedList, bool) {
	var head *LinkedList
	for head = l; head.tail != nil; head = head.tail {
		if head.tail.k.Eq(k) {
			return head, true
		}
	}
	return head, false
}

func (l *LinkedList) Get(k Key) (interface{}, bool) {
	prev, ok := l.getPrevElem(k)
	if !ok {
		return nil, false
	}
	return prev.tail.v, true
}

func (l *LinkedList) Del(k Key) bool {
	prev, ok := l.getPrevElem(k)
	if !ok {
		return false
	}
	prev.tail = prev.tail.tail
	return true
}

func (l *LinkedList) Add(k Key, v interface{}) {
	var head *LinkedList
	for head = l; head.tail != nil; head = head.tail {
	}
	head.tail = &LinkedList{
		k: k,
		v: v,
	}
}

type KeyValue struct {
	k Key
	v interface{}
}

func (l *LinkedList) Values() []KeyValue {
	s := make([]KeyValue, 0)
	if l == nil {
		return s
	}
	for q := l.tail; q != nil; q = q.tail {
		s = append(s, KeyValue{k: q.k, v: q.v})
	}
	return s
}

type HashMap struct {
	buckets []*LinkedList
	size    int
}

func NewHashMap() *HashMap {
	return &HashMap{
		buckets: make([]*LinkedList, InitialSize, InitialSize),
		size:    0,
	}
}

func getBucketIndex(k Key, buckets []*LinkedList) uint64 {
	return k.Hash() % uint64(len(buckets))
}

func addValToBuckets(k Key, v interface{}, buckets []*LinkedList) {
	bucketIndex := getBucketIndex(k, buckets)
	l := buckets[bucketIndex]
	if l == nil {
		l = MakeLinkedList()
		buckets[bucketIndex] = l
	}
	prev, found := l.getPrevElem(k)
	if !found {
		prev.tail = &LinkedList{k: k, v: v}
	} else {
		prev.tail.v = v
	}
}

func (m *HashMap) rehash(newBuckets []*LinkedList) {
	for _, bucket := range m.buckets {
		if bucket == nil {
			continue
		}
		for q := bucket.tail; q != nil; q = q.tail {
			addValToBuckets(q.k, q.v, newBuckets)
		}
	}
}

func (m *HashMap) resize(newSize int) {
	newBuckets := make([]*LinkedList, newSize, newSize)
	m.rehash(newBuckets)
	m.buckets = newBuckets
}

func (m *HashMap) expand() {
	m.resize(len(m.buckets) * 2)
}

func (m *HashMap) shrink() {
	m.resize(len(m.buckets) / 2)
}

func (m *HashMap) load() float64 {
	return float64(m.size) / float64(len(m.buckets))
}

func (m *HashMap) Set(k Key, v interface{}) {
	if m.load() > LoadFactorExpand {
		m.expand()
	}
	m.size++
	addValToBuckets(k, v, m.buckets)
}

func (m *HashMap) Get(k Key) (interface{}, bool) {
	bucketIndex := getBucketIndex(k, m.buckets)

	if m.buckets[bucketIndex] == nil {
		return nil, false
	}

	return m.buckets[bucketIndex].Get(k)
}

func (m *HashMap) Del(k Key) bool {
	bucketIndex := getBucketIndex(k, m.buckets)

	if m.buckets[bucketIndex] == nil {
		return false
	}

	val := m.buckets[bucketIndex].Del(k)
	m.size--
	mLen := len(m.buckets)
	if m.load() < LoadFactorShrink && mLen > InitialSize {
		m.shrink()
	}
	return val
}

func (m *HashMap) Values() []KeyValue {
	s := make([]KeyValue, 0)
	for _, bucket := range m.buckets {
		s = append(s, bucket.Values()...)
	}
	return s
}

type mapResponse struct {
	v      interface{}
	ok     bool
	values []KeyValue
}

type mapRequest struct {
	mapRequestType int
	k              Key
	v              interface{}
	response       chan mapResponse
}

// Clients of ConcurrentMap should call Close
type ConcurrentMap struct {
	requests chan mapRequest
	m        *HashMap
}

func NewConcurrentMap() *ConcurrentMap {
	m := &ConcurrentMap{
		m:        NewHashMap(),
		requests: make(chan mapRequest),
	}
	go m.Serve()
	return m
}

func (m *ConcurrentMap) get(k Key) mapResponse {
	v, ok := m.m.Get(k)
	return mapResponse{
		v:  v,
		ok: ok,
	}
}

func (m *ConcurrentMap) set(k Key, v interface{}) mapResponse {
	m.m.Set(k, v)
	return mapResponse{
		ok: true,
	}
}

func (m *ConcurrentMap) del(k Key) mapResponse {
	ok := m.m.Del(k)
	return mapResponse{
		ok: ok,
	}
}

func (m *ConcurrentMap) values() mapResponse {
	values := m.m.Values()
	return mapResponse{
		values: values,
	}
}

func (m *ConcurrentMap) Get(k Key) (interface{}, bool) {
	responseChan := make(chan mapResponse)
	go func() {
		m.requests <- mapRequest{
			mapRequestType: MapRequestGet,
			response:       responseChan,
			k:              k,
		}
	}()
	response := <-responseChan
	return response.v, response.ok
}

func (m *ConcurrentMap) Set(k Key, v interface{}) {
	responseChan := make(chan mapResponse)
	go func() {
		m.requests <- mapRequest{
			mapRequestType: MapRequestSet,
			response:       responseChan,
			k:              k,
			v:              v,
		}
	}()
	<-responseChan
}

func (m *ConcurrentMap) Del(k Key) bool {
	responseChan := make(chan mapResponse)
	go func() {
		m.requests <- mapRequest{
			mapRequestType: MapRequestDel,
			response:       responseChan,
			k:              k,
		}
	}()
	response := <-responseChan
	return response.ok
}

func (m *ConcurrentMap) Values() []KeyValue {
	responseChan := make(chan mapResponse)
	go func() {
		m.requests <- mapRequest{
			mapRequestType: MapRequestValues,
			response:       responseChan,
		}
	}()
	response := <-responseChan
	return response.values
}

func (m *ConcurrentMap) Serve() {
	for req := range m.requests {
		switch req.mapRequestType {
		case MapRequestGet:
			req.response <- m.get(req.k)
		case MapRequestSet:
			req.response <- m.set(req.k, req.v)
		case MapRequestDel:
			req.response <- m.del(req.k)
		case MapRequestValues:
			req.response <- m.values()
		default:
			panic("Unrecognized map request")
		}
	}
}

func (m *ConcurrentMap) Close() {
	close(m.requests)
}
