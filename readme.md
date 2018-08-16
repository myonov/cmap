## `cmap` is a package which provides a `ConcurrentMap` implementation in Golang

### Example

```
m := NewConcurrentMap()
defer m.Close()

l := 1000

for i := 0; i < l; i++ {
    go func(v int) {
        m.Set(IntKey(v), v)
    }(i)
}
``` 