# TSID repository notes

For installation and the full user documentation, see the
[Query Farm TSID documentation](https://query.farm/products/extensions/tsid/).

## Generation and ordering

`tsid()` / `tsid_uuid()` embed the current time; `tsid(ts)` / `tsid_uuid(ts)`
embed a given timestamp (1970-01-01 .. 2106-02-07). There is no seed overload:
the random tail comes from a per-thread engine seeded from `std::random_device`.
Use `ORDER BY` when the query output must be sorted:

```sql
SELECT tsid() AS id FROM range(100) ORDER BY id;
```

The timestamp prefix supports time-based sorting, but IDs do not establish a
strict insertion or commit order across concurrent producers or clock changes.
Store an explicit sequence or application timestamp when that order is required.
