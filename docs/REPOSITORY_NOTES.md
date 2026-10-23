# TSID repository notes

For installation and the full user documentation, see the
[Query Farm TSID documentation](https://query.farm/products/extensions/tsid/).

## Generation and ordering

`tsid()` takes no arguments; there is no seed overload. Use `ORDER BY` when the
query output must be sorted:

```sql
SELECT tsid() AS id FROM range(100) ORDER BY id;
```

The timestamp prefix supports time-based sorting, but IDs do not establish a
strict insertion or commit order across concurrent producers or clock changes.
Store an explicit sequence or application timestamp when that order is required.
