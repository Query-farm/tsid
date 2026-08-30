<p align="center">
  <a href="https://query.farm">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://query.farm/media-kit/logo/wordmark-dark.svg">
      <img alt="Query.Farm" src="https://query.farm/media-kit/logo/wordmark-light.svg" height="64">
    </picture>
  </a>
</p>

# DuckDB TSID Community Extension

[![DuckDB](https://img.shields.io/badge/DuckDB-community_extension-fdf1e0?logo=duckdb&logoColor=fff000)](https://duckdb.org/community_extensions/extensions/tsid.html)
[![v1.5 build](https://github.com/Query-farm/tsid/actions/workflows/MainDistributionPipeline.yml/badge.svg?branch=v1.5)](https://github.com/Query-farm/tsid/actions/workflows/MainDistributionPipeline.yml?query=branch%3Av1.5)

This extension adds support for Time-Sorted Unique Identifiers (TSID).

> Experimental: USE AT YOUR OWN RISK!

## Documentation

Full documentation, including installation, usage, the function reference, and cookbook examples, is available at:

**[https://query.farm/products/extensions/tsid](https://query.farm/products/extensions/tsid)**

## Installation

```sql
INSTALL tsid FROM community;
LOAD tsid;
```
