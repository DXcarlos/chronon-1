namespace py gen_thrift.fetcher
namespace java ai.chronon.fetcher

// Capture the information to identify a tile in the fetcher tiling based architecture.
// Based on the underlying KV store, we can use these fields to appropriately partition and sort the data
struct TileKey {
  1: optional string dataset
  2: optional list<byte> keyBytes
  3: optional i64 tileSizeMillis
  4: optional i64 tileStartTimestampMillis
}
