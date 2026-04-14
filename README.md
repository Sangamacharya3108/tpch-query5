# TPC-H Query 5 — Local Supplier Volume

## Implementation
- Language: C++14
- Threading: std::thread
- Build: CMake 3.10+
- Data: TPC-H Scale Factor 2 (~2 GB)

## Build
- cd build && make -j4

## Run Command
./tpch_query5 --r_name ASIA --start_date 1994-01-01 --end_date 1995-01-01 --threads 4 --table_path ~/tpch_data --result_path ~/tpch_results

## Results

| n_name    | revenue       |
|-----------|---------------|
| INDONESIA | 115979499.65  |
| CHINA     | 109568736.22  |
| INDIA     | 106258458.17  |
| JAPAN     | 104738341.03  |
| VIETNAM   | 98052109.13   |

## Runtime Benchmarks

| Threads | Query Time | Speedup |
|---------|------------|---------|
| 1       | 1.54 s     | 1x      |
| 4       | 1.33 s     | ~1.16x  |

## Notes
- Table loading is single threaded (~40s for SF2)
- Memory optimised using structs
- Parallelised query execution
