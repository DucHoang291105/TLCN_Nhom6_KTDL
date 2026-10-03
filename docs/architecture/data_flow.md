# Data Flow

```text
6 historical sources ─┐
                      ├─> Bronze MinIO/Parquet ─> normalize + DQ + dedup ─> Iceberg listing_observation
3 crawl sources ──────┘                                                    │
                                                                           ├─> crawl_current_27
                                                                           ├─> historical_current_27
                                                                           └─> silver_listings_current_27 (9 nguồn, 27 cột)
                                                                                          │
                                                                                          ├─> listing_history
                                                                                          └─> listing_location
```

Historical: `chotot`, `mogi`, `alonhadat`, `luachonnhadat`, `muaban`, `homedy`.

Snapshot crawl: `batdongsan`, `guland`, `nhadatvui`.

Gold và dashboard chỉ đọc bảng final `lakehouse.silver.silver_listings_current_27` cùng các bảng phụ History/Location; không đọc hai bảng current trung gian.
