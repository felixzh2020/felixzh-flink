# Doris
```
create database mydb;
```
```
create table mydb.mytable (
 `id` bigint,
 `data` text null
) engine=olap
duplicate key(`id`)
distributed by hash(`id`) buckets 10
properties(
  "replication_allocation" = "tag.location.default: 2"
);
```

# Flink
```
sh DataGen2Doris.sh
```