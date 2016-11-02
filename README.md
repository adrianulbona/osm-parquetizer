## OpenStreetMap Parquetizer

[![Build Status](https://travis-ci.org/adrianulbona/hmm.svg)](https://travis-ci.org/adrianulbona/osm-parquetizer)

The project intends to provide a way to get the [OpenStreetMap](https://www.openstreetmap.org) data available in a Big Data friendly format as [Parquet](https://parquet.apache.org/).

Currently any [PBF](http://wiki.openstreetmap.org/wiki/PBF_Format) file is converted into three parquet files, one for each type of entity from the original PBF (Nodes, Ways and Relations).

In order to get started: 

```shell
git clone https://github.com/adrianulbona/osm-parquetizer.git
cd osm-parquetizer
mvn clean package
java -jar target/osm-parquetizer-1.0.0-SNAPSHOT.jar path_to_your.pbf
```

For example, by running: 

```shell
java -jar target/osm-parquetizer-1.0.0-SNAPSHOT.jar romania-latest.osm.pbf
```

In a few seconds (on a decent laptop) you should get the following files:
```shell
-rw-r--r--  1 adrianbona  adrianbona   145M Apr  3 19:57 romania-latest.osm.pbf
-rw-r--r--  1 adrianbona  adrianbona   372M Apr  3 19:58 romania-latest.osm.pbf.node.parquet
-rw-r--r--  1 adrianbona  adrianbona   1.1M Apr  3 19:58 romania-latest.osm.pbf.relation.parquet
-rw-r--r--  1 adrianbona  adrianbona   123M Apr  3 19:58 romania-latest.osm.pbf.way.parquet
```

The parquet files have the following schemas:

```probobuf
message node {
  required int64 id;
  optional int32 version;
  optional int64 timestamp;
  optional int64 changeset;
  optional int32 uid;
  optional binary user_sid;
  repeated group tags {
    required binary key;
    optional binary value;
  }
  required double latitude;
  required double longitude;
}

message way {
  required int64 id;
  optional int32 version;
  optional int64 timestamp;
  optional int64 changeset;
  optional int32 uid;
  optional binary user_sid;
  repeated group tags {
    required binary key;
    optional binary value;
  }
  repeated int64 nodes;
}

message relation {
  required int64 id;
  optional int32 version;
  optional int64 timestamp;
  optional int64 changeset;
  optional int32 uid;
  optional binary user_sid;
  repeated group tags {
    required binary key;
    optional binary value;
  }
  repeated int64 members;
}
```
