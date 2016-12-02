## OpenStreetMap Parquetizer

[![Build Status](https://travis-ci.org/adrianulbona/hmm.svg)](https://travis-ci.org/adrianulbona/osm-parquetizer)

The project intends to provide a way to get the [OpenStreetMap](https://www.openstreetmap.org) data available in a Big Data friendly format as [Parquet](https://parquet.apache.org/).

Currently any [PBF](http://wiki.openstreetmap.org/wiki/PBF_Format) file is converted into three parquet files, one for each type of entity from the original PBF (Nodes, Ways and Relations).

In order to get started: 

```shell
git clone https://github.com/adrianulbona/osm-parquetizer.git
cd osm-parquetizer
mvn clean package
java -jar target/osm-parquetizer-1.0.1-SNAPSHOT.jar path_to_your.pbf
```

For example, by running: 

```shell
java -jar target/osm-parquetizer-1.0.1-SNAPSHOT.jar romania-latest.osm.pbf
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
node
 |-- id: long (nullable = true)
 |-- version: integer (nullable = true)
 |-- timestamp: long (nullable = true)
 |-- changeset: long (nullable = true)
 |-- uid: integer (nullable = true)
 |-- user_sid: string (nullable = true)
 |-- tags: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- key: string (nullable = true)
 |    |    |-- value: string (nullable = true)
 |-- latitude: double (nullable = true)
 |-- longitude: double (nullable = true)

way
 |-- id: long (nullable = true)
 |-- version: integer (nullable = true)
 |-- timestamp: long (nullable = true)
 |-- changeset: long (nullable = true)
 |-- uid: integer (nullable = true)
 |-- user_sid: string (nullable = true)
 |-- tags: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- key: string (nullable = true)
 |    |    |-- value: string (nullable = true)
 |-- nodes: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- index: integer (nullable = true)
 |    |    |-- nodeId: long (nullable = true)

relation
 |-- id: long (nullable = true)
 |-- version: integer (nullable = true)
 |-- timestamp: long (nullable = true)
 |-- changeset: long (nullable = true)
 |-- uid: integer (nullable = true)
 |-- user_sid: string (nullable = true)
 |-- tags: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- key: string (nullable = true)
 |    |    |-- value: string (nullable = true)
 |-- members: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- id: long (nullable = true)
 |    |    |-- role: string (nullable = true)
 |    |    |-- type: string (nullable = true)
```
