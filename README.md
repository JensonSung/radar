  
# Radar

Radar is the implementation of raw database construction in this paper.

# Getting Started

Requirements:
* Java 11 or above
* [Maven](https://maven.apache.org/) (`sudo apt install maven` on Ubuntu)
* The DBMS that you want to test (currently support MySQL, MariaDB, SQLite, CockroachDB, and TiDB)
* SQLite is an embedded database, which does not need extra setup and does not require connection parameters
* Other databases like MySQL, which requires connection parameters and needs to create a database named test
```
cd radar
mvn package -DskipTests
cd target
java -jar sqlancer-2.0.0.jar sqlite3
java -jar sqlancer-2.0.0.jar --host 127.0.0.1 --port 4000 --username root --password root tidb
java -jar sqlancer-2.0.0.jar --host 127.0.0.1 --port 26257 --username cockroach --password cockroach cockroachdb
java -jar sqlancer-2.0.0.jar --host 127.0.0.1 --port 3306 --username root --password root mysql
```

# Bug List
| ID | DBMS | Version | Issue | Status |
| -- | ---- | ------- | ----- | ------ |
| 1 | CockroachDB | 22.2.5 | [CockroachDB#99007](https://github.com/cockroachdb/cockroach/issues/99007) | Fixed |
| 2 | SQLite | 3.41.0 | [SQLite#2e427099d5](https://sqlite.org/forum/forumpost/2e427099d5) | Fixed |
| 3 | SQLite | 3.41.0 | [SQLite#d47a0e8e3a](https://sqlite.org/forum/forumpost/d47a0e8e3a) | Fixed |
| 4 | TiDB | 6.6.0 | [TiDB#41729](https://github.com/pingcap/tidb/issues/41729) | Fixed |
| 5 | TiDB | 6.6.0 | [TiDB#41730](https://github.com/pingcap/tidb/issues/41730) | Fixed |
| 6 | TiDB | 6.6.0 | [TiDB#41733](https://github.com/pingcap/tidb/issues/41733) | Fixed |
| 7 | TiDB | 6.6.0 | [TiDB#41734](https://github.com/pingcap/tidb/issues/41734) | Fixed |
| 8 | TiDB | 6.6.0 | [TiDB#41736](https://github.com/pingcap/tidb/issues/41736) | Fixed |
| 9 | TiDB | 6.6.0 | [TiDB#41753](https://github.com/pingcap/tidb/issues/41753) | Fixed |
| 10 | TiDB | 7.0.0 | [TiDB#44196](https://github.com/pingcap/tidb/issues/44196) | Fixed |
| 11 | TiDB | 7.0.0 | [TiDB#44127](https://github.com/pingcap/tidb/issues/44127) | Fixed |
| 12 | TiDB | 7.0.0 | [TiDB#44274](https://github.com/pingcap/tidb/issues/44274) | Fixed |
| 13 | TiDB | 7.1.0 | [TiDB#44359](https://github.com/pingcap/tidb/issues/44359) | Fixed |
| 14 | TiDB | 7.2.0 | [TiDB#45253](https://github.com/pingcap/tidb/issues/45253) | Fixed |
| 15 | TiDB | 7.2.0 | [TiDB#45378](https://github.com/pingcap/tidb/issues/45378) | Fixed |
| 16 | TiDB | 7.1.0 | [TiDB#45410](https://github.com/pingcap/tidb/issues/45410) | Fixed |
| 17 | MariaDB | 11.0.3 | [MariaDB#31951](https://jira.mariadb.org/browse/MDEV-31951) | Confirmed |
| 18 | MySQL | 8.0.32 | [MySQL#110125](https://bugs.mysql.com/bug.php?id=110125) | Confirmed |
| 19 | MySQL | 8.0.32 | [MySQL#110209](https://bugs.mysql.com/bug.php?id=110209) | Confirmed |
| 20 | MySQL | 8.0.32 | [MySQL#111272](https://bugs.mysql.com/bug.php?id=111272) | Confirmed |
| 21 | TiDB | 6.6.0 | [TiDB#41719](https://github.com/pingcap/tidb/issues/41719) | Confirmed |
| 22 | TiDB | 6.6.0 | [TiDB#41728](https://github.com/pingcap/tidb/issues/41728) | Confirmed |
| 23 | TiDB | 6.6.0 | [TiDB#41732](https://github.com/pingcap/tidb/issues/41732) | Confirmed |
| 24 | TiDB | 6.6.0 | [TiDB#41735](https://github.com/pingcap/tidb/issues/41735) | Confirmed |
| 25 | TiDB | 6.6.0 | [TiDB#41768](https://github.com/pingcap/tidb/issues/41768) | Confirmed |
| 26 | TiDB | 6.6.0 | [TiDB#41877](https://github.com/pingcap/tidb/issues/41877) | Confirmed |
| 27 | TiDB | 6.6.0 | [TiDB#41878](https://github.com/pingcap/tidb/issues/41878) | Confirmed |
| 28 | TiDB | 6.6.0 | [TiDB#41911](https://github.com/pingcap/tidb/issues/41911) | Confirmed |
| 29 | TiDB | 6.6.0 | [TiDB#41937](https://github.com/pingcap/tidb/issues/41937) | Confirmed |
| 30 | TiDB | 7.0.0 | [TiDB#44258](https://github.com/pingcap/tidb/issues/44258) | Confirmed |
| 31 | TiDB | 7.0.0 | [TiDB#44135](https://github.com/pingcap/tidb/issues/44135) | Confirmed |
| 32 | TiDB | 7.0.0 | [TiDB#44213](https://github.com/pingcap/tidb/issues/44213) | Confirmed |
| 33 | TiDB | 7.0.0 | [TiDB#44218](https://github.com/pingcap/tidb/issues/44218) | Confirmed |
| 34 | TiDB | 7.0.0 | [TiDB#44219](https://github.com/pingcap/tidb/issues/44219) | Confirmed |
| 35 | TiDB | 7.0.0 | [TiDB#44241](https://github.com/pingcap/tidb/issues/44241) | Confirmed |
| 36 | TiDB | 7.0.0 | [TiDB#44268](https://github.com/pingcap/tidb/issues/44268) | Confirmed |
| 37 | TiDB | 7.0.0 | [TiDB#44270](https://github.com/pingcap/tidb/issues/44270) | Confirmed |
| 38 | TiDB | 7.2.0 | [TiDB#45550](https://github.com/pingcap/tidb/issues/45550) | Confirmed |
| 39 | MySQL | 7.2.0 | [MySQL#110256](https://bugs.mysql.com/bug.php?id=110256) | Duplicate |
| 40 | CockroachDB | 22.2.5 | [CockroachDB#97672](https://github.com/cockroachdb/cockroach/issues/97672) | False positive |
| 41 | SQLite | 3.41.0 | [SQLite#60f85edfaf](https://sqlite.org/forum/forumpost/60f85edfaf) | False positive |
| 42 | SQLite | 3.41.0 | [SQLite#a2bde2b8f9](https://sqlite.org/forum/forumpost/a2bde2b8f9) | False positive |
