# Proglog

Proglog implements a commit log that works very similar to Kafka's internal storage system, [see here](https://thehoard.blog/how-kafkas-storage-internals-work-3a29b02e026).

## Concepts

* Record - the data stored in the log.
* Store - the file where the records are stored.
* Index - the file which stores the index entries of the Records present in the
  Store.
* Segment - the abstraction that ties a Store and an Index together i.e; a
  segment is a combination of both a store and an index. The Index denotes where
  to find a particular Record in the associated Store.
* Log - the abstraction that ties all the segments together.
