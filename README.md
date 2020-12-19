# seccamp_db_golang
## DBMS with MVTO
Small In-memory DBMS written in Go.

### Features
This DBMS provides the following:
- CC protocol: Multi-version timestamp ordering
- Crash Recovery
- Checkpointing

### Build and Run
Server
```
$ go build -o seccampdb
$ ./seccampdb
```
Client
```
$ telnet localhost 7777
seccampdb >> 
```

### Usage
```
// insert new record
seccampdb >> insert <key> <value>

// read value
seccampdb >> read <key>

// update record
seccampdb >> update <key> <new value>

// delete record
seccampdb >> delete <key>

// save current status
seccampdb >> commit

// abort
seccampdb >> abort
```
