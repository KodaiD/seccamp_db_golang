# seccamp_db_golang
## DBMS with MVTO
Small key-value-store written in Go.
- multi version timestamp ordering

### Build and Run
```bash
seccamp_db_golang >> go build -o seccampdb
seccamp_db_golang >> ./seccampdb
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

// shutdown
seccampdb >> exit

// abort
seccampdb >> abort
```
