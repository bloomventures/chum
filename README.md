# Human DB


## Introduction

### Why? / Problem / Motivation

moment that data goes into a database it is locked away behind:
  - REPL + query langauge
  - programming language
  - GUI


what if databases were made mostly for humans (not computers)
  SQL came out of a time when:
     disk / memory / processing  was: limited / slow / expensive

  Now we have don't have those limits, but DB designers have moved on to making databases web-scale
    but on the UX side, we have: Microsoft Access, Excel, and CSVs


### Vision

human readable database
direct manipulation through text files
(GUI as needed)
"eventual validity"
   like "tests" for code
   can save invalid data
   script identifies invalid data
      format syntax errors
      schema errors


git + data!
  ... github + data!

what for:
  collaborative human-scale data

  relational / doc-store / graph


what not for:
  web-scale data
    generated data
    logs, analytics, user accounts


human-db is not web-scale




### What

  spec for human readable database serialization format

  database engine implementing spec
    - clojure
    - data source:
      - file system
      - file system + git repo
      - github api
    - io: protobuf

  language specific client libraries to connect to database engine


  REST server (?)

  import/export to other databases scripts


Some day...

github for data (human-db)
  hosting
  gui for editing
  public / private
  forking
  collaborators


## Details

### Folder Structure

```
/some-folder
  schema.yaml
  /data
    data-1.yaml
    /arbitrary-folders
      data-2.yaml
      data-3.yaml
```

### Using

```
./human-db ./path/to/folder
```

#### Querying

```
> (query '[:find ?name
           :where
           [?person :name ?name]])
```

#### Changing Data
```
> (transact! [{:id 123
               :name "Bob"}])

> (transact! [[:db/add [[?id :id 123]] :name "Bill"]])
```



## Other

### Contributors

- @rafd
- @10plusY
