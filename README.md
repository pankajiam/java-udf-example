# HIVE UDF Example
---

Hive User Defined Functions, also known as UDFs, allow you to create custom functions to process records or groups of records. This project has a simple UDF example to demonstrate reverse string of table and access hive configuration variable in UDFs. 

## Database setup

Create a database called udfdb

### Create database

```
CREATE DATABASE IF NOT EXISTS udfdb;
USE udfdb;
```

### Create function

Create a hive Sql function

```
CREATE FUNCTION my_func AS 'com.example.ReverseString' USING JAR '<jar-path>';
```

### Create table

Create a table called student to store data

```
CREATE TABLE IF NOT EXISTS udfdb.student
( 
  name string, 
  roll_no string
);
```

### Insert test data

Insert some test data in table

```
INSERT INTO udfdb.student
    (name,roll_no)
VALUES
    ('bob','UP123'),
    ('Emily','UP173'),
    ('Emma','UP129');
```

### Run query

Run the query 

```
beeline -u "<hive-db-config>" -f demo.sql
```
