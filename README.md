# java Exam (Cassandra)
The app downloads a web page content,
divides the page content to slices and stores each slice to Cassandra table.

## Prerequisites
To run the app you need java 8 or higher installed,
a Cassandra cluster up and running and a an Internet connection.

## Arguments
* args[0] - page url
* args[1] - Cassandra ip
* args[2] - Cassandra port

## Cassandra scheme
```
Keyspace: "razKeySpace"
table: "slices" (
            url text
            slice int
            content text
            PRIMARY KEY(url, slice)
        );
```