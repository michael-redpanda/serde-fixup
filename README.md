# Serde Fixup

## Building

```shell
mvn clean package
```

## Running

```shell
java -jar target/serde-fixup-1.0-SNAPSHOT.jar \ 
  -s <schema registry URL> \
  -b <broker list> \
  -t <topic name>
```

Note that the subject looked up right now will be `<topic-name>-value`.
