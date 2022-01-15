# Memq

[logo]: https://phabricator.pinadmin.com/source/ambud_tools/browse/master/memq/memq.png

A simple light weight log queue and upload service

## How to build & run?

Server:

```
git clone https://phabricator.pinadmin.com/source/ambud_tools.git
cd ambud_tools
mvn clean package -DskipTests -pl memq -am
java -jar memq/target/memq-0.0.1.jar server
```

Client:

```
java -cp memq/target/memq-0.0.1.jar com.pinterest.memq.core.tools.LogClientTool
```