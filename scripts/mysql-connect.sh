curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d '{
  "name": "inventory-connector",
    "config": {
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "database.hostname": "mysql",
        "database.port": "3306",
        "database.user": "debezium",
        "database.password": "debezium",
        "database.server.id": "123456",
        "topic.prefix": "prefix",
        "database.include.list": "default",
        "schema.history.internal.kafka.bootstrap.servers": "kafka1:9092,kafka2:9092,kafka3:9092",
        "schema.history.internal.kafka.topic": "schemahistory.fullfillment",
        "include.schema.changes": "true"
    }
}'
