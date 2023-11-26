
# Inicializamos los contenedores
docker-compose up

Kafka: 		      http://localhost:29092
Kafka Connect: 	http://localhost:8083
ElasticSearch: 	http://localhost:9200
Kibana:		      http://localhost:5601
ksqldb-server:  http://localhost:8088
zookeeper:      http://localhost:2181

# Eliminar los contenedores
docker-compose down

# Entramos al broker de kafka
docker exec -it broker bash

# Creamos los topics
kafka-topics --bootstrap-server localhost:29092 --topic tweets --create --partitions 3 --replication-factor 1
kafka-topics --bootstrap-server localhost:29092 --topic tweets-hashtags-lang --create --partitions 3 --replication-factor 1
kafka-topics --bootstrap-server localhost:29092 --topic tweets-trending-topics --create --partitions 3 --replication-factor 1

# Borramos los topics
kafka-topics --bootstrap-server localhost:29092 --delete --topic tweets
kafka-topics --bootstrap-server localhost:29092 --delete --topic tweets-hashtags-lang
kafka-topics --bootstrap-server localhost:29092 --delete --topic tweets-trending-topics

# Consumidor
kafka-console-consumer --bootstrap-server localhost:29092 --topic tweets --from-beginning
kafka-console-consumer --bootstrap-server localhost:29092 --topic tweets-hashtags-lang --from-beginning
kafka-console-consumer --bootstrap-server localhost:29092 --topic tweets-trending-topics --from-beginning

# Productor
kafka-console-producer --bootstrap-server localhost:29092 --topic tweets
kafka-console-producer --bootstrap-server localhost:29092 --topic tweets-hashtags-lang
kafka-console-producer --bootstrap-server localhost:29092 --topic tweets-trending-topics

# Ver topics con ksql
docker exec -it ksqldb-server bash
ksql
ksql> show topics;
ksql> print 'tweets';
