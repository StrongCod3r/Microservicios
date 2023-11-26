

//POST
http://localhost:8083/connectors
{
  "name": "elasticsearch-sink",
  "config": {
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "tasks.max": "1",
    "topics": "topic-tweets",
    "key.ignore": "true",
    "schema.ignore": "true",
    "connection.url": "http://elastic:9200",
    "type.name": "_doc",
    "name": "elasticsearch-sink",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}

// PUT
localhost:8083/connectors/elasticsearch-sink/config
{
  "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
  "connection.url": "http://elasticsearch:9200",
  "tasks.max": "1",
  "topics": "topic-tweets",
  "name": "simple-elasticsearch-connector",
  "type.name": "_doc",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false",
  "schema.ignore": "true",
  "key.ignore": "true"
 }

# Ver indices
curl -XGET 'http://localhost:9200/_cat/indices?v'

# Crear índice
curl -XPUT 'http://localhost:9200/topic-tweets'


# Búsqueda
curl -XGET 'http://localhost:9200/topic-tweets/_search' -H 'Content-Type: application/json' -d '{
  "query": {
    "match": {
      "message": "omega"
    }
  }
}'

http://localhost:9200/topic-tweets/_search: Especifica la URL del índice topic-tweets y la ruta _search para realizar la búsqueda.
"query": Especifica el tipo de consulta que se va a realizar.
"match": Indica que se realizará una consulta de coincidencia.
"message": Es el campo en el que se buscará la palabra "omega".
"omega": Es la palabra clave que estás buscando.

{
  "query": {
    "query_string": {
      "query": "omega"
    }
  }
}
---
{
  "query": {
    "match": {
      "message": "omega"
    }
  }
}
--
{
  "query": {
    "match_all": {}
  }
}


curl -XGET 'http://localhost:9200/topic-tweets/_search' -H 'Content-Type: application/json' -d '{
  "query": {
    "match": {
      "message": "omega"
    }
  }
}


curl -X GET http://localhost:8083/connectors/elasticsearch-sink/status