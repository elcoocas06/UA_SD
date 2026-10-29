# ==================================================================
# Broker Kafka de un solo nodo en modo KRaft (sin ZooKeeper)
#
# Dos "listeners" (puertas de entrada) para clientes:
#   INTERNAL (29092): CENTRAL, que está en la misma red que Kafka
#   EXTERNAL (9092):  Engines y FO de los PC del laboratorio (o de tu portátil)
# Kafka devuelve a cada cliente la dirección "anunciada" (ADVERTISED) del listener
# por el que ha entrado, y el cliente se conecta después a esa dirección. Por eso
# cada listener anuncia la dirección con la que se le alcanza desde fuera.
#
# Los valores de abajo sirven para docker-compose en local. En Railway se
# sobrescriben KAFKA_LISTENERS y KAFKA_ADVERTISED_LISTENERS con variables del servicio.
# ==================================================================

FROM apache/kafka:3.9.0

ENV KAFKA_NODE_ID=1 \
    KAFKA_PROCESS_ROLES=broker,controller \
    KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
    KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    KAFKA_LISTENERS=INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092,CONTROLLER://localhost:9093 \
    KAFKA_ADVERTISED_LISTENERS=INTERNAL://kafka:29092,EXTERNAL://localhost:9092 \
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT \
    KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
    KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
    KAFKA_NUM_PARTITIONS=1 \
    KAFKA_AUTO_CREATE_TOPICS_ENABLE=true

EXPOSE 9092 29092
