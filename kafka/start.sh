sudo systemctl daemon-reload
sudo systemctl start zookeeper
sudo systemctl enable zookeeper
sudo /opt/zookeeper/bin/zkCli.sh -server 127.0.0.1:2181

cd /opt/kafka/kafka-3.9.0-src
bin/kafka-storage.sh random-uuid
bin/kafka-storage.sh format -t oBkAyrWYSJC_eUWJtHk7Yg -c config/kraft/server.properties
sudo  bin/kafka-server-start.sh config/kraft/server.properties
sudo bin/kafka-topics.sh --create --topic my-first-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
bin/kafka-console-producer.sh --topic my-first-topic --bootstrap-server localhost:9092
bin/kafka-console-consumer.sh --topic my-first-topic --from-beginning --bootstrap-server localhost:9092
