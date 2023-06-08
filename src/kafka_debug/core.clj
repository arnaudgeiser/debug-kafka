(ns kafka-debug.core
  (:import java.util.Properties
           org.apache.kafka.clients.consumer.KafkaConsumer))

(def lock (Object.))

(defn consumer-props []
  (doto (Properties.)
    (.put "bootstrap.servers" "91.92.117.222:21701")
    (.put "group.id" (str (random-uuid)))
    (.put "security.protocol" "SSL")
    (.put "ssl.truststore.location" "/home/arnaudgeiser/test-kafka-truststore.jks")
    (.put "ssl.truststore.password" "dummypassword")
    (.put "ssl.keystore.location" "/home/arnaudgeiser/test-kafka.jks")
    (.put "ssl.keystore.password" "dummypassword")
    (.put "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer")
    (.put "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer")))

(defn make-consumer []
  (KafkaConsumer. (consumer-props)))

(defn worker []
  (let [consumer (make-consumer)]
    (.subscribe consumer ["demo-topic2"])
    (loop []
      (when-let [records (.poll consumer Long/MAX_VALUE)]
        (locking lock
          (prn (str (.getName (Thread/currentThread)) " " records)))
        (recur)))))

(defn run []
  (mapv (fn [_] (future (worker))) (range 20)))

(comment
  (run))
