package com.garanti.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaConfig {

    private final String bootstrapServers;
    private final String groupId;
    private final boolean useKerberos;
    private final String truststoreLocation;
    private final String truststorePassword;

    public KafkaConfig(String bootstrapServers, String groupId, boolean useKerberos,
                       String truststoreLocation, String truststorePassword) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
        this.useKerberos = useKerberos;
        this.truststoreLocation = truststoreLocation;
        this.truststorePassword = truststorePassword;
    }

    public Properties getConsumerProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        if (useKerberos) {
            addKerberosConfig(props);
        }

        return props;
    }

    public Properties getProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        if (useKerberos) {
            addKerberosConfig(props);
        }

        return props;
    }

    private void addKerberosConfig(Properties props) {
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "GSSAPI");
        props.put("ssl.truststore.type", "JKS");
        props.put("ssl.truststore.location", truststoreLocation);
        props.put("ssl.truststore.password", truststorePassword);
        props.put("ssl.secure.random.implementation", "SHA1PRNG");
        props.put("sasl.kerberos.service.name", "kafka");
    }
}