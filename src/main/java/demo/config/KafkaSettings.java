package demo.config;

import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.ProducerSettings;
import io.vavr.Tuple;
import io.vavr.control.Option;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;

public class KafkaSettings {

    private final static Logger LOGGER = LoggerFactory.getLogger(KafkaSettings.class);

    private final String servers;

    private final Option<String> keyStorePath;
    private final Option<String> trustStorePath;
    private final Option<String> keyPass;

    public KafkaSettings(String servers, Option<String> keyStorePath, Option<String> trustStorePath, Option<String> keyPass) {
        this.servers = servers;
        this.keyStorePath = keyStorePath;
        this.trustStorePath = trustStorePath;
        this.keyPass = keyPass;
    }

    public static KafkaSettings fromConfig(String path, Environment config) {
        return  new KafkaSettings(
                config.getProperty(path+".servers"),
                Option.of(config.getProperty(path+".keystore.location")),
                Option.of(config.getProperty(path+".truststore.location")),
                Option.of(config.getProperty(path+".keypass"))
        );
    }

    public KafkaSettings(String servers) {
        this(servers, Option.none(), Option.none(), Option.none());
    }

    public String servers() {
        return servers;
    }

    public Option<String> keyStorePath() {
        return keyStorePath;
    }

    public Option<String> trustStorePath() {
        return trustStorePath;
    }

    public Option<String> keyPass() {
        return keyPass;
    }

    public ConsumerSettings<String, String> consumerSettings(ActorSystem system, String groupId) {
        return consumerSettings(system, groupId, new StringDeserializer());
    }

    public <S> ConsumerSettings<String, S> consumerSettings(ActorSystem system, String groupId, Deserializer<S> deserializer) {

        final ConsumerSettings<String, S> settings = ConsumerSettings
                .create(system, new StringDeserializer(), deserializer)
                .withGroupId(groupId)
                .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
                .withBootstrapServers(this.servers);

        ConsumerSettings<String, S> consumerSettings = this.keyStorePath
                .flatMap(ksp -> this.trustStorePath.map(tsp -> Tuple.of(ksp, tsp)))
                .map(trustores -> settings
                        .withProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL")
                        .withProperty(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "required")
                        .withProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, null)
                        .withProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, this.keyPass.get())
                        .withProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, trustores._1)
                        .withProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, this.keyPass.get())
                        .withProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustores._2)
                        .withProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, this.keyPass.get())
                )
                .getOrElse(settings);

        return consumerSettings;
    }

    public ProducerSettings<String, String> producerSettings(ActorSystem system) {
        return producerSettings(system, new StringSerializer());
    }

    public <S> ProducerSettings<String, S> producerSettings(ActorSystem system, Serializer<S> serializer) {
        ProducerSettings<String, S> settings = ProducerSettings
                .create(system, new StringSerializer(), serializer)
                .withBootstrapServers(this.servers);

        return this.keyStorePath
                .flatMap(ksp -> this.trustStorePath.map(tsp -> Tuple.of(ksp, tsp)))
                .map(trustores -> settings
                        .withProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL")
                        .withProperty(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "required")
                        .withProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, null)
                        .withProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, trustores._1)
                        .withProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, this.keyPass.get())
                        .withProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustores._2)
                        .withProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, this.keyPass.get())
                )
                .getOrElse(settings);
    }
}
