package com.redpanda;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;

public class SerdeFixup {
    private static final Logger logger = LogManager.getLogger(SerdeFixup.class);
    private SchemaRegistryClient schemaRegistryClient;
    private Properties props;
    private String topic;
    private String subject;

    public SerdeFixup(String brokers, String schemaRegistryUrl, String topic) {
        this.props = new Properties();
        this.props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        this.props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        this.props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class.getName());
        this.props.put("schema.registry.url", schemaRegistryUrl);
        // Use the latest schema version from Schema Registry
        this.props.put("auto.register.schemas", "false");
        this.props.put("use.latest.version", "true");
        this.schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 20, Collections.singletonList(new ProtobufSchemaProvider()), null);
        this.topic = topic;
        this.subject = this.topic + "-value";
    }

    private Map<String, String> resolveReferencesFully(SchemaMetadata schema) throws IOException, RestClientException {
        logger.info("Resolving references fully for " + schema.getSubject() + " with references " + schema.getReferences());
        Map<String, String> rv = new HashMap<>();
        for (SchemaReference ref : schema.getReferences()) {
            logger.info("Getting reference " + ref.getSubject() + ":v" + ref.getVersion());
            SchemaMetadata refMetadata = this.schemaRegistryClient.getSchemaMetadata(ref.getSubject(), ref.getVersion());
            rv.put(refMetadata.getSubject(), refMetadata.getSchema());
            if (!refMetadata.getReferences().isEmpty()) {
                logger.info("Getting references for " + refMetadata.getSubject() + ": " + refMetadata.getReferences());
                rv.putAll(resolveReferencesFully(refMetadata));
            }
        }

        return rv;
    }

    /**
     * This function runs it all
     */
    public void run() {
        logger.info("Running");
        try {
            logger.info("Fetching schema metadata for " + this.subject);
            SchemaMetadata mainSchemaMetadata = this.schemaRegistryClient.getLatestSchemaMetadata(this.subject);
            logger.info("Schema data for " + this.subject + ": " + mainSchemaMetadata);
            ParsedSchema mainSchema = this.schemaRegistryClient.getSchemaById(mainSchemaMetadata.getId());
            logger.info("Parsed schema " + mainSchema);
            logger.info("Parsed schema canonical: " + mainSchema.canonicalString());
            
            logger.info("Fetching all references");
            Map<String, String> resolvedReferences = this.resolveReferencesFully(mainSchemaMetadata);
            logger.info("resolved References:\n" + resolvedReferences);

            logger.info("Creating protobuf schema");
            ProtobufSchema pschema = new ProtobufSchema(mainSchema.canonicalString(), mainSchema.references(), resolvedReferences, mainSchema.version(), mainSchema.name());
            logger.info("Creating descriptor");
            Descriptors.Descriptor descriptor = pschema.toDescriptor();
            
            logger.info("Creating builder");
            DynamicMessage.Builder mb = DynamicMessage.newBuilder(descriptor);

            logger.info("Creating producer");
            Producer<String, DynamicMessage> producer = new KafkaProducer<>(this.props);

            logger.info("Creating record");
            ProducerRecord<String, DynamicMessage> record = new ProducerRecord<>(this.topic, "key", mb.build());

            logger.info("Producing");
            producer.send(record, (metadata, exception) -> {
                logger.info("Record sent");
                if (exception == null) {
                    logger.info("Sent message to " + metadata.topic() + " partition " + metadata.partition() + " @ offset " + metadata.offset());
                } else {
                    logger.error("Error sending message", exception);
                }
            });
            logger.info("Closing producer");
            producer.close();
            
        } catch(IOException e) {
            logger.error("I/O Exception", e);
            return;
        } catch(RestClientException e) {
            logger.error("Error fetching schema metadata", e);
            return;
        }
        logger.info("Run completed");
    }
}
