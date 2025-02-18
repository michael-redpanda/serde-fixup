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
import org.apache.kafka.common.errors.SerializationException;
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
        this.props.put("latest.compatibility.strict", "false");
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

    private Map<String, String> resolveReferencesFully() throws IOException, RestClientException {
        return this.resolveReferencesFully(this.getSchemaMetadata());
    }

    private void uploadNewSchema(ParsedSchema schema) throws IOException, RestClientException {
        logger.trace("uploadNewSchema");
        int id = this.schemaRegistryClient.register(this.subject, schema);
        logger.info("New schema: " + id);
    }

    private SchemaMetadata getSchemaMetadata() throws IOException, RestClientException {
        logger.trace("getSchemaMetadata");
        SchemaMetadata md = this.schemaRegistryClient.getLatestSchemaMetadata(this.subject);
        logger.debug("Schema:\n" + md.getSchema());
        logger.debug("References:\n" + md.getReferences());
        return md;
    }

    private ParsedSchema getParsedSchema() throws IOException, RestClientException {
        return this.getParsedSchema(this.getSchemaMetadata());
    }

    private ParsedSchema getParsedSchema(SchemaMetadata md)  throws IOException, RestClientException {
        logger.trace("getParsedSchema");
        ParsedSchema ps = this.schemaRegistryClient.getSchemaById(md.getId());
        logger.debug("Parsed schema:\n" + ps.canonicalString());
        return ps;
    }

    private ProtobufSchema getProtobufSchema() throws IOException, RestClientException {
        return this.getProtobufSchema(this.getSchemaMetadata(), this.getParsedSchema());
    }


    private ProtobufSchema getProtobufSchema(SchemaMetadata md, ParsedSchema pschema) throws IOException, RestClientException {
        return this.getProtobufSchema(pschema, this.resolveReferencesFully(md));
    }

    private ProtobufSchema getProtobufSchema(ParsedSchema pschema, Map<String, String> resolvedReferences) throws IOException, RestClientException {
        logger.trace("getProtobufSchema");
        return new ProtobufSchema(pschema.canonicalString(), pschema.references(), resolvedReferences, pschema.version(), pschema.name());
    }

    private void sendMessage(Descriptors.Descriptor descriptor) throws SerializationException {
        logger.trace("sendMessage");
        DynamicMessage.Builder mb = DynamicMessage.newBuilder(descriptor);
        try (Producer<String, DynamicMessage> producer = new KafkaProducer<>(this.props)) {
            ProducerRecord<String, DynamicMessage> record = new ProducerRecord<>(this.topic, "key", mb.build());
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    logger.info("Sent message to " + metadata.topic() + " partition " + metadata.partition() + " @ offset " + metadata.offset());
                } else {
                    logger.error("Error sending message", exception);
                }
            });
        }
    }

    public void run() {
        logger.info("Starting run...");
        ProtobufSchema pschema = null;
        
        try {
            logger.debug("Fetching descriptor for " + this.subject);
            pschema = getProtobufSchema();
        } catch (IOException e) {
            logger.error("Failed I/O exception getting descriptor for " + this.subject, e);
            return;
        } catch(RestClientException e) {
            logger.error("Failed with Rest client error for " + this.subject, e); 
            return;
        }

        logger.debug("pschema: " + pschema);
        Descriptors.Descriptor descriptor = pschema.toDescriptor();

        logger.debug("descriptor: " + descriptor);

        var success = false;
        var reloadSchema = false;
        try {
            logger.debug("Sending message");
            this.sendMessage(descriptor);
            logger.info("Successfully sent message");
            success = true;
        }catch(SerializationException e) {
            success = false;
            if (e.getCause() instanceof RestClientException restError) {
                if (40403 == restError.getErrorCode()) {
                    logger.warn("Failed to produce data due to subject not found");
                    reloadSchema = true;
                } else {
                    logger.warn("RestClientException on produce to " + this.subject, restError);
                }
            } else {
                logger.warn("SerializationException to " + this.subject, e);
            }
        }

        if (success) {
            logger.info("Successful run for " + this.subject);
            return;
        } else if (!reloadSchema) {
            logger.warn("Failed to produce but not a reload event for " + this.subject);
            return;
        }

        logger.info("Uploading new schema");
        try {
            this.uploadNewSchema(this.getParsedSchema());
        } catch (IOException e) {
            logger.error("Failed I/O exception uploading new schema for " + this.subject, e);
            return;
        } catch(RestClientException e) {
            logger.error("Failed with Rest client error uploading new schema for " + this.subject, e); 
            return;
        }

        logger.info("Succesfully uploaded new schema, retyring produce");

        try {
            logger.debug("Fetching descriptor for " + this.subject);
            descriptor = getProtobufSchema().toDescriptor();
        } catch (IOException e) {
            logger.error("Failed I/O exception getting descriptor for " + this.subject, e);
            return;
        } catch(RestClientException e) {
            logger.error("Failed with Rest client error for " + this.subject, e); 
            return;
        }

        try {
            logger.debug("Sending message");
            this.sendMessage(descriptor);
            logger.info("Successfully sent message with new schema!");
            return;
        }catch(SerializationException e) {
            if (e.getCause() instanceof RestClientException restError) {
                if (40403 == restError.getErrorCode()) {
                    logger.warn("Failed to produce data due to subject not found");
                    reloadSchema = true;
                } else {
                    logger.warn("RestClientException on produce to " + this.subject, restError);
                }
            } else {
                logger.warn("SerializationException to " + this.subject, e);
            }
        }

        logger.error("Failed to produce with new schema for " + this.subject);
    }
}
