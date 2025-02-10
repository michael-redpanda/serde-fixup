package com.redpanda;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class SerdeFixupMain {

    private static final Logger logger = LogManager.getLogger(SerdeFixupMain.class);

    public static void main(String[] args) {
        logger.info("Hello, world!");
        var parser = createArgumentParser();
        Namespace ns = null;
        try {
            ns = parser.parseArgs(args);
        } catch(ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }

        logger.info("Arguments: " + ns);

        SerdeFixup fixup = new SerdeFixup(ns.getString("brokers"), ns.getString("schema_registry"), ns.getString("topic"));
        fixup.run();
    }

    private static ArgumentParser createArgumentParser() {
        ArgumentParser parser = ArgumentParsers.newFor("Java Kafka Serde Fixup").build().defaultHelp(true).description("Fixer-upper");

        parser.addArgument("-s", "--schema-registry").help("URL for Schema Registry").required(true);
        parser.addArgument("-b", "--brokers").help("Kafka brokers").required(true);
        parser.addArgument( "-t", "--topic").help("Kafka topic").required(true);
        return parser;
    }
}
