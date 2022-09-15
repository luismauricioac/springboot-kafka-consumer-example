package com.mauricio.springbootkafkaconsumerexample.constants;

public class ApplicationConstants {

    private ApplicationConstants(){
        // Private Constructor
    }

    //Constants related to Kafka configuration properties
    public static final String BASIC_AUTH_CREDENTIALS_SOURCE = "basic.auth.credentials.source";
    public static final String SCHEMA_REGISTRY_URL = "schema.registry.url";
    public static final String SECURITY_PROTOCOL = "security.protocol";
    public static final String SASL_JAAS_CONFIG = "sasl.jaas.config";

}
