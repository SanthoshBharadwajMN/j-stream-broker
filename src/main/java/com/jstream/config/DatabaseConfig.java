package com.jstream.config;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.postgresql.client.SSLMode;
import io.r2dbc.spi.ConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration;

import java.util.Map;

@Configuration
public class DatabaseConfig extends AbstractR2dbcConfiguration {

    @Override
    @Bean
    public ConnectionFactory connectionFactory() {
        String endpointId = System.getenv("NEON_ENDPOINT_ID");
        String username = System.getenv("NEON_USERNAME");
        String password = System.getenv("NEON_PASSWORD");

        if (endpointId == null || username == null || password == null) {
            throw new IllegalStateException("CRITICAL: Missing Neon credentials for Postgres connection");
        }

        return new PostgresqlConnectionFactory(
                PostgresqlConnectionConfiguration.builder()
                        .host(endpointId + ".c-4.us-east-2.aws.neon.tech")
                        .port(5432)
                        .database("neondb")
                        .username(username)
                        .password(password)
                        .sslMode(SSLMode.REQUIRE)
                        .options(Map.of("options", "endpoint=" + endpointId))
                        .build()
        );
    }
}
