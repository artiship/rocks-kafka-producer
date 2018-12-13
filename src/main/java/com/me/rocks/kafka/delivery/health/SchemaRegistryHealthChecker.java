package com.me.rocks.kafka.delivery.health;

import com.me.rocks.kafka.config.RocksProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.URL;

public class SchemaRegistryHealthChecker {
    private static final Logger log = LoggerFactory.getLogger(SchemaRegistryHealthChecker.class);

    private static final String schema_url = RocksProducerConfig.getSchemaRegistryUrl();
    private URL url;
    private HttpURLConnection conn;

    public boolean isAvailable() {
        try {
            if(url == null) {
                url = new URL(schema_url);
            }

            conn = (HttpURLConnection) url.openConnection();
            //Setting timeout
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);
            conn.setRequestMethod("GET");
            conn.connect();

            if(conn.getResponseCode() == 200) {
                return true;
            }
        } catch (Exception e) {
            log.error("Kafka Schema Registry {} is not available", schema_url);
        } finally {
            conn.disconnect();
        }

        return false;
    }

    public void clear() {
        if(this.conn != null) {
            this.conn.disconnect();
        }
    }
}
