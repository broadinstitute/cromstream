package org.broadinstitute.cromstream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.api.services.pubsub.PubsubScopes;
import com.spotify.google.cloud.pubsub.client.Pubsub;
import com.spotify.google.cloud.pubsub.client.Puller;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.net.ssl.SSLContext;
import javax.sql.DataSource;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.time.ZoneId;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import java.util.zip.Deflater;

public class PubSubEventConsumer {

    public static void main(String[] args) throws Exception {
        Config conf = ConfigFactory.load();

        final String project = conf.getString("app.project");
        final String subscription = conf.getString("app.subscription");
        final int concurrency = conf.getInt("app.concurrency");

        GoogleCredential credential = getCredentials(conf);
        DataSource dataSource = getDataSource(conf);

        PubSubEventConsumer app = new PubSubEventConsumer();
        app.doWork(credential, project, subscription, concurrency, dataSource);
    }

    private PubSubEventConsumer() {
    }

    private void doWork(GoogleCredential credential, String googleProject, String subscription, int concurrency, DataSource dataSource) throws Exception {

        System.out.println("Initializing...");


        final Pubsub pubsub = Pubsub.builder()
                .credential(credential)
                .compressionLevel(Deflater.BEST_SPEED)
                .enabledCipherSuites(nonGcmCiphers())
                .build();

        final Puller.MessageHandler handler = (puller, sub, message, ackId) -> {
            final String m = new String(message.decodedData());

            final String sql = "INSERT INTO METADATA_ENTRY(WORKFLOW_EXECUTION_UUID, METADATA_KEY, CALL_FQN, JOB_SCATTER_INDEX, " +
                    "JOB_RETRY_ATTEMPT, METADATA_VALUE, METADATA_VALUE_TYPE, METADATA_TIMESTAMP) VALUES (?,?,?,?,?,?,?,?)";

            ObjectMapper mapper = new ObjectMapper()
                    .registerModule(new ParameterNamesModule())
                    .registerModule(new Jdk8Module())
                    .registerModule(new JavaTimeModule());

            try ( Connection con = dataSource.getConnection();
                  PreparedStatement pst = con.prepareStatement(sql) ){

                MetadataEvent me = mapper.readValue(m, MetadataEvent.class);

                pst.setString(1, me.key.workflowId);
                pst.setString(2, me.key.key);

                // FIXME, this works but is ugly... clean it up
                if (me.key.jobKey != null && me.key.jobKey.callFqn != null) {
                    pst.setString(3, me.key.jobKey.callFqn);
                } else {
                    pst.setNull(3, Types.VARCHAR);
                }

                if (me.key.jobKey != null && me.key.jobKey.index != null) {
                    pst.setInt(4, me.key.jobKey.index);
                } else {
                    pst.setNull(4, Types.INTEGER);
                }

                if (me.key.jobKey != null && me.key.jobKey.attempt != null) {
                    pst.setInt(5, me.key.jobKey.attempt);
                } else {
                    pst.setNull(5, Types.INTEGER);
                }

                if (me.value != null && me.value.value != null) {
                    pst.setString(6, me.value.value);
                } else {
                    pst.setNull(6, Types.VARCHAR);
                }

                if (me.value != null && me.value.valueType != null) {
                    pst.setString(7, me.value.valueType);
                } else {
                    pst.setNull(7, Types.VARCHAR);
                }

                // This is the same conversion done in Cromwell cromwell.database.sql.SqlConverters
                Timestamp timestamp = Timestamp.valueOf(me.offsetDateTime.atZoneSameInstant(ZoneId.systemDefault()).toLocalDateTime());
                pst.setTimestamp(8, timestamp);
                pst.executeUpdate();

                System.out.println("A new event has been inserted for " + me.key.key);

                return CompletableFuture.completedFuture(ackId);
            } catch (IOException | SQLException ex) {
                System.out.println("error!!!" + ex.toString());
                System.out.println("error!!!" + ex.getMessage());

                // since we failed, don't acknowledge the message so it gets redelivered
                return CompletableFuture.completedFuture(ex.getMessage());
            }
        };

        final Puller puller = Puller.builder()
                .pubsub(pubsub)
                .batchSize(1000)
                .concurrency(concurrency)
                .project(googleProject)
                .subscription(subscription)
                .messageHandler(handler)
                .build();

        //noinspection InfiniteLoopStatement
        while (true) {
            Thread.sleep(1000);
            System.out.println("outstanding messages: " + puller.outstandingMessages());
        }


    }

    private static DataSource getDataSource(Config conf) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl( conf.getString("db.url") );
        config.setUsername( conf.getString("db.username")  );
        config.setPassword( conf.getString("db.password") );
        config.addDataSourceProperty( "cachePrepStmts" , "true" );
        config.addDataSourceProperty( "prepStmtCacheSize" , "250" );
        config.addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" );

        return new HikariDataSource( config );
    }

    private static GoogleCredential getCredentials(Config conf) throws IOException {
        final String credsPath = "app.googleJsonCredentials";
        GoogleCredential credential;

        // Use credentials from file if available
        if (conf.hasPath(credsPath)) {
            credential = GoogleCredential
                    .fromStream(new FileInputStream(conf.getString(credsPath)))
                    .createScoped(PubsubScopes.all());
        } else {
            credential = GoogleCredential.getApplicationDefault()
                    .createScoped(PubsubScopes.all());
        }
        return credential;
    }

    /**
     * Return a list of non-GCM ciphers. GCM performance in Java 8 (pre 8u60) is unusably bad and currently worse than
     * CBC in >= 8u60.
     *
     * https://bugs.openjdk.java.net/browse/JDK-8069072
     */
    private static String[] nonGcmCiphers() {
        final SSLContext sslContext;
        try {
            sslContext = SSLContext.getDefault();
        } catch (NoSuchAlgorithmException e) {
            throw Throwables.propagate(e);
        }

        final String[] defaultCiphers = sslContext.getDefaultSSLParameters().getCipherSuites();

        return Stream.of(defaultCiphers)
                .filter(cipher -> !cipher.contains("GCM"))
                .toArray(String[]::new);
    }

}
