package test;

import org.jdbi.v3.core.Jdbi;
import org.testcontainers.containers.CockroachContainer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.http.HttpClient.newHttpClient;

public final class ExtendedCockroachContainer
        extends CockroachContainer
{
    private static final String COCKROACH_VERSION = "21.2";
    private static final String COCKROACH_CONTAINER = "cockroachdb/cockroach:latest-v%s".formatted(COCKROACH_VERSION);
    private static final String COCKROACH_LICENSE_URL = "https://register.cockroachdb.com/api/license?kind=demo&version=v%s&clusterid=%s";
    private static final String REGION = "us-east1";

    public ExtendedCockroachContainer()
    {
        super(COCKROACH_CONTAINER);
        withCommand("start-single-node --insecure --locality=region=%s".formatted(REGION));
    }

    @Override
    protected void runInitScriptIfRequired()
    {
        super.runInitScriptIfRequired();
        Jdbi.create(() -> createConnection("")).useHandle(handle -> {
            String clusterId = handle.select("SELECT crdb_internal.cluster_id()").mapTo(String.class).one();
            handle.execute("SET CLUSTER SETTING \"enterprise.license\" = ?", getCockroachLicense(clusterId));
            handle.execute("SET CLUSTER SETTING \"kv.closed_timestamp.lead_for_global_reads_override\" = '1ms'");
            handle.execute("ALTER DATABASE %s SET PRIMARY REGION \"%s\"".formatted(getDatabaseName(), REGION));
        });
    }

    private static String getCockroachLicense(String clusterId)
    {
        URI uri = URI.create(COCKROACH_LICENSE_URL.formatted(COCKROACH_VERSION, clusterId));
        HttpRequest request = HttpRequest.newBuilder(uri).build();
        try {
            HttpResponse<String> response = newHttpClient().send(request, BodyHandlers.ofString());
            if (response.statusCode() != HTTP_OK) {
                throw new RuntimeException("Invalid Cockroach license response: " + response);
            }
            return response.body();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
