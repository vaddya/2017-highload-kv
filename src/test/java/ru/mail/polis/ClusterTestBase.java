package ru.mail.polis;

import com.google.common.collect.Iterators;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Set;

/**
 * Facilities for cluster tests
 *
 * @author Vadim Tsesko <incubos@yandex.com>
 */
abstract class ClusterTestBase extends TestBase {
    Set<String> endpoints;

    @NotNull
    private String url(
            final int node,
            @NotNull final String id,
            final int rf) {
        final String endpoint = Iterators.get(endpoints.iterator(), node);
        return endpoint + "/v0/entity?id=" + id + (rf == 2 ? "" : "&replicas=" + rf);
    }

    HttpResponse get(
            final int node,
            @NotNull final String key,
            final int rf) throws IOException {
        return Request.Get(url(node, key, rf)).execute().returnResponse();
    }

    HttpResponse delete(
            final int node,
            @NotNull final String key,
            final int rf) throws IOException {
        return Request.Delete(url(node, key, rf)).execute().returnResponse();
    }

    HttpResponse upsert(
            final int node,
            @NotNull final String key,
            @NotNull final byte[] data,
            final int rf) throws IOException {
        return Request.Put(url(node, key, rf)).bodyByteArray(data).execute().returnResponse();
    }
}
