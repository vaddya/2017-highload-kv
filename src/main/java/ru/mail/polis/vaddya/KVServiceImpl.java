package ru.mail.polis.vaddya;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.KVService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;

public class KVServiceImpl implements KVService {

    private static final String QUERY_PREFIX = "id=";
    private static final String METHOD_NOT_ALLOWED = "Method is not allowed";
    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";
    private static final String CONTENT_LENGTH = "Content-Length";

    @NotNull
    private final HttpServer server;
    @NotNull
    private final DAO dao;

    public KVServiceImpl(int port,
                         @NotNull DAO dao) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = dao;

        this.server.createContext(
                "/v0/status",
                http -> sendResponse(http, 200, null)
        );

        this.server.createContext(
                "/v0/entity",
                this::processEntity
        );
    }

    @Override
    public void start() {
        this.server.start();
    }

    @Override
    public void stop() {
        this.server.stop(0);
    }

    private void processEntityGet(@NotNull HttpExchange http,
                                  @NotNull String id) throws IOException {
        final byte[] getValue;
        try {
            getValue = dao.get(id);
            sendResponse(http, 200, getValue);
        } catch (IllegalArgumentException e) {
            sendResponse(http, 400, e.getMessage().getBytes());
        } catch (NoSuchElementException e) {
            sendResponse(http, 404, e.getMessage().getBytes());
        }
    }

    private void processEntityPut(@NotNull HttpExchange http,
                                  @NotNull String id) throws IOException {
        final int size = Integer.valueOf(http.getRequestHeaders().getFirst(CONTENT_LENGTH));
        final byte[] putValue = new byte[size];
        if (size != 0 && http.getRequestBody().read(putValue) != size) {
            throw new IOException("Cannot read in one go");
        }
        try {
            dao.upsert(id, putValue);
            sendResponse(http, 201, null);
        } catch (IllegalArgumentException e) {
            sendResponse(http, 400, e.getMessage().getBytes());
        }
    }

    private void processEntityDelete(@NotNull HttpExchange http,
                                     @NotNull String id) throws IOException {
        try {
            dao.delete(id);
            sendResponse(http, 202, null);
        } catch (IllegalArgumentException e) {
            sendResponse(http, 400, e.getMessage().getBytes());
        }
    }

    private void processEntity(HttpExchange http) throws IOException {
        String id = getId(http.getRequestURI().getQuery());

        switch (http.getRequestMethod()) {
            case METHOD_GET:
                processEntityGet(http, id);
                break;
            case METHOD_PUT:
                processEntityPut(http, id);
                break;
            case METHOD_DELETE:
                processEntityDelete(http, id);
                break;
            default:
                sendResponse(http, 405, METHOD_NOT_ALLOWED.getBytes());
        }
    }

    private String getId(@NotNull String query) {
        if (!query.startsWith(QUERY_PREFIX)) {
            throw new IllegalArgumentException("Query is invalid");
        }
        return query.substring(QUERY_PREFIX.length());
    }

    private void sendResponse(@NotNull HttpExchange http,
                              int code,
                              @Nullable byte[] value) throws IOException {
        if (value != null) {
            http.sendResponseHeaders(code, value.length);
            http.getResponseBody().write(value);
        } else {
            http.sendResponseHeaders(code, 0);
        }
        http.close();
    }

}