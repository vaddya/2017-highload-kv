package ru.mail.polis.vaddya;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.KVService;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;

public class KVServiceImpl implements KVService {

    private static final String QUERY_PREFIX = "id=";
    private static final String METHOD_NOT_ALLOWED = "Method is not allowed";
    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";

    @NotNull
    private final HttpServer server;
    @NotNull
    private final DAO dao;

    public KVServiceImpl(int port,
                         @NotNull DAO dao) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = dao;

        server.createContext(
                "/v0/status",
                http -> sendResponse(http, 200, null)
        );

        server.createContext(
                "/v0/entity",
                this::processEntity
        );
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
    }

    private void processEntityGet(@NotNull HttpExchange http,
                                  @NotNull String id) throws IOException {
        try {
            final byte[] getValue = dao.get(id);
            sendResponse(http, 200, getValue);
        } catch (IllegalArgumentException e) {
            sendResponse(http, 400, e.getMessage().getBytes());
        } catch (NoSuchElementException e) {
            sendResponse(http, 404, e.getMessage().getBytes());
        }
    }

    private byte[] readData(HttpExchange http) throws IOException {
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[1024];
            for (int len; (len = http.getRequestBody().read(buffer)) != -1; ) {
                os.write(buffer, 0, len);
            }
            os.flush();
            return os.toByteArray();
        }
    }

    private void processEntityPut(@NotNull HttpExchange http,
                                  @NotNull String id) throws IOException {
        try {
            final byte[] data = readData(http);
            dao.upsert(id, data);
            sendResponse(http, 201, null);
        } catch (IOException | IllegalArgumentException e) {
            sendResponse(http, 400, e.getMessage().getBytes());
        }
    }

    private void processEntityDelete(@NotNull HttpExchange http,
                                     @NotNull String id) throws IOException {
        try {
            this.dao.delete(id);
            sendResponse(http, 202, null);
        } catch (IllegalArgumentException e) {
            sendResponse(http, 400, e.getMessage().getBytes());
        }
    }

    private void processEntity(HttpExchange http) throws IOException {
        try {
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
                    break;
            }

        } catch (IllegalArgumentException e) {
            sendResponse(http, 400, e.getMessage().getBytes());
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
