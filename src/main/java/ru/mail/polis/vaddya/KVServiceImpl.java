package ru.mail.polis.vaddya;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.KVService;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.util.*;
import java.io.*;
import java.net.*;

public class KVServiceImpl implements KVService {

    private static final String URL_STATUS = "/v0/status";
    private static final String URL_INNER = "/v0/inner";
    private static final String URL_ENTITY = "/v0/entity";

    private static final String QUERY_ID = "id";
    private static final String QUERY_REPLICAS = "replicas";

    private static final String METHOD_NOT_ALLOWED = "Method is not allowed";
    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";

    private static final int BUFFER_SIZE = 1024;

    @NotNull
    private final HttpServer server;
    @NotNull
    private final DAO dao;
    @NotNull
    private final Set<String> topolgy;

    public KVServiceImpl(int port,
                         @NotNull DAO dao,
                         Set<String> topolgy) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = dao;
        this.topolgy = topolgy;

        server.createContext(URL_STATUS, this::processStatus);
        server.createContext(URL_INNER, this::processInner);
//        server.createContext(URL_ENTITY, this::processInner);
        server.createContext(URL_ENTITY, this::processEntity);
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
    }

    //    =========================================

    private void processStatus(HttpExchange http) throws IOException {
        sendResponse(http, 200, null);
    }

    //    =========================================

    private void processInner(HttpExchange http) throws IOException {
        try {
            QueryParams params = parseQuery(http.getRequestURI().getQuery());

            switch (http.getRequestMethod()) {
                case METHOD_GET:
                    processInnerGet(http, params);
                    break;
                case METHOD_PUT:
                    processInnerPut(http, params);
                    break;
                case METHOD_DELETE:
                    processInnerDelete(http, params);
                    break;
                default:
                    sendResponse(http, 405, METHOD_NOT_ALLOWED.getBytes());
                    break;
            }
        } catch (IllegalArgumentException e) {
            sendResponse(http, 400, e.getMessage().getBytes());
        }
    }

    private void processInnerGet(@NotNull HttpExchange http,
                                 @NotNull QueryParams params) throws IOException {
        try {
            String id = params.getId();
            final byte[] getValue = dao.get(id);
            sendResponse(http, 200, getValue);
        } catch (IllegalArgumentException e) {
            sendResponse(http, 400, e.getMessage().getBytes());
        } catch (NoSuchElementException e) {
            sendResponse(http, 404, e.getMessage().getBytes());
        }
    }

    private void processInnerPut(@NotNull HttpExchange http,
                                 @NotNull QueryParams params) throws IOException {
        try {
            String id = params.getId();
            final byte[] data = readData(http.getRequestBody());
            dao.upsert(id, data);
            sendResponse(http, 201, null);
        } catch (IOException | IllegalArgumentException e) {
            sendResponse(http, 400, e.getMessage().getBytes());
        }
    }

    private void processInnerDelete(@NotNull HttpExchange http,
                                    @NotNull QueryParams params) throws IOException {
        try {
            String id = params.getId();
            dao.delete(id);
            sendResponse(http, 202, null);
        } catch (IllegalArgumentException e) {
            sendResponse(http, 400, e.getMessage().getBytes());
        }
    }

    //    =========================================

    private void processEntity(@NotNull HttpExchange http) throws IOException {
        try {
            QueryParams params = parseQuery(http.getRequestURI().getQuery());

            switch (http.getRequestMethod()) {
                case METHOD_GET:
                    processEntityGet(http, params);
                    break;
                case METHOD_PUT:
                    processEntityPut(http, params);
                    break;
                case METHOD_DELETE:
                    processEntityDelete(http, params);
                    break;
                default:
                    sendResponse(http, 405, METHOD_NOT_ALLOWED.getBytes());
                    break;
            }
        } catch (IllegalArgumentException e) {
            sendResponse(http, 400, e.getMessage().getBytes());
        }
    }

    private Response makeGetRequest(@NotNull String link,
                                    @NotNull String method,
                                    @NotNull String params) throws IOException {
        URL url = new URL(link + params);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod(method);
        conn.setUseCaches(false);
        conn.setDoInput(true);
        conn.setDoOutput(true);
        conn.setReadTimeout(2 * 1000);
        conn.connect();
        // TODO: 11/18/17
//        InputStream is = conn.getInputStream();
//        byte[] data = readData(is);
        byte[] data = null;
        int code = conn.getResponseCode();
        conn.disconnect();
        return new Response(code, data);
    }

    private void processEntityGet(@NotNull HttpExchange http,
                                  @NotNull QueryParams params) throws IOException {
        try {
            String id = params.getId();
            final byte[] getValue = dao.get(id);

            int ack = params.getAck();
            int from = params.getFrom();

            // TODO: 11/18/17
            int numberOf200 = 0;
            int numberOf404 = 0;

            for (String url : topolgy) {
                Response resp = makeGetRequest(url + URL_INNER, "GET", "?id=" + id);
                if (resp.getCode() == 404) {
                    numberOf404 += 1;
                } else {
                    numberOf200 += 1;
                }
            }
            sendResponse(http, 200, getValue);
        } catch (IllegalArgumentException e) {
            sendResponse(http, 400, e.getMessage().getBytes());
        } catch (NoSuchElementException e) {
            sendResponse(http, 404, e.getMessage().getBytes());
        }
    }

    private void processEntityPut(@NotNull HttpExchange http,
                                  @NotNull QueryParams params) throws IOException {
        try {
            String id = params.getId();
            final byte[] data = readData(http.getRequestBody());
            dao.upsert(id, data);
            sendResponse(http, 201, null);
        } catch (IOException | IllegalArgumentException e) {
            sendResponse(http, 400, e.getMessage().getBytes());
        }
    }

    private void processEntityDelete(@NotNull HttpExchange http,
                                     @NotNull QueryParams params) throws IOException {
        try {
            String id = params.getId();
            dao.delete(id);
            sendResponse(http, 202, null);
        } catch (IllegalArgumentException e) {
            sendResponse(http, 400, e.getMessage().getBytes());
        }
    }

    //    =========================================

    private QueryParams parseQuery(@NotNull String query) {
        Map<String, String> params = parseParams(query);
        String id = params.get(QUERY_ID);
        int ack;
        int from;
        if (params.containsKey(QUERY_REPLICAS)) {
            String replParams[] = params.get(QUERY_REPLICAS).split("/");
            ack = Integer.valueOf(replParams[0]);
            from = Integer.valueOf(replParams[1]);
        } else {
            ack = topolgy.size() / 2 + 1;
            from = topolgy.size();
        }

        if (id == null || ack < 1 || from < 1 || ack > from) {
            throw new IllegalArgumentException("Query is invalid");
        }

        return new QueryParams(id, ack, from);
    }

    private Map<String, String> parseParams(@NotNull String query) {
        try {
            Map<String, String> params = new LinkedHashMap<>();
            for (String param : query.split("&")) {
                int idx = param.indexOf("=");
                params.put(URLDecoder.decode(param.substring(0, idx), "UTF-8"),
                        URLDecoder.decode(param.substring(idx + 1), "UTF-8"));
            }
            return params;
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("Query is invalid");
        }
    }

    private byte[] readData(InputStream is) throws IOException {
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[BUFFER_SIZE];
            for (int len; (len = is.read(buffer, 0, BUFFER_SIZE)) != -1; ) {
                os.write(buffer, 0, len);
            }
            os.flush();
            return os.toByteArray();
        }
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

    private class QueryParams {

        private final String id;
        private final int ack;
        private final int from;

        QueryParams(String id, int ack, int from) {
            this.id = id;
            this.ack = ack;
            this.from = from;
        }

        String getId() {
            return id;
        }

        int getAck() {
            return ack;
        }

        int getFrom() {
            return from;
        }
    }

    private class Response {

        private final int code;
        private final byte[] data;

        Response(int code, byte[] data) {
            this.code = code;
            this.data = data;
        }

        int getCode() {
            return code;
        }

        byte[] getData() {
            return data;
        }
    }

}
