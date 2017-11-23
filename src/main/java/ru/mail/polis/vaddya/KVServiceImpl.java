package ru.mail.polis.vaddya;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.KVService;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLDecoder;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import static ru.mail.polis.vaddya.KVServiceImpl.Method.DELETE;
import static ru.mail.polis.vaddya.KVServiceImpl.Method.GET;
import static ru.mail.polis.vaddya.KVServiceImpl.Method.PUT;
import static ru.mail.polis.vaddya.Response.*;

public class KVServiceImpl implements KVService {

    private static final String URL_STATUS = "/v0/status";
    private static final String URL_INNER = "/v0/inner";
    private static final String URL_ENTITY = "/v0/entity";

    private static final String QUERY_ID = "id";
    private static final String QUERY_REPLICAS = "replicas";

    private static final String METHOD_IS_NOT_ALLOWED = "Method is not allowed";
    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";

    private static final int BUFFER_SIZE = 1024;

    @NotNull
    private final HttpServer server;
    @NotNull
    private final DAO dao;
    @NotNull
    private final List<String> topology;

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public KVServiceImpl(int port,
                         @NotNull DAO dao,
                         Set<String> topology) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = dao;
        this.topology = new ArrayList<>(topology);

        server.createContext(URL_STATUS, this::processStatus);
        server.createContext(URL_INNER, this::processInner);
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
        sendResponse(http, new Response(OK));
    }

    //    =========================================

    private void processInner(HttpExchange http) throws IOException {
        try {
            QueryParams params = parseQuery(http.getRequestURI().getQuery());

            Response resp;
            switch (http.getRequestMethod()) {
                case METHOD_GET:
                    resp = processInnerGet(params);
                    break;
                case METHOD_PUT:
                    final byte[] data = readData(http.getRequestBody());
                    resp = processInnerPut(params, data);
                    break;
                case METHOD_DELETE:
                    resp = processInnerDelete(params);
                    break;
                default:
                    resp = new Response(NOT_ALLOWED, METHOD_IS_NOT_ALLOWED);
                    break;
            }
            sendResponse(http, resp);
        } catch (IllegalArgumentException e) {
            sendResponse(http, new Response(BAD_REQUEST, e.getMessage()));
        }
    }

    private Response processInnerGet(@NotNull QueryParams params) throws IOException {
        try {
            String id = params.getId();
            final byte[] getValue = dao.get(id);
            return new Response(OK, getValue);
        } catch (IllegalArgumentException e) {
            return new Response(BAD_REQUEST, e.getMessage());
        } catch (NoSuchElementException e) {
            return new Response(NOT_FOUND, e.getMessage());
        }
    }

    private Response processInnerPut(@NotNull QueryParams params,
                                     @NotNull byte[] data) throws IOException {
        try {
            String id = params.getId();
            dao.upsert(id, data);
            return new Response(CREATED);
        } catch (IOException | IllegalArgumentException e) {
            return new Response(BAD_REQUEST, e.getMessage());
        }
    }

    private Response processInnerDelete(@NotNull QueryParams params) throws IOException {
        try {
            String id = params.getId();
            dao.delete(id);
            return new Response(ACCEPTED);
        } catch (IllegalArgumentException e) {
            return new Response(BAD_REQUEST, e.getMessage());
        }
    }

    //    =========================================

    private void processEntity(@NotNull HttpExchange http) throws IOException {
        try {
            QueryParams params = parseQuery(http.getRequestURI().getQuery());

            Response resp;
            switch (http.getRequestMethod()) {
                case METHOD_GET:
                    resp = processEntityGet(params);
                    break;
                case METHOD_PUT:
                    final byte[] data = readData(http.getRequestBody());
                    resp = processEntityPut(params, data);
                    break;
                case METHOD_DELETE:
                    resp = processEntityDelete(params);
                    break;
                default:
                    resp = new Response(NOT_ALLOWED, METHOD_IS_NOT_ALLOWED);
                    break;
            }
            sendResponse(http, resp);
        } catch (IllegalArgumentException e) {
            sendResponse(http, new Response(BAD_REQUEST, e.getMessage()));
        }
    }

    private List<FutureTask<Response>> executeTasks(@NotNull Method method,
                                                    @NotNull QueryParams params,
                                                    @NotNull List<String> nodes,
                                                    @Nullable byte[] data) throws IOException {
        List<FutureTask<Response>> futures = new ArrayList<>();
        String self = "http://localhost:" + server.getAddress().getPort();
        for (String url : nodes) {
            FutureTask<Response> future;
            if (url.equals(self)) {
                switch (method) {
                    case GET:
                        future = new FutureTask<>(() -> processInnerGet(params));
                        break;
                    case PUT:
                        future = new FutureTask<>(() -> processInnerPut(params, data));
                        break;
                    case DELETE:
                        future = new FutureTask<>(() -> processInnerDelete(params));
                        break;
                    default:
                        throw new IllegalArgumentException(METHOD_IS_NOT_ALLOWED);
                }
            } else {
                if (method == PUT) {
                    future = new FutureTask<>(
                            () -> makeRequest(method, url + URL_INNER, "?id=" + params.getId(), data));
                } else {
                    future = new FutureTask<>(
                            () -> makeRequest(method, url + URL_INNER, "?id=" + params.getId(), null));
                }
            }
            futures.add(future);
            executor.execute(future);
        }
        return futures;
    }

    private Response processEntityGet(@NotNull QueryParams params) throws IOException {
        try {
            int ack = params.getAck();
            int from = params.getFrom();

            String id = params.getId();
            List<String> nodes = getNodesById(id, from);
            List<FutureTask<Response>> futures = executeTasks(GET, params, nodes, null);

            int ok = 0;
            int notFound = 0;

            byte[] value = null;
            for (Future<Response> future : futures) {
                try {
                    Response resp = future.get();
                    if (resp.getCode() == OK) {
                        ok++;
                        value = resp.getData();
                    } else if (resp.getCode() == NOT_FOUND) {
                        notFound++;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (ok + notFound < ack) {
                return new Response(NOT_ENOUGH_REPLICAS);
            } else if (ok < ack) {
                return new Response(NOT_FOUND);
            } else {
                return new Response(OK, value);
            }

        } catch (IllegalArgumentException e) {
            return new Response(BAD_REQUEST, e.getMessage());
        } catch (NoSuchElementException e) {
            return new Response(NOT_FOUND, e.getMessage());
        }
    }

    private Response processEntityPut(@NotNull QueryParams params,
                                      @NotNull byte[] data) throws IOException {
        try {
            int ack = params.getAck();
            int from = params.getFrom();

            String id = params.getId();
            List<String> nodes = getNodesById(id, from);
            List<FutureTask<Response>> futures = executeTasks(PUT, params, nodes, data);

            int ok = 0;

            for (Future<Response> future : futures) {
                try {
                    Response resp = future.get();
                    if (resp.getCode() == CREATED) {
                        ok++;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (ok < ack) {
                return new Response(NOT_ENOUGH_REPLICAS);
            } else {
                return new Response(CREATED);
            }

        } catch (IllegalArgumentException e) {
            return new Response(BAD_REQUEST, e.getMessage());
        } catch (NoSuchElementException e) {
            return new Response(NOT_FOUND, e.getMessage());
        }
    }

    private Response processEntityDelete(@NotNull QueryParams params) throws IOException {
        try {
            int ack = params.getAck();
            int from = params.getFrom();

            String id = params.getId();
            List<String> nodes = getNodesById(id, from);
            List<FutureTask<Response>> futures = executeTasks(DELETE, params, nodes, null);

            int ok = 0;

            for (Future<Response> future : futures) {
                try {
                    Response resp = future.get();
                    if (resp.getCode() == ACCEPTED) {
                        ok++;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (ok < ack) {
                return new Response(NOT_ENOUGH_REPLICAS);
            } else {
                return new Response(ACCEPTED);
            }

        } catch (IllegalArgumentException e) {
            return new Response(BAD_REQUEST, e.getMessage());
        } catch (NoSuchElementException e) {
            return new Response(NOT_FOUND, e.getMessage());
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
            ack = topology.size() / 2 + 1;
            from = topology.size();
        }

        if (id == null || "".equals(id) || ack < 1 || from < 1 || ack > from) {
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

    private List<String> getNodesById(String id, int from) {
        List<String> nodes = new ArrayList<>();
        int hash = id.hashCode();
        hash = hash > 0 ? hash : -hash;
        for (int i = 0; i < from; i++) {
            int idx = (hash + i) % topology.size();
            nodes.add(topology.get(idx));
        }
        return nodes;
    }

    private Response makeRequest(@NotNull Method method,
                                 @NotNull String link,
                                 @NotNull String params,
                                 @Nullable byte[] data) throws IOException {
        try {
            URL url = new URL(link + params);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod(method.toString());
            conn.setUseCaches(false);
            conn.setDoInput(true);
            conn.setDoOutput(true);
            conn.setReadTimeout(5 * 1000);
            conn.connect();

            if (method == PUT) {
                conn.getOutputStream().write(data);
                conn.getOutputStream().flush();
                conn.getOutputStream().close();
            }

            int code = conn.getResponseCode();
            if (method == GET && code == OK) {
                InputStream dataStream = conn.getInputStream();
                byte[] inputData = readData(dataStream);
                conn.disconnect();
                return new Response(code, inputData);
            }
            return new Response(code);
        } catch (Exception e) {
            return new Response(500);
        }
    }

    private byte[] readData(@NotNull InputStream is) throws IOException {
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
                              @NotNull Response resp) throws IOException {
        if (resp.hasDate()) {
            http.sendResponseHeaders(resp.getCode(), resp.getData().length);
            http.getResponseBody().write(resp.getData());
        } else {
            http.sendResponseHeaders(resp.getCode(), 0);
        }
        http.close();
    }

    class QueryParams {

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

    enum Method {

        GET,
        PUT,
        DELETE

    }
}
