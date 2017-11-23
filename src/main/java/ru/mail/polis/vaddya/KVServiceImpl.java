package ru.mail.polis.vaddya;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.KVService;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLDecoder;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import static ru.mail.polis.vaddya.KVServiceImpl.HttpMethod.*;
import static ru.mail.polis.vaddya.Response.*;

public class KVServiceImpl implements KVService {

    private static final String URL_STATUS = "/v0/status";
    private static final String URL_INNER = "/v0/inner";
    private static final String URL_ENTITY = "/v0/entity";
    private static final String URL_SERVER = "http://localhost";

    private static final String QUERY_ID = "id";
    private static final String QUERY_REPLICAS = "replicas";

    private static final String METHOD_IS_NOT_ALLOWED = "Method is not allowed";

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
                         @NotNull Set<String> topology) throws IOException {
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

    private void processStatus(@NotNull HttpExchange http) throws IOException {
        sendResponse(http, new Response(OK));
    }

    private void processInner(@NotNull HttpExchange http) throws IOException {
        try {
            QueryParams params = parseQuery(http.getRequestURI().getQuery());

            Response resp;
            switch (HttpMethod.valueOf(http.getRequestMethod())) {
                case GET:
                    resp = processInnerGet(params);
                    break;
                case PUT:
                    final byte[] data = readData(http.getRequestBody());
                    resp = processInnerPut(params, data);
                    break;
                case DELETE:
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
                                     @NotNull byte[] data) {
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

    private void processEntity(@NotNull HttpExchange http) throws IOException {
        try {
            QueryParams params = parseQuery(http.getRequestURI().getQuery());

            Response resp;
            switch (HttpMethod.valueOf(http.getRequestMethod())) {
                case GET:
                    resp = processEntityGet(params);
                    break;
                case PUT:
                    final byte[] data = readData(http.getRequestBody());
                    resp = processEntityPut(params, data);
                    break;
                case DELETE:
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

    private Response processEntityGet(@NotNull QueryParams params) throws IOException {
        try {
            String id = params.getId();
            List<String> nodes = getNodesById(id, params.getFrom());
            List<FutureTask<Response>> futures = executeFutures(GET, params, nodes, null);

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
                    return new Response(SERVER_ERROR);
                }
            }

            boolean hasLocally = processInnerGet(params).getCode() == OK;
            if (ok > 0 && notFound == 1 && !hasLocally) { // we probably missed the insertion
                processInnerPut(params, value);
                notFound--;
                ok++;
            }

            if (ok + notFound < params.getAck()) {
                return new Response(NOT_ENOUGH_REPLICAS);
            } else if (ok < params.getAck()) {
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
                                      @Nullable byte[] data) throws IOException {
        try {
            List<String> nodes = getNodesById(params.getId(), params.getFrom());
            List<FutureTask<Response>> futures = executeFutures(PUT, params, nodes, data);

            int ok = 0;
            for (Future<Response> future : futures) {
                try {
                    Response resp = future.get();
                    if (resp.getCode() == CREATED) {
                        ok++;
                    }
                } catch (Exception e) {
                    return new Response(SERVER_ERROR);
                }
            }

            if (ok < params.getAck()) {
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
            List<String> nodes = getNodesById(params.getId(), params.getFrom());
            List<FutureTask<Response>> futures = executeFutures(DELETE, params, nodes, null);

            int ok = 0;
            for (Future<Response> future : futures) {
                try {
                    Response resp = future.get();
                    if (resp.getCode() == ACCEPTED) {
                        ok++;
                    }
                } catch (Exception e) {
                    return new Response(SERVER_ERROR);
                }
            }

            if (ok < params.getAck()) {
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

    private List<FutureTask<Response>> executeFutures(@NotNull HttpMethod method,
                                                      @NotNull QueryParams params,
                                                      @NotNull List<String> nodes,
                                                      @Nullable byte[] data) {
        List<FutureTask<Response>> futures = new ArrayList<>();
        String self = URL_SERVER + ":" + server.getAddress().getPort();
        for (String node : nodes) {
            FutureTask<Response> future;
            if (node.equals(self)) {
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
                            () -> makeRequest(method, node + URL_INNER, "?id=" + params.getId(), data));
                } else {
                    future = new FutureTask<>(
                            () -> makeRequest(method, node + URL_INNER, "?id=" + params.getId(), null));
                }
            }
            futures.add(future);
            executor.execute(future);
        }
        return futures;
    }

    private QueryParams parseQuery(@NotNull String query) {
        Map<String, String> params = parseParams(query);
        String id = params.get(QUERY_ID);
        int ack;
        int from;
        if (params.containsKey(QUERY_REPLICAS)) {
            String replicasParams[] = params.get(QUERY_REPLICAS).split("/");
            ack = Integer.valueOf(replicasParams[0]);
            from = Integer.valueOf(replicasParams[1]);
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

    private List<String> getNodesById(@NotNull String id, int from) {
        List<String> nodes = new ArrayList<>();
        int hash = id.hashCode();
        hash = hash > 0 ? hash : -hash;
        for (int i = 0; i < from; i++) {
            int idx = (hash + i) % topology.size();
            nodes.add(topology.get(idx));
        }
        return nodes;
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

    private Response makeRequest(@NotNull HttpMethod method,
                                 @NotNull String link,
                                 @NotNull String params,
                                 @Nullable byte[] data) {
        HttpURLConnection conn = null;
        try {
            URL url = new URL(link + params);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod(method.toString());
            conn.setDoInput(true);
            conn.setDoOutput(true);
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
                return new Response(code, inputData);
            }
            return new Response(code);
        } catch (IOException e) {
            return new Response(SERVER_ERROR);
        } finally {
            if (conn != null) conn.disconnect();
        }
    }

    private void sendResponse(@NotNull HttpExchange http,
                              @NotNull Response resp) throws IOException {
        if (resp.hasData()) {
            http.sendResponseHeaders(resp.getCode(), resp.getData().length);
            http.getResponseBody().write(resp.getData());
        } else {
            http.sendResponseHeaders(resp.getCode(), 0);
        }
        http.close();
    }

    enum HttpMethod {

        GET,
        PUT,
        DELETE

    }
}
