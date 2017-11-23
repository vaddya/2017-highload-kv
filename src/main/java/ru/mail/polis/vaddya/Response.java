package ru.mail.polis.vaddya;

class Response {

    static final int OK = 200;
    static final int CREATED = 201;
    static final int ACCEPTED = 202;
    static final int BAD_REQUEST = 400;
    static final int NOT_FOUND = 404;
    static final int NOT_ALLOWED = 405;
    static final int NOT_ENOUGH_REPLICAS = 504;

    private final int code;
    private final byte[] data;

    Response(int code) {
        this.code = code;
        this.data = null;
    }

    Response(int code, String data) {
        this.code = code;
        this.data = data.getBytes();
    }

    Response(int code, byte[] data) {
        this.code = code;
        this.data = data;
    }

    int getCode() {
        return code;
    }

    boolean hasDate() {
        return data != null;
    }

    byte[] getData() {
        return data;
    }
}
