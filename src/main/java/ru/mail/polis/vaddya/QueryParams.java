package ru.mail.polis.vaddya;

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
