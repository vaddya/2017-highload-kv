package ru.mail.polis;

import java.io.File;
import java.io.IOException;

/**
 * Starts storage and waits for shutdown
 *
 * @author Vadim Tsesko <mail@incubos.org>
 */
public final class Server {
    private static final int PORT = 8080;

    private Server() {
        // Not instantiable
    }

    public static void main(String[] args) throws IOException {
        // Temporary storage in the file system
        final File data = Files.createTempDirectory();

        // Start the storage
        final KVService storage = KVServiceFactory.create(PORT, data);
        storage.start();
        Runtime.getRuntime().addShutdownHook(new Thread(storage::stop));
    }
}
