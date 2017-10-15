package ru.mail.polis.vaddya;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.NoSuchElementException;

public class DAOImpl implements DAO {

    private final String dir;

    public DAOImpl(String dir) {
        this.dir = dir;
    }

    private Path getPath(String id) {
        if (id.isEmpty()) {
            throw new IllegalArgumentException("ID is empty");
        }
        return Paths.get(dir, id);
    }

    @NotNull
    @Override
    public byte[] get(@NotNull String id) throws NoSuchElementException, IllegalArgumentException, IOException {
        Path path = getPath(id);
        if (!Files.exists(path)) {
            throw new NoSuchElementException("Invalid ID: " + id);
        }
        return Files.readAllBytes(getPath(id));
    }

    @Override
    public void upsert(@NotNull String id, @NotNull byte[] value) throws IllegalArgumentException, IOException {
        Files.write(getPath(id), value);
    }

    @Override
    public void delete(@NotNull String id) throws IllegalArgumentException, IOException {
        Files.deleteIfExists(getPath(id));
    }

}
