package ru.mail.polis.vaddya;

import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.util.NoSuchElementException;

public class DAOImpl implements DAO {

    private final String dir;

    public DAOImpl(String dir) {
        this.dir = dir;
    }

    private File getFile(String filename) {
        if (filename.isEmpty()) {
            throw new IllegalArgumentException("ID is empty");
        }
        return new File(dir, filename);
    }

    @NotNull
    @Override
    public byte[] get(@NotNull String id) throws NoSuchElementException, IllegalArgumentException, IOException {
        File file = getFile(id);
        if (!file.exists()) {
            throw new NoSuchElementException("Invalid ID: " + id);
        }
        try (InputStream is = new FileInputStream(file)) {
            int size = (int) file.length();
            byte[] value = new byte[size];
            if (is.read(value) != size) {
                throw new IOException("Cannot read in one go");
            }
            return value;
        }
    }

    @Override
    public void upsert(@NotNull String id, @NotNull byte[] value) throws IllegalArgumentException, IOException {
        File file = getFile(id);
        try (OutputStream os = new FileOutputStream(file)) {
            os.write(value);
        }
    }

    @Override
    public void delete(@NotNull String id) throws IllegalArgumentException, IOException {
        getFile(id).delete();
    }

}