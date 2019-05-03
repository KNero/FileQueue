package com.geekhua.filequeue;

import com.geekhua.filequeue.exception.FileQueueClosedException;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public interface FileQueue<E> {
    E get() throws InterruptedException, IOException;

    E get(long timeout, TimeUnit timeUnit) throws InterruptedException, IOException;

    void add(E m) throws IOException, FileQueueClosedException;

    void close() throws IOException;

}
