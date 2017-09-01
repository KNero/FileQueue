package com.geekhua.filequeue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.geekhua.filequeue.exception.FileQueueClosedException;

public interface FileQueue<E> 
{
	E poll() throws InterruptedException, IOException;
	
    E get() throws InterruptedException, IOException;

    E get(long timeout, TimeUnit timeUnit) throws InterruptedException, IOException;

    void add(E m) throws IOException, FileQueueClosedException;

    void close() throws IOException;

}
