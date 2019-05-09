package com.geekhua.filequeue;

import com.geekhua.filequeue.datastore.DataStore;
import com.geekhua.filequeue.datastore.DataStoreImpl;
import com.geekhua.filequeue.exception.FileQueueClosedException;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class FileQueueImpl<E> implements FileQueue<E> {
	private DataStore<E> dataStore;
	private volatile boolean isStopped = false;
	private final ReentrantLock writeLock = new ReentrantLock();
	private final ReentrantLock readLock = new ReentrantLock();

	public FileQueueImpl(Config config) throws IOException{
		if(config == null) {
			config = new Config();
		}

		dataStore = new DataStoreImpl<>(config);
		dataStore.init();
	}

	/**
	 * immediately get
	 */
	@Override
	public E get() throws InterruptedException, IOException {
		this.readLock.lockInterruptibly();

		try {
			return this.dataStore.take();
		} finally {
			this.readLock.unlock();
		}
	}

	@Override
	public E get(long timeout, TimeUnit unit) throws InterruptedException, IOException {
		long startNanos = System.nanoTime();
		long timeoutNanos = unit.toNanos(timeout);
		this.readLock.lockInterruptibly();
		
		try {
			while(!isStopped) {
				E res = this.dataStore.take();
				
				if(res != null) {
					return res;
				} else {
					// Since res == null not only caused by queue empty,
					// but also last msg not flush to disk completely. We
					// couldn't wait until not empty like
					// LinkedBlockingQueue.poll
					if(System.nanoTime() - startNanos < timeoutNanos) {
						TimeUnit.NANOSECONDS.sleep(100);
					} else {
						return null;
					}
				}
			}

			return null;
		} finally {
			this.readLock.unlock();
		}
	}

	@Override
	public void add(E m) throws IOException, FileQueueClosedException {
		this.writeLock.lock();

		try{
			if(this.isStopped) {
				throw new FileQueueClosedException();
			}

			this.dataStore.put(m);
		} finally {
			this.writeLock.unlock();
		}
	}

	@Override
	public void close() {
		writeLock.lock();
		readLock.lock();
		
		try {
			this.isStopped = true;
			this.dataStore.close();
		} finally {
			writeLock.unlock();
			readLock.unlock();
		}
	}

	public long getReadingFileNo() {
		return dataStore.readingFileNo();
	}

	public long getReadingFileOffset() {
		return dataStore.readingFileOffset();
	}

	public long getWritingFileNo() {
		return dataStore.writingFileNo();
	}

	public long getWritingFileOffset() {
		return dataStore.writingFileOffset();
	}
}
