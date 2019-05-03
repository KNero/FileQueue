package com.geekhua.filequeue;

import com.geekhua.filequeue.datastore.DataStore;
import com.geekhua.filequeue.datastore.DataStoreImpl;
import com.geekhua.filequeue.exception.FileQueueClosedException;
import com.geekhua.filequeue.meta.MetaHolder;
import com.geekhua.filequeue.meta.MetaHolderImpl;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class FileQueueImpl<E> implements FileQueue<E> {
	private DataStore<E> dataStore;
	private MetaHolder metaHolder;
	private volatile boolean isStopped = false;
	private final ReentrantLock writeLock = new ReentrantLock();
	private final ReentrantLock readLock = new ReentrantLock();

	public FileQueueImpl(Config config) throws IOException{
		if(config == null) {
			config = new Config();
		}

		this.metaHolder = new MetaHolderImpl(config.getName(), config.getBaseDir());
		this.metaHolder.init();

		this.dataStore = new DataStoreImpl<>(config, metaHolder.getReadingFileNo(), metaHolder.getReadingFileOffset());
		this.dataStore.init();
	}

	/**
	 * immediately get
	 */
	@Override
	public E get() throws InterruptedException, IOException {
		this.readLock.lockInterruptibly();

		try {
			E res = this.dataStore.take();
			if(res != null) {
				this.metaHolder.update(dataStore.readingFileNo(), dataStore.readingFileOffset());
			}

			return res;
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
					this.metaHolder.update(dataStore.readingFileNo(), dataStore.readingFileOffset());
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
	public void close() throws IOException {
		writeLock.lock();
		readLock.lock();
		
		try {
			this.isStopped = true;
			this.dataStore.close();
			this.metaHolder.close();
		} finally {
			writeLock.unlock();
			readLock.unlock();
		}
	}

	public long getReadingFileNo() {
		return metaHolder.getReadingFileNo();
	}

	public long getReadingFileOffset() {
		return metaHolder.getReadingFileOffset();
	}

	public long getWritingFileNo() {
		return dataStore.writingFileNo();
	}

	public long getWritingFileOffset() {
		return dataStore.writingFileOffset();
	}
}
