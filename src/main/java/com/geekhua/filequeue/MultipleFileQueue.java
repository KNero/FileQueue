package com.geekhua.filequeue;

import com.geekhua.filequeue.exception.FileQueueClosedException;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MultipleFileQueue<E> implements Closeable
{
	private List<FileQueue<E>> queueList;

	private AtomicInteger readCount;
	private AtomicInteger writeCount;
	
	public MultipleFileQueue(int _size)
	{
		this(_size, null);
	}
	
	public MultipleFileQueue(int _size, Config _config) {
		if (_config == null) {
			_config = new Config();
		}

		this.readCount = new AtomicInteger();
		this.writeCount = new AtomicInteger();
		this.queueList = new ArrayList<>(_size);

		for (int i = 0; i < _size; ++i) {
			Config conf = _config.clone();
			conf.setName(_config.getName() + "_" + i);

			FileQueue<E> que = new FileQueueImpl<E>(conf);
			this.queueList.add(que);
		}
	}
	
	public void add(E _e) throws IOException, FileQueueClosedException
	{
		int queIndex = Math.abs(this.writeCount.getAndIncrement() % this.queueList.size());

		FileQueue<E> queue = this._getQueue(queIndex);
		queue.add(_e);
	}

	public E poll() throws InterruptedException, IOException {
		int queSize = this.queueList.size();
		for (int i = 0; i < queSize; ++i) {
			int queIndex = Math.abs(this.readCount.getAndIncrement() % queSize);

			FileQueue<E> queue = this._getQueue(queIndex);
			E e = queue.poll();
			if (e != null) {
				return e;
			}
		}

		return null;
	}

	private FileQueue<E> _getQueue(int _index) {
		synchronized (this.queueList) {
			return this.queueList.get(_index);
		}
	}
	
	@Override
	public void close() throws IOException
	{
		for(FileQueue<E> fq : this.queueList)
		{
			fq.close();
		}
	}
}
