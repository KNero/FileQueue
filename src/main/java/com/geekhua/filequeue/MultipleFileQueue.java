package com.geekhua.filequeue;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.geekhua.filequeue.exception.FileQueueClosedException;

public class MultipleFileQueue<E> implements Closeable
{
	private List<FileQueue<E>> queueList;
	
	private Object readLock;
	private AtomicLong readCount;
	
	private Object writeLock;
	private AtomicLong writeCount;
	
	public MultipleFileQueue(int _size)
	{
		this(_size, null);
	}
	
	public MultipleFileQueue(int _size, Config _config)
	{
		if(_config == null)
		{
			_config = new Config();
		}
		
		this.readLock = new Object();
		this.readCount = new AtomicLong(0);
		
		this.writeLock = new Object();
		this.writeCount = new AtomicLong(0);
		
		this.queueList = new ArrayList<>(_size);
		
		for(int i = 0; i < _size; ++i)
		{
			Config conf = _config.clone();
			conf.setName(_config.getName() + "_" + i);
			
			FileQueue<E> que = new FileQueueImpl<E>(conf);
			this.queueList.add(que);
		}
	}
	
	public void add(E _e) throws IOException, FileQueueClosedException
	{
		int queIndex = (int)(this.writeCount.getAndIncrement() % this.queueList.size());
		
		this.add(queIndex, _e);
	}
	
	public void add(int _queIndex, E _e) throws IOException, FileQueueClosedException
	{
		FileQueue<E> queue = null;
		synchronized(this.writeLock)
		{
			queue = this.queueList.get(_queIndex);
		}
		
		queue.add(_e);
	}
	
	public E get() throws InterruptedException, IOException
	{
		int queIndex = (int)(this.readCount.getAndIncrement() % this.queueList.size());
		
		return this.get(queIndex);
	}
	
	public E get(int _queIndex) throws InterruptedException, IOException
	{
		FileQueue<E> queue = null;
		synchronized(this.readLock)
		{
			queue = this.queueList.get(_queIndex);
		}
		
		return queue.get();
	}
	
	public E get(long _timeout, TimeUnit _timeUnit) throws InterruptedException, IOException
	{
		int queIndex = (int)(this.readCount.getAndIncrement() % this.queueList.size());
		
		return this.get(queIndex, _timeout, _timeUnit);
	}
	
	public E get(int _queIndex, long _timeout, TimeUnit _timeUnit) throws InterruptedException, IOException
	{
		FileQueue<E> queue = null;
		synchronized(this.writeLock)
		{
			queue = this.queueList.get(_queIndex);
		}
		
		return queue.get(_timeout, _timeUnit);
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
