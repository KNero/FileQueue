package com.geekhua.filequeue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com.geekhua.filequeue.codec.ByteArrayCodec;

public class MultipleQueueTest 
{
	@Test
	public void addAndGetTest()
	{
		try
		{
			Config conf = new Config();
			conf.setBaseDir("./test");
			conf.setName("multi");
			conf.setCodec(new ByteArrayCodec());
			
			MultipleFileQueue<byte[]> q = new MultipleFileQueue<>(3, conf);
			
			for(int i = 0; i < 10; ++i)
			{
				q.add(("테스트 데이터" + i).getBytes());
			}
			
			for(int i = 0; i < 10; ++i)
			{
				byte[] buf = q.get();
				System.out.println(new String(buf));
			}
			
			q.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	@Test
	public void addAndGetMultiThreadTest()
	{
		try
		{
			int threadCnt = 10;
			final int testCnt = 100;
			final CountDownLatch cntDown = new CountDownLatch(threadCnt * 2);
			
			Config conf = new Config();
			conf.setBaseDir("./test");
			conf.setName("multi");
			conf.setCodec(new ByteArrayCodec());
			
			final MultipleFileQueue<byte[]> q = new MultipleFileQueue<>(3, conf);
			final AtomicInteger count = new AtomicInteger();
			
			for(int i = 0; i < threadCnt; ++i)
			{
				new Thread(){
					public void run() 
					{
						for(int i = 0; i < testCnt; ++i)
						{
							try
							{
								q.add(("테스트 데이터" + count.getAndIncrement()).getBytes());
								
								Thread.sleep(1);
							}
							catch(Exception e)
							{
								e.printStackTrace();
							}
						}
						
						cntDown.countDown();
					}
				}.start();
			}
			
			final Set<String> checkSet = Collections.synchronizedSet(new HashSet<String>());
			
			for(int i = 0; i < threadCnt; ++i)
			{
				new Thread(){
					public void run() 
					{
						for(int i = 0; i < testCnt; ++i)
						{
							try
							{
								String data = new String(q.get());
								
								synchronized(checkSet)
								{
									if(checkSet.contains(data))
									{
										System.err.println("data collistion.");
									}
									else
									{
										checkSet.add(data);
									}
								}
								
								Thread.sleep(1);
							}
							catch(Exception e)
							{
								e.printStackTrace();
							}
						}
						
						cntDown.countDown();
					}
				}.start();
			}
			
			cntDown.await();
			
			List<String> list = new ArrayList<>(checkSet);
			Collections.sort(list);
			System.out.println("size : " + list.size());
			System.out.println("index 0 : " + list.get(0));
			System.out.println("index 0 : " + list.get(list.size() - 1));
			
			q.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
			Assert.fail();
		}
	}
}
