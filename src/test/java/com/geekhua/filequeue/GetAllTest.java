package com.geekhua.filequeue;

import java.util.concurrent.TimeUnit;

public class GetAllTest 
{
//	@Test
	public void getAll() throws Exception
	{
		Config config = new Config();
		config.setBaseDir("./fileque");
		config.setName("test");
		config.setMsgAvgLen(10);
		
		FileQueue<Object> que = new FileQueueImpl<Object>(config);
		
		Object result = que.get(100, TimeUnit.MILLISECONDS);
		while(result != null)
		{
			System.out.println(result.toString());
			
			result = que.get(100, TimeUnit.MILLISECONDS);
		}
	}
}
