package com.geekhua.filequeue.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class EncryptUtils 
{
	public static byte[] sha1(byte[] data) 
	{
		MessageDigest mDigest;
		
		try 
		{
			mDigest = MessageDigest.getInstance("SHA1");
			return mDigest.digest(data);
		}
		catch(NoSuchAlgorithmException e) 
		{
			return new byte[40];
		}
	}
}
