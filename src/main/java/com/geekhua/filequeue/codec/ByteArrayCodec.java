package com.geekhua.filequeue.codec;

public class ByteArrayCodec implements Codec 
{
	@Override
	public byte[] encode(Object _element) 
	{
		return (byte[])_element;
	}

	@Override
	public Object decode(byte[] _bytes) 
	{
		return _bytes;
	}
}
