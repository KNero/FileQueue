package com.geekhua.filequeue.codec;

public class ByteArrayCodec implements Codec {
	@Override
	public byte[] encode(Object element) {
		return (byte[]) element;
	}

	@Override
	public Object decode(byte[] bytes) {
		return bytes;
	}
}
