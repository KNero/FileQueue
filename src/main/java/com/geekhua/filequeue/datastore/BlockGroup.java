package com.geekhua.filequeue.datastore;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import org.apache.commons.lang.ArrayUtils;

import com.geekhua.filequeue.utils.EncryptUtils;
import com.geekhua.filequeue.utils.StreamUtils;

/**
 * @author Leo Liang
 * 
 */
public class BlockGroup 
{
	private static final byte[] HEADER = new byte[] { (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAB };
	private static final int CHECKSUMLEN = 20;

	private ByteBuffer data;
	private int blockSize;
	private byte[] content;
	private int blockCount;

	private BlockGroup(ByteBuffer _data, int _blockSize, int _blockCount) 
	{
		this.data = _data;
		this.blockSize = _blockSize;
		this.blockCount = _blockCount;
	}

	public byte[] getContent() 
	{
		return content;
	}

	public int getBlockCount()
	{
		return blockCount;
	}

	public int getBlockSize() 
	{
		return blockSize;
	}

	public static int estimateBlockSize(int _contentSize) 
	{
		return _contentSize + HEADER.length + 4 + CHECKSUMLEN;
	}

	/**
	 * content에 필요한 block 개수를 구한뒤 block 전체크기의 Buffer를 할당한 BlockGroup를 반환한다.
	 * @param _contentSize 저장할 content 크기
	 * @param _blockSize
	 * @return content를 저장할 BlockGroup
	 */
	public static BlockGroup allocate(int _contentSize, int _blockSize) 
	{
		int blockCount = _getBlockCount(_contentSize + CHECKSUMLEN, _blockSize);
		ByteBuffer data = ByteBuffer.allocate(blockCount * _blockSize);

		BlockGroup blockGroup = new BlockGroup(data, _blockSize, blockCount);
		if(data.remaining() >= 4) 
		{
			data.put(HEADER);
		}

		data.putInt(_contentSize + CHECKSUMLEN);

		return blockGroup;
	}

	/**
	 * content 크기를 block 크기로 쪼개서 저장하기 위해서 필요한 block의 개수를 구한다.<br>
	 * content 크기가 block 기로 나누어 떨어지지 않으면 하나를 추가한다. 
	 * @param _contentLength 저장한 content의 크기
	 * @param _blockSize block 하나의 크기
	 * @return
	 */
	private static int _getBlockCount(int _contentLength, int _blockSize) 
	{
		int dataLen = _contentLength + HEADER.length + 4;
		return dataLen / _blockSize + (dataLen % _blockSize == 0 ? 0 : 1);
	}

	public static BlockGroup read(RandomAccessFile _file, int _blockSize) throws IOException 
	{
		long markedPos = _file.getFilePointer();
		byte[] block = new byte[_blockSize];

		if(_file.length() - _file.getFilePointer() < _blockSize) 
		{
			return null;
		}

		StreamUtils.readFully(_file, block, 0, _blockSize);

		if(_validateHeader(block)) 
		{
			ByteBuffer blockBuffer = ByteBuffer.wrap(block);
			blockBuffer.position(HEADER.length);

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			baos.write(block);

			int contentAndChecksumLen = blockBuffer.getInt();
			//위에서 하나의 block을 읽었기 때문에 -1 해준다.
			int unreadBlockCount = _getBlockCount(contentAndChecksumLen, _blockSize) - 1;

			for(int i = 0; i < unreadBlockCount; i++) 
			{
				if(_file.length() - _file.getFilePointer() < _blockSize) 
				{
					_file.seek(markedPos);
					return null;
				}

				StreamUtils.readFully(_file, block, 0, _blockSize);
				
				baos.write(block);
			}

			byte[] data = baos.toByteArray();
			if(_validateChecksum(data, contentAndChecksumLen)) 
			{
				BlockGroup blockGroup = BlockGroup.allocate(contentAndChecksumLen - CHECKSUMLEN, _blockSize);
				
				byte[] content = new byte[contentAndChecksumLen - CHECKSUMLEN];
				System.arraycopy(data, HEADER.length + 4, content, 0, content.length);
				
				blockGroup.setContent(content);
				return blockGroup;
			}
		}
		
		_file.seek(markedPos + _blockSize);
		
		return read(_file, _blockSize);
	}

	private static boolean _validateHeader(byte[] _block) 
	{
		if(_block != null && _block.length >= HEADER.length) 
		{
			for(int i = 0; i < HEADER.length; i++) 
			{
				if(_block[i] != HEADER[i]) 
				{
					return false;
				}
			}

			return true;
		}

		return false;
	}

	private static boolean _validateChecksum(byte[] _block, int _contentAndChecksumLen) 
	{
		if(_block != null && _block.length >= _contentAndChecksumLen + HEADER.length + 4) 
		{
			byte[] content = new byte[_contentAndChecksumLen - CHECKSUMLEN];
			byte[] checksum = new byte[CHECKSUMLEN];

			System.arraycopy(_block, HEADER.length + 4, content, 0, content.length);
			System.arraycopy(_block, HEADER.length + 4 + content.length, checksum, 0, checksum.length);
			
			byte[] contentChecksum = EncryptUtils.sha1(content);
			if(contentChecksum.length == CHECKSUMLEN) 
			{
				if(ArrayUtils.isEquals(contentChecksum, checksum)) 
				{
					return true;
				}
			}
		}
		
		return false;
	}

	public void setContent(byte[] _content) 
	{
		if(_content != null && data.remaining() >= _content.length) 
		{
			this.data.put(_content);
			this.content = _content;
		}
	}

	public byte[] array() 
	{
		if(this.data.remaining() >= CHECKSUMLEN) 
		{
			this.data.put(EncryptUtils.sha1(this.content));
			return this.data.array();
		}

		return new byte[0];
	}
}
