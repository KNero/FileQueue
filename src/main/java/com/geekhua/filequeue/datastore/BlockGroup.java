package com.geekhua.filequeue.datastore;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import com.geekhua.filequeue.utils.EncryptUtils;
import com.geekhua.filequeue.utils.StreamUtils;

/**
 * @author kwonsm
 * 
 */
public class BlockGroup 
{
	private static final byte[] HEADER = new byte[] {(byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAB};
	private static final int CHECKSUM_LEN = 20;
	private static final int CONTENT_CHECKSUM_SIZE_LEN = 4;

	private ByteBuffer data;
	private int blockSize;
	private byte[] content;
	private int blockCount;

	private BlockGroup(ByteBuffer data, int blockSize, int blockCount) {
		this.data = data;
		this.blockSize = blockSize;
		this.blockCount = blockCount;
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

	static int estimateBlockSize(int contentSize) {
		return HEADER.length + CONTENT_CHECKSUM_SIZE_LEN + contentSize + CHECKSUM_LEN;
	}

	/**
	 * content에 필요한 block 개수를 구한뒤 block 전체크기의 Buffer를 할당한 BlockGroup를 반환한다.
	 * 데이터 하나의 구조 : HEADER(4) + Length(4 = data length + checksum length) + Data + Checksum
	 * @param _content 저장할 content
	 * @param _blockSize
	 * @return content를 저장할 BlockGroup
	 */
	public static BlockGroup allocate(byte[] _content, int _blockSize) {
		if (_blockSize <= HEADER.length + CHECKSUM_LEN) {
			_blockSize = HEADER.length + _blockSize + CHECKSUM_LEN;
		}

		int blockCount = _getBlockCount(_content.length + CHECKSUM_LEN, _blockSize);

		ByteBuffer data = ByteBuffer.allocate(blockCount * _blockSize);
		data.put(HEADER);
		data.putInt(_content.length + CHECKSUM_LEN);

		BlockGroup blockGroup = new BlockGroup(data, _blockSize, blockCount);
		blockGroup.content = _content;

		return blockGroup;
	}

	/**
	 * content 크기를 block 크기로 쪼개서 저장하기 위해서 필요한 block의 개수를 구한다.<br>
	 * content 크기가 block 기로 나누어 떨어지지 않으면 하나를 추가한다. 
	 * @param _contentWithChecksumLength 저장한 content의 크기
	 * @param _blockSize block 하나의 크기
	 * @return
	 */
	private static int _getBlockCount(int _contentWithChecksumLength, int _blockSize) {
		int dataLen = HEADER.length + CONTENT_CHECKSUM_SIZE_LEN + _contentWithChecksumLength;
		return dataLen / _blockSize + (dataLen % _blockSize == 0 ? 0 : 1);
	}

	public static BlockGroup read(RandomAccessFile _file, int _blockSize) throws IOException {
		long markedPos = _file.getFilePointer();
		byte[] block = new byte[_blockSize];

		if(_file.length() - markedPos < _blockSize) {
			return null;
		}

		//HEADER가 포함된 첫 번째 들럭을 읽는다.
		StreamUtils.readFully(_file, block, 0, _blockSize);

		if(_validateHeader(block)) {
			ByteBuffer blockBuffer = ByteBuffer.wrap(block);
			blockBuffer.position(HEADER.length);

			ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
			outBuffer.write(block);

			int contentAndChecksumLen = blockBuffer.getInt();
			//HEADER 검사를 위해서 먼저 읽은 하나의 블록은 빼준다.
			int unreadBlockCount = _getBlockCount(contentAndChecksumLen, _blockSize) - 1;

			for(int i = 0; i < unreadBlockCount; i++) {
				long remain = _file.length() - _file.getFilePointer();
				if(remain > 0 && remain < _blockSize) {
					return null;
				}

				StreamUtils.readFully(_file, block, 0, _blockSize);
				outBuffer.write(block);
			}

			byte[] data = outBuffer.toByteArray();
			if(_validateChecksum(data, contentAndChecksumLen)) {
				byte[] content = new byte[contentAndChecksumLen - CHECKSUM_LEN];
				System.arraycopy(data, HEADER.length + CONTENT_CHECKSUM_SIZE_LEN, content, 0, content.length);

				return BlockGroup.allocate(content, _blockSize);
			} else {
				_file.seek(markedPos + _blockSize);
			}
		} else {
			_file.seek(markedPos + HEADER.length);
		}
		
		return read(_file, _blockSize);
	}

	/**
	 * HEADER가 포함된 첫 번째 블록에 맨 앞에서 부터 HEADER가 포함되어 있는지 검사한다.
	 * @param _block
	 * @return
	 */
	private static boolean _validateHeader(byte[] _block) {
		if(_block != null && _block.length >= HEADER.length) {
			for(int i = 0; i < HEADER.length; i++) {
				if(_block[i] != HEADER[i]) {
					return false;
				}
			}

			return true;
		}

		return false;
	}

	private static boolean _validateChecksum(byte[] _block, int _contentAndChecksumLen) {
		if(_block != null && _block.length >= _contentAndChecksumLen + HEADER.length + CONTENT_CHECKSUM_SIZE_LEN) {
			byte[] content = new byte[_contentAndChecksumLen - CHECKSUM_LEN];
			System.arraycopy(_block, HEADER.length + CONTENT_CHECKSUM_SIZE_LEN, content, 0, content.length);

			byte[] contentChecksum = EncryptUtils.sha1(content);

			int blockOffset = HEADER.length + CONTENT_CHECKSUM_SIZE_LEN + content.length;
			if (_block.length - blockOffset < contentChecksum.length) {
				return false;
			}

			for (int i = 0; i < contentChecksum.length; ++i) {
				if (contentChecksum[i] != _block[i + blockOffset]) {
					return false;
				}
			}

			return true;
		}
		
		return false;
	}

	public byte[] array() {
		if(this.content != null && data.remaining() >= this.content.length) {
			this.data.put(this.content);
		}
		
		if(this.data.remaining() >= CHECKSUM_LEN) {
			this.data.put(EncryptUtils.sha1(this.content));
			
			return this.data.array();
		}

		return new byte[0];
	}
}
