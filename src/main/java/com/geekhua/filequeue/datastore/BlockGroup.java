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
class BlockGroup
{
	private static final byte[] HEADER = new byte[] {(byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAB};
	private static final int CHECKSUM_LEN = 20;
	private static final int CONTENT_CHECKSUM_SIZE_LEN = 4;

	private ByteBuffer data;
	private int blockSize;
	private byte[] content;

	private BlockGroup(ByteBuffer data, int blockSize) {
		this.data = data;
		this.blockSize = blockSize;
	}
	
	byte[] getContent()
	{
		return content;
	}

	int getBlockSize()
	{
		return blockSize;
	}

	static int estimateBlockSize(int contentSize) {
		return HEADER.length + CHECKSUM_LEN + CONTENT_CHECKSUM_SIZE_LEN + contentSize;
	}

	/**
	 * content 에 필요한 block 개수를 구한뒤 block 전체크기의 Buffer 를 할당한 BlockGroup 를 반환한다.
	 * 데이터 하나의 구조 : HEADER(4) + Length(4 = data length + checksum length) + Data + Checksum
	 * @param content 저장할 content
	 * @param blockSize 평균 블록 사이즈
	 * @return content 를 저장할 BlockGroup
	 */
	static BlockGroup allocate(byte[] content, int blockSize) {
		if (blockSize <= HEADER.length + CHECKSUM_LEN) {
			blockSize = HEADER.length + blockSize + CHECKSUM_LEN;
		}

		int blockCount = getBlockCount(content.length + CHECKSUM_LEN, blockSize);

		ByteBuffer data = ByteBuffer.allocate(blockCount * blockSize);
		data.put(HEADER);
		data.putInt(content.length + CHECKSUM_LEN);

		BlockGroup blockGroup = new BlockGroup(data, blockSize);
		blockGroup.content = content;

		return blockGroup;
	}

	/**
	 * content 크기를 block 크기로 쪼개서 저장하기 위해서 필요한 block 의 개수를 구한다.<br>
	 * content 크기가 block 기로 나누어 떨어지지 않으면 하나를 추가한다. 
	 * @param contentAndChecksumLength content length + checksum length
	 * @param blockSize block 하나의 크기
	 * @return 전체 block 개수
	 */
	private static int getBlockCount(int contentAndChecksumLength, int blockSize) {
		int dataLen = HEADER.length + CONTENT_CHECKSUM_SIZE_LEN + contentAndChecksumLength;
		return dataLen / blockSize + (dataLen % blockSize == 0 ? 0 : 1);
	}

	static BlockGroup read(RandomAccessFile fileQue, int blockSize) throws IOException {
		long markedPos = fileQue.getFilePointer();
		if(fileQue.length() - markedPos < blockSize) {
			return null;
		}

        byte[] block = new byte[blockSize];
		StreamUtils.readFully(fileQue, block, 0, blockSize);

		if(validateHeader(block)) {
			ByteBuffer blockBuffer = ByteBuffer.wrap(block);
			blockBuffer.position(HEADER.length);

			ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
			outBuffer.write(block);

			int contentAndChecksumLen = blockBuffer.getInt();
			//HEADER 검사를 위해서 먼저 읽은 하나의 블록은 빼준다.
			int unreadBlockCount = getBlockCount(contentAndChecksumLen, blockSize) - 1;

			for(int i = 0; i < unreadBlockCount; i++) {
				long remain = fileQue.length() - fileQue.getFilePointer();
				if(remain > 0 && remain < blockSize) {
					return null;
				}

				StreamUtils.readFully(fileQue, block, 0, blockSize);
				outBuffer.write(block);
			}

			byte[] data = outBuffer.toByteArray();
			if(validateChecksum(data, contentAndChecksumLen)) {
				byte[] content = new byte[contentAndChecksumLen - CHECKSUM_LEN];
				System.arraycopy(data, HEADER.length + CONTENT_CHECKSUM_SIZE_LEN, content, 0, content.length);

				return BlockGroup.allocate(content, blockSize);
			} else {
				fileQue.seek(markedPos + blockSize);
			}
		} else {
			fileQue.seek(markedPos + HEADER.length);
		}
		
		return read(fileQue, blockSize);
	}

	/**
	 * HEADER 가 포함된 첫 번째 블록에 맨 앞에서 부터 HEADER 가 포함되어 있는지 검사한다.
	 */
	private static boolean validateHeader(byte[] block) {
		if(block != null && block.length >= HEADER.length) {
			for(int i = 0; i < HEADER.length; i++) {
				if(block[i] != HEADER[i]) {
					return false;
				}
			}

			return true;
		}

		return false;
	}

	private static boolean validateChecksum(byte[] block, int contentAndChecksumLen) {
		if(block != null && block.length >= contentAndChecksumLen + HEADER.length + CONTENT_CHECKSUM_SIZE_LEN) {
			byte[] content = new byte[contentAndChecksumLen - CHECKSUM_LEN];
			System.arraycopy(block, HEADER.length + CONTENT_CHECKSUM_SIZE_LEN, content, 0, content.length);

			byte[] contentChecksum = EncryptUtils.sha1(content);

			int blockOffset = HEADER.length + CONTENT_CHECKSUM_SIZE_LEN + content.length;
			if (block.length - blockOffset < contentChecksum.length) {
				return false;
			}

			for (int i = 0; i < contentChecksum.length; ++i) {
				if (contentChecksum[i] != block[i + blockOffset]) {
					return false;
				}
			}

			return true;
		}
		
		return false;
	}

	byte[] array() {
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
