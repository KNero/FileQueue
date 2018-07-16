package com.geekhua.filequeue.datastore;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import com.geekhua.filequeue.utils.EncryptUtils;
import com.geekhua.filequeue.utils.StreamUtils;

/**
 * @author kwonsm
 * ----------------------------------------------------------
 * | header(4) | checksum(20) | content length(4) | content |
 * ----------------------------------------------------------
 * 
 */
class BlockGroup {
	private static final byte[] HEADER = new byte[] {(byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAB};
	private static final int CHECKSUM_SIZE = 20;
	private static final int CONTENT_SIZE_LENGTH = 4;
	// content 를 제외한 고정된 길이
	private static final int PRE_FIX_LENGTH = HEADER.length + CHECKSUM_SIZE + CONTENT_SIZE_LENGTH;

	private ByteBuffer data;
	private byte[] content;

	private BlockGroup(int blockSize, byte[] content) {
        int blockCount = getBlockCount(content.length, blockSize);

        this.data = ByteBuffer.allocate(blockCount * blockSize);
		this.content = content;
	}
	
	byte[] getContent() {
		return content;
	}

	static int estimateBlockGroupSize(int contentSize) {
		return PRE_FIX_LENGTH + contentSize;
	}

    /**
     * 하나의 데이터를 저장할 때 필요한 블록 개수를 구한다.
     * header(4) + checksum(20) + content length(4) + content
     * @param contentLength content length
     * @param blockSize block 하나의 크기
     * @return 전체 block 개수
     */
    private static int getBlockCount(int contentLength, int blockSize) {
        int dataLength = estimateBlockGroupSize(contentLength);
        return dataLength / blockSize + (dataLength % blockSize == 0 ? 0 : 1);
    }

	/**
	 * content 에 필요한 block 개수를 구한뒤 block 전체크기의 Buffer 를 할당한 BlockGroup 를 반환한다.
	 * 데이터 하나의 구조 : header(4) + checksum(20) + content length(4) + content
	 * @param content 저장할 content
	 * @param blockSize 평균 블록 사이즈
	 * @return content 를 저장할 BlockGroup
	 */
	static BlockGroup allocate(byte[] content, int blockSize) {
		if (blockSize <= PRE_FIX_LENGTH) {
			blockSize = PRE_FIX_LENGTH + blockSize;
		}

		return new BlockGroup(blockSize, content);
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

			byte[] checksum = new byte[CHECKSUM_SIZE];
			blockBuffer.get(checksum);
			int contentLength = blockBuffer.getInt();
			//HEADER 검사를 위해서 먼저 읽은 하나의 블록은 빼준다.
			int unreadBlockCount = getBlockCount(contentLength, blockSize) - 1;

			ByteArrayOutputStream buffer = new ByteArrayOutputStream();
			buffer.write(block);

			for(int i = 0; i < unreadBlockCount; i++) {
				long remain = fileQue.length() - fileQue.getFilePointer();
				if(remain > 0 && remain < blockSize) {
					return null;
				}

				StreamUtils.readFully(fileQue, block, 0, blockSize);
                buffer.write(block);
			}

			byte[] blockGroupData = buffer.toByteArray();
			byte[] content = new byte[contentLength];
			System.arraycopy(blockGroupData, PRE_FIX_LENGTH, content, 0, contentLength);

			if(validateChecksum(checksum, content)) {
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

	private static boolean validateChecksum(byte[] checksum, byte[] content) {
	    byte[] contentChecksum = EncryptUtils.sha1(content);
	    for (int i = 0; i < CHECKSUM_SIZE; ++i) {
	        if (checksum[i] != contentChecksum[i]) {
	            return false;
            }
        }

        return true;
	}

	byte[] array() {
	    data.put(HEADER);
	    data.put(EncryptUtils.sha1(content));
	    data.putInt(content.length);
	    data.put(content);
	    return data.array();
	}
}
