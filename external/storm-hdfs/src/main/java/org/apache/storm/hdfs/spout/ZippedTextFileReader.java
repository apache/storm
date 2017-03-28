/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.hdfs.spout;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.spout.AbstractFileReader;
import org.apache.storm.hdfs.spout.FileOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Todo: Track file offsets instead of line number
public class ZippedTextFileReader extends AbstractFileReader {
	public static final String[] defaultFields = { "line" };
	public static final String CHARSET = "hdfsspout.reader.charset";
	public static final String BUFFER_SIZE = "hdfsspout.reader.buffer.bytes";
	public static Map config = null;
	FSDataInputStream fsin = null;
	private static final int DEFAULT_BUFF_SIZE = 4096;
	private String zipEntryName; // Entry filename in current zipfile
	private String zipFilename;
	private BufferedReader reader;
	private ZipInputStream zip = null;
	private final Logger LOG = LoggerFactory.getLogger(ZippedTextFileReader.class);
	private ZippedTextFileReader.Offset offset;

	public ZippedTextFileReader(FileSystem fs, Path file, Map conf) throws IOException {
		this(fs, file, conf, new ZippedTextFileReader.Offset("",0, 0));
	}

	public ZippedTextFileReader(FileSystem fs, Path file, Map conf, String startOffset) throws IOException {
		this(fs, file, conf, new ZippedTextFileReader.Offset(startOffset));
	}

	protected ZippedTextFileReader(FileSystem fs, Path file, Map conf, ZippedTextFileReader.Offset startOffset)
			throws IOException {

		super(fs, file);
		offset = startOffset;
		fsin = fs.open(file);
		zip = new ZipInputStream(fsin);
		if (reader == null) {
			ZipEntry entry;
			// If there are no more zip entries it means end of zip.
			if ((entry = zip.getNextEntry()) == null) {
				return;
			}
			zipEntryName = entry.getName();
			String fileName = offset.fileName;
			if(StringUtils.isNotBlank(fileName)){
				while(!fileName.equals(zipEntryName)){
					entry = zip.getNextEntry();
					if(entry== null){
						throw new RuntimeException(fileName +" Not found in "+file);
					}
					zipEntryName = entry.getName();
				}
			} else {
				offset.fileName=zipEntryName;
			}
			LOG.info("zipEntryName: ============:" + zipEntryName);
			// Get current position for updating progress
			String charSet = conf == null || !conf.containsKey(CHARSET) ? "UTF-8" : conf.get(CHARSET).toString();
			int buffSz = conf == null || !conf.containsKey(BUFFER_SIZE) ? DEFAULT_BUFF_SIZE
					: Integer.parseInt(conf.get(BUFFER_SIZE).toString());
			reader = new BufferedReader(new InputStreamReader(zip, charSet), buffSz);
			if(offset.charOffset >0) {
				reader.skip(offset.charOffset);
			}
		}

	}

	public void initialize(Map conf,ZippedTextFileReader.Offset startOffset) throws IOException {
		offset = startOffset;
		String charSet = conf == null || !conf.containsKey(CHARSET) ? "UTF-8" : conf.get(CHARSET).toString();
		int buffSz = conf == null || !conf.containsKey(BUFFER_SIZE) ? DEFAULT_BUFF_SIZE
				: Integer.parseInt(conf.get(BUFFER_SIZE).toString());
		reader = new BufferedReader(new InputStreamReader(zip, charSet), buffSz);
		if(offset.charOffset >0) {
			reader.skip(offset.charOffset);
		}
	}

	@Override
	public Offset getFileOffset() {
		return offset.clone();
	}

	@Override
	public List<Object> next() throws IOException {
		// Read line from the file.
		String line = readLineAndTrackOffset(reader);
		if (StringUtils.isBlank(line)) {
			return null;
		}
		return Collections.singletonList((Object) line);
	}

	private StringBuffer readline() throws IOException{
		StringBuffer sb = new StringBuffer(1000);
		//		long before = offset.charOffset;
		int ch;
		while ((ch = reader.read()) != -1) {
			++offset.charOffset;
			if (ch == '\n') {
				++offset.lineNumber;
				reader.mark(1);
				if((ch = reader.read()) != -1){
					reader.reset();
				} else {
					offset.isLastLine=true;
				}
				return sb;
			} else if (ch != '\r') {
				sb.append((char) ch);
			}
		}
		return null;
	}

	private String readLineAndTrackOffset(BufferedReader reader) throws IOException {

		StringBuffer sb = readline();
		if (sb == null)
		{
			offset.isLastLine=true;
			// reached EOF, didnt read anything
			try{
				ZipEntry entry;
				if ((entry = zip.getNextEntry()) == null) {
					return null;
				}
				zipEntryName = entry.getName();
				LOG.info("zipEntryName: ============:" + zipEntryName);
				initialize(config,new ZippedTextFileReader.Offset(zipEntryName,0, 0));
				sb = readline();
				if (sb==null) {
					offset.isLastLine=true;
					return null;
				}
			}catch (IOException e) {
				LOG.warn("Ignoring error when iterating zipFile " + getFilePath(), e);
			}

		}
		return sb.toString();
	}

	@Override
	public void close() {
		try {
			reader.close();
		} catch (IOException e) {
			LOG.warn("Ignoring error when closing file " + getFilePath(), e);
		}
		if (zip != null) {
			try {
				zip.close();
			} catch (IOException e) {
				LOG.warn("Ignoring error when closing zipinputstream " + getFilePath(), e);
			}
		}
		if (fsin != null) {
			try {
				fsin.close();
			} catch (IOException e) {
				LOG.warn("Ignoring error when closing FSDataInputStream " + getFilePath(), e);
			}
		}
	}

	public static class Offset implements FileOffset {
		long charOffset;
		long lineNumber;
		String fileName;
		boolean isLastLine;
		public Offset(String fileName, long byteOffset, long lineNumber) {
			this.fileName=fileName;
			this.charOffset = byteOffset;
			this.lineNumber = lineNumber;
		}

		public Offset(String offset) {
			if(offset==null) {
				throw new IllegalArgumentException("offset cannot be null");
			}
			try {
				if(offset.equalsIgnoreCase("0")) {
					charOffset = 0;
					lineNumber = 0;
				} else {
					String[] parts = offset.split(":");
					fileName= parts[0].split("=")[1];
					charOffset = Long.parseLong(parts[1].split("=")[1]);
					lineNumber = Long.parseLong(parts[2].split("=")[1]);
					isLastLine = Boolean.parseBoolean(parts[3].split("=")[1]);
				}
			} catch (Exception e) {
				throw new IllegalArgumentException("'" + offset +
						"' cannot be interpreted. It is not in expected format for AvZippedTextFileReader." +
						" Format e.g.  {char=123:line=5:lineNumber=true}");
			}
		}


		@Override
		public String toString() {
			return '{' + "FileName="+fileName+
					":char=" + charOffset +
					":line=" + lineNumber +
					":lineNumber="+isLastLine+":}";
		}

		@Override
		public boolean isNextOffset(FileOffset rhs) {
			if (rhs instanceof Offset) {
				Offset other = (Offset) rhs;
				if(other.fileName.equals(fileName)){
					return  other.charOffset > charOffset &&
							other.lineNumber == lineNumber+1;
				} else if(other.fileName.compareTo(fileName)>0){
					if(isLastLine && other.lineNumber==1){
						return true;
					}
				}
			}
			return false;
		}

		@Override
		public int compareTo(FileOffset o) {
			Offset rhs = (Offset) o;
			if(fileName.compareTo(rhs.fileName)==0){
				if (lineNumber < rhs.lineNumber) {
					return -1;
				}
				if (lineNumber == rhs.lineNumber) {
					return 0;
				}
				return 1;
			} else if(fileName.compareTo(rhs.fileName)<0){
				return -1;
			}
			return 1;
		}

		/* (non-Javadoc)
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			Offset other = (Offset) obj;
			if (charOffset != other.charOffset) {
				return false;
			}
			if (fileName == null) {
				if (other.fileName != null) {
					return false;
				}
			} else if (!fileName.equals(other.fileName)) {
				return false;
			}
			if (isLastLine != other.isLastLine) {
				return false;
			}
			if (lineNumber != other.lineNumber) {
				return false;
			}
			return true;
		}

		/* (non-Javadoc)
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (int) (charOffset ^ charOffset >>> 32);
			result = prime * result + (fileName == null ? 0 : fileName.hashCode());
			result = prime * result + (isLastLine ? 1231 : 1237);
			result = prime * result + (int) (lineNumber ^ lineNumber >>> 32);
			return result;
		}

		@Override
		public Offset clone() {
			Offset offset = new Offset(fileName,charOffset, lineNumber);
			offset.isLastLine=isLastLine;
			return offset ;
		}
	} // class Offset
}
