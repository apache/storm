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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

// Todo: Track file offsets instead of line number
public class ZippedTextFileReader extends AbstractFileReader {
  public static final String[] defaultFields = {"line"};
  public static final String CHARSET = "hdfsspout.reader.charset";
  public static final String BUFFER_SIZE = "hdfsspout.reader.buffer.bytes";

  private static final int DEFAULT_BUFF_SIZE = 4096;
  private long start;
  private long end;
  private LineReader in = null;
  private String zipEntryName; // Entry filename in current zipfile
  private String zipFilename;
  private BufferedReader reader;
  private ZipInputStream zip = null;
  private final Logger LOG = LoggerFactory.getLogger(ZippedTextFileReader.class);
  private ZippedTextFileReader.Offset offset;
  private Text value = new Text();  
  FSDataInputStream fsin = null;
  public ZippedTextFileReader(FileSystem fs, Path file, Map conf) throws IOException {
    this(fs, file, conf, new ZippedTextFileReader.Offset(0,0) );
  }

  public ZippedTextFileReader(FileSystem fs, Path file, Map conf, String startOffset) throws IOException {
    this(fs, file, conf, new ZippedTextFileReader.Offset(startOffset) );
  }

  private ZippedTextFileReader(FileSystem fs, Path file, Map conf, ZippedTextFileReader.Offset startOffset)
          throws IOException {
    super(fs, file);
    offset = startOffset;
    FSDataInputStream in = fs.open(file);

    String charSet = (conf==null || !conf.containsKey(CHARSET) ) ? "UTF-8" : conf.get(CHARSET).toString();
    int buffSz = (conf==null || !conf.containsKey(BUFFER_SIZE) ) ? DEFAULT_BUFF_SIZE : Integer.parseInt( conf.get(BUFFER_SIZE).toString() );
    reader = new BufferedReader(new InputStreamReader(in, charSet), buffSz);
    if(offset.charOffset >0) {
      reader.skip(offset.charOffset);
    }

  }
  public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {

		FileSplit split = (FileSplit) inputSplit;
		Configuration conf = context.getConfiguration();

		// Initialize start and end markers for the current split.
		start = split.getStart();
		end = start + split.getLength();

		final Path path = split.getPath(); // Filename of the current split.
		// key.set(path.getName()); // Set the current filename as key
		zipFilename = path.getName();

		LOG.info("zipFilename: ============:" + zipFilename);
		// Open current file for reading by wrapping it inside zip reader
		FileSystem fs = path.getFileSystem(conf);
		fsin = fs.open(path);
		zip = new ZipInputStream(fsin);
	}

  public Offset getFileOffset() {
    return offset.clone();
  }

  public List<Object> next() {
	boolean done = false;

	try {
		// Get current position for updating progress.
		while (!done) {
			// Initialize lineReader object if required.
			if (in == null) {
				ZipEntry entry;
				// If there are no more zip entries it means end of zip.
				if ((entry = zip.getNextEntry()) == null) {
					return null;
				} else {
					zipEntryName = entry.getName();
					LOG.info("zipEntryName: ============:" + zipEntryName);
				}
				in = new LineReader(zip);
			}
			// Read line from the file. if readLine() returns 0, it means end of file (zip entry).
			if (in.readLine(value) > 0) {
				done =true;
				return Collections.singletonList((Object) value);
			} else {
				in = null; // mark LineReader as null for moving to next zip entry.
			}
		}
	} catch (IOException e) {
		LOG.info("Corrupted Zip File. Skiping.");
		in = null;
		return null;
	}
	return null;
}


  @Override
  public void close() {
	 if (in != null) {
			in.close();
		}
		if (zip != null) {
			zip.close();
		}
		if (fsin != null) {
			fsin.close();
		}
  }

  public static class Offset implements FileOffset {
    long charOffset;
    long lineNumber;

    public Offset(long byteOffset, long lineNumber) {
      this.charOffset = byteOffset;
      this.lineNumber = lineNumber;
    }

    public Offset(String offset) {
      if(offset==null) {
        throw new IllegalArgumentException("offset cannot be null");
      }
      try {
        if(offset.equalsIgnoreCase("0")) {
          this.charOffset = 0;
          this.lineNumber = 0;
        } else {
          String[] parts = offset.split(":");
          this.charOffset = Long.parseLong(parts[0].split("=")[1]);
          this.lineNumber = Long.parseLong(parts[1].split("=")[1]);
        }
      } catch (Exception e) {
        throw new IllegalArgumentException("'" + offset +
                "' cannot be interpreted. It is not in expected format for TextFileReader." +
                " Format e.g.  {char=123:line=5}");
      }
    }

    @Override
    public String toString() {
      return '{' +
              "char=" + charOffset +
              ":line=" + lineNumber +
              ":}";
    }

    @Override
    public boolean isNextOffset(FileOffset rhs) {
      if(rhs instanceof Offset) {
        Offset other = ((Offset) rhs);
        return  other.charOffset > charOffset &&
                other.lineNumber == lineNumber+1;
      }
      return false;
    }

    @Override
    public int compareTo(FileOffset o) {
      Offset rhs = ((Offset)o);
      if(lineNumber < rhs.lineNumber) {
        return -1;
      }
      if(lineNumber == rhs.lineNumber) {
        return 0;
      }
      return 1;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) { return true; }
      if (!(o instanceof Offset)) { return false; }

      Offset that = (Offset) o;

      if (charOffset != that.charOffset)
        return false;
      return lineNumber == that.lineNumber;
    }

    @Override
    public int hashCode() {
      int result = (int) (charOffset ^ (charOffset >>> 32));
      result = 31 * result + (int) (lineNumber ^ (lineNumber >>> 32));
      return result;
    }

    @Override
    public Offset clone() {
      return new Offset(charOffset, lineNumber);
    }
  } //class Offset
}

