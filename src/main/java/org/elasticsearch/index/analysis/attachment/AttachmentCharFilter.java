package org.elasticsearch.index.analysis.attachment;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.CharFilter;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugin.mapper.attachments.tika.TikaInstance;

public class AttachmentCharFilter extends CharFilter {
	StringReader in;

	public AttachmentCharFilter(Reader in) {
		super(in);

		char[] arr = new char[8*1024]; // 8K at a time
		StringBuffer buf = new StringBuffer();
		int numChars;
		
		try{
			while ((numChars = in.read(arr, 0, arr.length)) > 0) {
				buf.append(arr, 0, numChars);
			}
		}
		catch(IOException exception){throw new RuntimeException(exception);}

		
		
		XContentParser parser;
		
		try{
			String stringValue = buf.toString();
			
			if(stringValue.length() % 4 != 0){
				throw new RuntimeException("Please note that Base64-encoded strings need to be padded! This one is missing " + (4 - (stringValue.length() % 4)) + " equal-signs (%3D url encoded).");
			}
			
			parser = XContentType.JSON.xContent().createParser("{\"data\" : \"" + stringValue + "\"}");
			while(parser.nextToken() != XContentParser.Token.VALUE_STRING){ }
		}
		catch(IOException exception){throw new RuntimeException(exception);}

		try{
			this.in = new StringReader(TikaInstance.tika().parseToString(new ByteArrayInputStream(parser.binaryValue())));
		} catch (Throwable e) {
			// It could happen that Tika adds a System property `sun.font.fontmanager` which should not happen
			// TODO Remove when this will be fixed in Tika. See https://issues.apache.org/jira/browse/TIKA-1548
			System.clearProperty("sun.font.fontmanager");
			throw new RuntimeException(e);
		}
	}

	@Override
	public int read(char[] cbuf, int off, int len) throws IOException {
		final int charsRead = in.read(cbuf, off, len);
		//		if (charsRead > 0) {
		//			final int end = off + charsRead;
		//			while (off < end) {
		//				if (cbuf[off] == ' ')
		//					cbuf[off] = '_';
		//				off++;
		//			}
		//		}
		return charsRead;
	}

	@Override
	protected int correct(int currentOff) {
		return 0;
	}
}