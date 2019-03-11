package com.cpc.spark.novel;

/*
 * Created by anthony at 1/10/19, in project adv
 */

/*
 * This file contains the name of "GNU" but it's not rules under GPL license LOL
 */

import noveltoutiao.IOUtils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GnuZip {

	//
	//	public static class GzipByteBundle: gzip compresser
	//
	public static class GzipByteBundle extends IOUtils.ByteBundle {

		public GzipByteBundle(byte[] buf) throws IOException {
			super(buf);
		}

		@Override
		public void add(byte... buf) throws IOException {
			byte[] gzipbuf = buf;
			if (gzipbuf != null && !isGzip(gzipbuf)) {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				GZIPOutputStream gos = new GZIPOutputStream(baos);
				gos.write(gzipbuf);
				gos.flush();
				gos.close();
				gzipbuf = baos.toByteArray();
			}
			super.add(gzipbuf);
		}
	}

	//
	//	public static class GzipByteBundle: gzip decompresser
	//
	public static class GunzipByteBundle extends IOUtils.ByteBundle {

		public GunzipByteBundle(byte[] buf) throws IOException {
			super(buf);
		}

		@Override
		public void add(byte... buf) throws IOException {
			if (isGzip(buf)) {
				ByteArrayInputStream bais = new ByteArrayInputStream(buf);
				GZIPInputStream gis = new GZIPInputStream(bais);
				super.add(gis);
				gis.close();
			}
		}
	}

	public static boolean isGzip(byte... buf) {
		return (buf != null && buf.length > 2	// length at least longer than magic number
				&& (((int) buf[1] << 8 | (int) buf[0]) & 0xffff) == 0x8b1f);	// the first two byte was '\x1f' and '\x8b'
	}

	public static boolean isGzip(InputStream is) throws IOException {
		// WARNING: the function will read from input stream -- that may causes your seek point right shift!
		byte[] buf = new byte[2];
		return is.read(buf) > 1 && isGzip(buf[0], buf[1], (byte)0);
	}
}
