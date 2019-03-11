package noveltoutiao;


import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/*
 * Created by anthony at 1/10/19, in project adv
 */
public class IOUtils {

	public static class ByteBundle {

		private List<byte[]> mList = new ArrayList<>();

		public ByteBundle() {

		}

		public ByteBundle(byte[] buf) throws IOException {
			add(buf);
		}

		public void add(byte... buf) throws IOException {
			synchronized (mList) {
				ntsadd(buf);
			}
		}

		protected void ntsadd(byte... buf) throws IOException {
			// NTS == non thread safe
			mList.add(buf);
		}

		public void add(InputStream is) throws IOException {
			// not really add a stream ... just read all data to a buffer then add the buffer
			synchronized (mList) {
				while (true) {
					byte[] buf = new byte[1024];
					int length = is.read(buf);
					if (length < buf.length) {
						buf = Arrays.copyOf(buf, length);
						ntsadd(buf);
						break;
					}
					ntsadd(buf);
				}
			}
		}

		public int length() {
			int totalLength = 0;
			for (byte[] bytes : mList) {
				totalLength += bytes.length;
			}
			return totalLength;
		}

		public void trim() {
			synchronized (mList) {
				if (mList.size() > 1) {
					byte[] buf = new byte[length()];
					int pos = 0;
					for (byte[] bytes : mList) {
						System.arraycopy(bytes, 0, buf, pos, bytes.length);
						pos += bytes.length;
					}
					mList.clear();
					mList.add(buf);
				}
			}
		}

		public byte[] toByteArray() {
			if (mList.size() > 1) {
				trim();
			}
			return mList.get(0);
		}
	}
}
