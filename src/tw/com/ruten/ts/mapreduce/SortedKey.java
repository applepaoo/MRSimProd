package tw.com.ruten.ts.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortedKey implements WritableComparable<SortedKey> {
	Text sortValue = new Text();
	Text defaultKey = new Text();

	SortedKey() {
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		defaultKey.set(Text.readString(in));
		sortValue.set(in.readLine());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, defaultKey.toString());
		out.writeBytes(sortValue.toString());
	}

	@Override
	public int compareTo(SortedKey other) { /// default
		return this.defaultKey.compareTo(other.defaultKey);
	}

	public int sort(SortedKey other) { /// for sort
		int r = this.defaultKey.compareTo(other.defaultKey);
		if (r == 0) {
			return this.sortValue.toString().compareTo(other.sortValue.toString());
		}

		return r;
	}

	public int group(SortedKey other) { /// for group
		return compareTo(other);
	}

	@Override
	public int hashCode() { /// for partition
		return this.defaultKey.toString().hashCode();
	}

	public static class SortComparator extends WritableComparator {
		SortComparator() {
			super(SortedKey.class, true);
		}

		@Override
		public int compare(WritableComparable o1, WritableComparable o2) {
			if (o1 instanceof SortedKey && o2 instanceof SortedKey) {
				SortedKey k1 = (SortedKey) o1;
				SortedKey k2 = (SortedKey) o2;

				return k1.sort(k2);
			}

			return o1.compareTo(o2);
		}
	}

	public static class GroupComparator extends WritableComparator {
		GroupComparator() {
			super(SortedKey.class, true);
		}

		@Override
		public int compare(WritableComparable o1, WritableComparable o2) {
			if (o1 instanceof SortedKey && o2 instanceof SortedKey) {
				SortedKey k1 = (SortedKey) o1;
				SortedKey k2 = (SortedKey) o2;

				return k1.group(k2);
			}

			return o1.compareTo(o2);
		}
	}
}

