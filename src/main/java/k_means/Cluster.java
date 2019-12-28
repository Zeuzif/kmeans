package k_means;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class Cluster implements WritableComparable<Cluster> {

	private IntWritable index;
	private IntWritable numberOfPoints;
	private Point center;

	public Cluster() {
		this.index = new IntWritable(0);
		this.numberOfPoints = new IntWritable(0);
		this.center = new Point();
	}

	public Cluster(List<DoubleWritable> list) {
		super();
		this.center = new Point(list);
		this.index = new IntWritable(0);
		this.numberOfPoints = new IntWritable(0);
	}

	public Cluster(IntWritable index, IntWritable numberOfPoints, Point center) {
		super();
		this.index = index;
		this.numberOfPoints = numberOfPoints;
		this.center = center;
	}

	public Cluster(IntWritable index, IntWritable numberOfPoints) {
		super();
		this.index = index;
		this.numberOfPoints = numberOfPoints;
	}

	public IntWritable getIndex() {
		return index;
	}

	public void setIndex(IntWritable index) {
		this.index = index;
	}

	public int compareTo(@Nonnull Cluster c) {
		return index.get() == c.getIndex().get() ? 0 : 1;
	}

	public IntWritable getNumberOfPoints() {
		return numberOfPoints;
	}

	public void setNumberOfPoints(IntWritable numberOfPoints) {
		this.numberOfPoints = numberOfPoints;
	}

	public Point getCenter() {
		return center;
	}

	public void setCenter(Point center) {
		this.center = center;
	}

	public void readFields(DataInput in) throws IOException {
		this.index = new IntWritable(in.readInt());
		this.numberOfPoints = new IntWritable(in.readInt());
		center.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(index.get());
		out.writeInt(numberOfPoints.get());
		center.write(out);
	}

	@Override
	public String toString() {
		return this.index + ";" + this.center.toString();
	}

}
