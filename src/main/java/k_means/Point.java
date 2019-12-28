package k_means;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

public class Point implements WritableComparable<Point> {
	private List<DoubleWritable> listOfCoordinates;

	Point(List<DoubleWritable> listOfCoordinates) {
		this.listOfCoordinates = new ArrayList<DoubleWritable>();
		for (DoubleWritable p : listOfCoordinates) {
			this.listOfCoordinates.add(p);
		}
	}

	Point() {
		listOfCoordinates = new ArrayList<DoubleWritable>();
	}

	Point(int n) {
		listOfCoordinates = new ArrayList<DoubleWritable>();
		for (int i = 0; i < n; i++)
			listOfCoordinates.add(new DoubleWritable(0.0));
	}

	public void readFields(DataInput dataInput) throws IOException {
		int iParams = dataInput.readInt();
		listOfCoordinates = new ArrayList<DoubleWritable>();
		for (int i = 0; i < iParams; i++) {
			listOfCoordinates.add(new DoubleWritable(dataInput.readDouble()));
		}
	}

	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeInt(listOfCoordinates.size());
		for (DoubleWritable p : listOfCoordinates) {
			dataOutput.writeDouble(p.get());
		}
	}

	public List<DoubleWritable> getListOfCoordinates() {
		return listOfCoordinates;
	}

	public int compareTo(Point o) {
		return 0;
	}

	public boolean isConverged(Point p, Double threshold) {
		return threshold > Distance.findDistance(this, p);
	}
	
	public String toString() {
        String elements = "";
        for (DoubleWritable e : listOfCoordinates) {
            elements += e.get() + ";";
        }
        return elements;
    }
}
