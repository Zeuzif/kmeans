package k_means;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<Object, Text, Cluster, Point> {
	private Vector<Cluster> clusters = new Vector<Cluster>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		Path centersPath = new Path(conf.get("centersFilePath"));
		SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(centersPath));
		IntWritable key = new IntWritable();
		Point value = new Point();
		while (reader.next(key, value)) {
			Cluster c = new Cluster(value.getListOfCoordinates());
			c.setIndex(key);
			clusters.add(c);
			key = new IntWritable(0);
			value = new Point();
		}
		reader.close();
	}

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		List<DoubleWritable> coordinates = new ArrayList<DoubleWritable>();
		StringTokenizer tokenizer = new StringTokenizer(line, ";");
		while (tokenizer.hasMoreTokens()) {
			coordinates.add(new DoubleWritable(Double.parseDouble(tokenizer.nextToken())));
		}
		Point p = new Point(coordinates);
		Cluster minDistanceCluster = null;
		Double minDistance = Double.MAX_VALUE;
		for (Cluster c : clusters) {
			Double distanceTemp = Distance.findDistance(p, c.getCenter());
			if (minDistance > distanceTemp) {
				minDistanceCluster = c;
				minDistance = distanceTemp;
			}
		}
		context.write(minDistanceCluster, p);
	}
}
