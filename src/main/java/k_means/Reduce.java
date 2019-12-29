package k_means;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Cluster, Point, IntWritable, Point> {
	private HashMap<IntWritable, Cluster> newClusters = new HashMap<IntWritable, Cluster>();
	private HashMap<IntWritable, Cluster> oldClusters = new HashMap<IntWritable, Cluster>();
	private int iConvergedCenters = 0;

	public enum CONVERGE_COUNTER {
		CONVERGED
	}

	@Override
	protected void reduce(Cluster cluster, Iterable<Point> values, Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		Cluster oldCluster = cluster, newCluster = new Cluster(oldCluster.getIndex(), new IntWritable(0));
		boolean exsistingCluster = false;
		if (newClusters.containsKey(oldCluster.getIndex())) {
			exsistingCluster = true;
			newCluster = newClusters.get(cluster.getIndex());
		}
		Point newcenter = new Point(conf.getInt("iCoordinates", 2));
		int countValues = newCluster.getNumberOfPoints().get();
		Double temp;
		for (Point p : values) {
			for (int i = 0; i < p.getListOfCoordinates().size(); i++) {
				temp = newcenter.getListOfCoordinates().get(i).get() + p.getListOfCoordinates().get(i).get();
				newcenter.getListOfCoordinates().get(i).set(temp);
			}
			countValues++;
		}

		for (int i = 0; i < newcenter.getListOfCoordinates().size(); i++) {
			temp = newcenter.getListOfCoordinates().get(i).get() / countValues;
			newcenter.getListOfCoordinates().get(i).set(temp);
		}
		newCluster.setCenter(newcenter);
		if(!exsistingCluster) {
			newClusters.put(cluster.getIndex(), newCluster);
			oldClusters.put(oldCluster.getIndex(), oldCluster);
		}
		
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		Path centersPath = new Path(conf.get("centersFilePath"));
		SequenceFile.Writer centerWriter = SequenceFile.createWriter(conf, SequenceFile.Writer.file(centersPath),
				SequenceFile.Writer.keyClass(IntWritable.class), SequenceFile.Writer.valueClass(Point.class));
		Iterator<Cluster> it = newClusters.values().iterator();
		Cluster newClusterValue, sameOldC;
		Point oldcenter, newcenter;
		Double threshold = conf.getDouble("threshold", 0.5);
		Double avgValue = 0.0;
		int k = conf.getInt("k", 2);
		while (it.hasNext()) {
			newClusterValue = it.next();
			newcenter = newClusterValue.getCenter();
			sameOldC = oldClusters.get(newClusterValue.getIndex());
			oldcenter = sameOldC.getCenter();
			if (newcenter.isConverged(oldcenter, threshold))
				iConvergedCenters++;
			avgValue += Math.pow(Distance.findDistance(newcenter, oldcenter), 2);
			centerWriter.append(newClusterValue.getIndex(), newcenter);
		}
		avgValue = Math.sqrt(avgValue / k);
		int percentSize = (k * 90) / 100;
		if (iConvergedCenters >= percentSize || avgValue < threshold)
			context.getCounter(CONVERGE_COUNTER.CONVERGED).increment(1);
		System.out.println("iconverged centers : " + iConvergedCenters);
		centerWriter.close();
	}

	@SuppressWarnings("unused")
	private void printClusters() {
		for (IntWritable i : oldClusters.keySet()) {
			System.out.println(oldClusters.get(i));
		}
	}
}
