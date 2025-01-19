package com.github.pgsqlio.benchmarksql.jtpcc;

import java.util.Random;
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.commons.lang3.tuple.Pair;

public class SkewRandom {
	private static Logger log = LogManager.getLogger(SkewRandom.class);

	private Random random;

	private long numWarehouses;
	// districts per warehouse
	private ArrayList<Integer> districts;
	// 0 based, districtPosition.get(i) is the warehouse id & district id of the
	// ith-district
	private ArrayList<Pair<Long, Long>> districtsPosition = new ArrayList<>();
	// alpha parameter for data skew pareto distribution
	private double alphaData;
	// alpha parameter for transaction skew pareto distribution
	private double alphaTxn;
	private long districtsPerWarehouse;
	private long updateInterval;
	private long ticker;
	// adjust weights for warehouse selection, based on the number of districts in
	// each warehouse and inbalance pareto distribution of warehouses
	private ArrayList<Double> adjustedCDF;

	public SkewRandom(long numWarehouses, long districtsPerWarehouse, long seed, double alphaData, double alphaTxn,
			long updateInterval) {
		this.numWarehouses = numWarehouses;
		this.random = new Random(seed);
		this.districts = new ArrayList<Integer>();
		this.alphaData = alphaData;
		this.alphaTxn = alphaTxn;
		this.districtsPerWarehouse = districtsPerWarehouse;
		this.adjustedCDF = new ArrayList<Double>();
		this.updateInterval = updateInterval;
		this.ticker = 0;

		generateSkewDistricts();
		generateAdjustedWeights();

		if (this.updateInterval != -1) {
			Thread updateThread = new Thread(new Runnable() {
				public void run() {
					while (true) {
						try {
							Thread.sleep(updateInterval);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						generateAdjustedWeights();
						ticker++;
					}
				}
			});
			updateThread.setDaemon(true);
			updateThread.start();
		}
	}

	// return a random number from a pareto distribution with the given alpha and
	// scale
	public static long nextPareto(double alpha, double scale, Random random) {
		if (alpha == 0) {
			return 1;
		}
		double u = random.nextDouble();
		return (long) (scale / Math.pow(u, 1.0 / alpha));
	}

	// generate a list of districts with a pareto distribution
	public void generateSkewDistricts() {
		ArrayList<Double> rawParetoValues = new ArrayList<>();
		for (int i = 0; i < numWarehouses; i++) {
			double paretoValue = nextPareto(alphaData, 1.0, random);
			rawParetoValues.add(paretoValue);
		}

		// normalize the pareto values to sum to total regions
		double sumParetoValues = rawParetoValues.stream().mapToDouble(Double::doubleValue).sum();

		long totalDistricts = numWarehouses * districtsPerWarehouse;
		for (double value : rawParetoValues) {
			int regionCount = (int) Math.floor(value / sumParetoValues * totalDistricts);
			districts.add(regionCount);
		}

		// set the minimum number of districts per warehouse to 1
		for (int i = 0; i < districts.size(); i++) {
			if (districts.get(i) == 0) {
				districts.set(i, 1);
			}
		}

		// adjust the total number of districts to match the total number of districts
		long currentTotal = districts.stream().mapToInt(Integer::intValue).sum();
		long difference = totalDistricts - currentTotal;

		if (difference > 0) {
			// assign the remaining districts to random warehouses
			for (int i = 0; i < difference; i++) {
				int index = random.nextInt((int) numWarehouses);
				districts.set(index, districts.get(index) + 1);
			}
		} else if (difference < 0) {
			// remove the extra districts from random warehouses
			difference = -difference;
			while (difference > 0) {
				int index = random.nextInt((int) numWarehouses);
				if (districts.get(index) > 1) {
					districts.set(index, districts.get(index) - 1);
					difference--;
				}
			}
		}

		for (int i = 0; i < districts.size(); i++) {
			for (int j = 0; j < districts.get(i); j++) {
				districtsPosition.add(Pair.of((long) i + 1, (long) j + 1));
			}
		}
	}

	public ArrayList<Integer> getDistricts() {
		return districts;
	}

	public long getDistrictCount(int w_id) {
		return districts.get(w_id - 1);
	}

	// return the warehouse id and district id of the given district index
	public Pair<Long, Long> getDistrictPosition(long index) {
		return districtsPosition.get((int) index);
	}

	// return a random district from the given warehouse
	public int nextDistrict(int w_id) {
		return random.nextInt((int) districts.get(w_id - 1)) + 1;
	}

	// return a number statisfying guassian distribution in [start, end]
	public int nextGaussian(double psigma, int start, int end) {
		double sigma = psigma * (end - start + 1) / 2.0;
		int retval;
		do {
			retval = (int) (random.nextGaussian() * sigma + (start + end + 1) / 2.0);
		} while (retval < start || retval > end);
		return retval;
	}

	// generate a list of adjusted weights for warehouse selection
	public void generateAdjustedWeights() {
		// the adjusted weight for each warehouse is the number of districts in the
		// warehouse multiplied by the pareto distribution weight
		ArrayList<Double> adjustedWeights = new ArrayList<>();
		for (int i = 0; i < numWarehouses; i++) {
			double paretoValue = nextPareto(alphaTxn, 1.0, random);
			adjustedWeights.add(paretoValue * districts.get(i));
		}

		ArrayList<Double> newAdjustedCDF = new ArrayList<>();
		double sum = 0;
		for (double weight : adjustedWeights) {
			sum += weight;
			newAdjustedCDF.add(sum);
		}
		for (int i = 0; i < newAdjustedCDF.size(); i++) {
			newAdjustedCDF.set(i, newAdjustedCDF.get(i) / sum);
		}
		this.adjustedCDF = newAdjustedCDF;
	}

	public ArrayList<Double> getAdjustedCDF() {
		return adjustedCDF;
	}

	// return a random warehouse id according to the adjusted weights
	public int nextParetoWarehouse() {
		double r = random.nextDouble();
		// choose a warehouse based on the adjusted weights using binary search
		int low = 0;
		int high = adjustedCDF.size() - 1;
		while (low < high) {
			int mid = (low + high) / 2;
			if (adjustedCDF.get(mid) < r) {
				low = mid + 1;
			} else {
				high = mid;
			}
		}
		return low + 1;
	}

}
