package com.github.pgsqlio.benchmarksql.jtpcc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.ArrayList;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

public class SkewRandomTest {

	@Test
	public void testSkewRandom() throws InterruptedException {
		long updateInterval = 100;
		SkewRandom rnd1 = new SkewRandom(100, 10, 50, 0.2, 0.2, updateInterval);
		SkewRandom rnd2 = new SkewRandom(100, 10, 50, 0.2, 0.2, updateInterval);
		assertEquals(100, rnd1.getDistricts().size());
		// assert the minium district number of warehouse greater or equal to 1
		assertEquals(true, rnd1.getDistricts().stream().mapToInt(i -> i).min().getAsInt() >= 1);
		assertEquals(100* 10, rnd1.getDistricts().stream().mapToInt(i -> i).sum());
		assertEquals(rnd1.getDistricts(), rnd2.getDistricts());

		ArrayList<Double> adjustedCDF = rnd1.getAdjustedCDF();

		Thread.sleep(updateInterval + 1);

		ArrayList<Double> adjustedCDF2 = rnd1.getAdjustedCDF();
		assertNotEquals(adjustedCDF, adjustedCDF2);

		// test skew 
		ArrayList<Integer> districts = rnd1.getDistricts();
		// sort districts descending
		districts.sort((a, b) -> b - a);
		// assert the first district are 2x greater than the last district
		assertEquals(true, districts.get(0) > 2 * districts.get(districts.size() - 1));

		// test uniform distribution
		rnd1 = new SkewRandom(100, 10, 50, 0.0, 0.0, updateInterval);
		districts = rnd1.getDistricts();
		for (int i = 0; i < districts.size(); i++) {
			assertEquals(10, districts.get(i));
		}
		ArrayList<Double> adjustedCDF3 = rnd1.getAdjustedCDF();
		for (int i = 0; i < adjustedCDF3.size(); i++) {
			assertEquals((i+1) / 100.0, adjustedCDF3.get(i));
		}
		for (int i = 0; i < 100 * 10; i++) {
			Pair<Long, Long> p = rnd1.getDistrictPosition(i);
			assertEquals(i / 10, p.getLeft().longValue() - 1);
			assertEquals(i % 10, p.getRight().longValue() - 1);
		}
	}
}
