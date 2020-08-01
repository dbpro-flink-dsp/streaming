package impro.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.checkpoint.*;
import java.util.Collections;
import java.util.List;


import impro.data.DataPoint;
import org.apache.commons.math3.distribution.NormalDistribution;


public class NormalDistributionFunction extends RichMapFunction<DataPoint<Long>, DataPoint<Double>> implements ListCheckpointed<Integer> {

	// State!
	private int currentStep;

	final private double mean; // mean
	final private double sd;   // standard deviation
	private NormalDistribution nd;


	public NormalDistributionFunction(double mean_var, double sd_var){
		this.mean = mean_var;
		this.sd = sd_var;
		nd = new NormalDistribution(this.mean, this.sd);
	}

	@Override
	public DataPoint<Double> map(DataPoint<Long> dataPoint) throws Exception {

		//System.out.println("NormalDistFunction: " + Thread.currentThread().getName());

		return dataPoint.withNewValue(nd.sample() + 1);
	}

	@Override
	public List<Integer> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
		return Collections.singletonList(currentStep);
	}

	@Override
	public void restoreState(List<Integer> state) throws Exception {
		currentStep = state.get(0);  //TODO: check, this work here because is only one value
	}

}
