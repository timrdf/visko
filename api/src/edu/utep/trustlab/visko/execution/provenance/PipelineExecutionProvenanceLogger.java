package edu.utep.trustlab.visko.execution.provenance;

import java.io.OutputStream;

import org.mindswap.owl.OWLValue;
import org.mindswap.owls.process.variable.Input;
import org.mindswap.query.ValueMap;

import edu.utep.trustlab.visko.ontology.viskoService.Service;

/**
 * 
 */
public interface PipelineExecutionProvenanceLogger {

	/**
	 * 
	 * @param query - the value of the VisKo query. 
	 *                e.g. VISUALIZE .. AS ... IN ... WHERE FORMAT = ... AND TYPE = ...
	 */
	public abstract void recordVisKoQuery(String query);

	/**
	 * 
	 * @param inDatasetURL
	 * @param ingestingService
	 */
	public abstract void recordInitialDataset(String datasetURL, Service initialService);
	
	/**
	 * 
	 * @param service
	 * @param inDatasetURL
	 * @param outDatasetURL
	 * @param inputValueMap
	 */
	public abstract void recordServiceInvocation(Service service, String inDatasetURL,
												 String outDatasetURL, 
												 ValueMap<Input, OWLValue> inputValueMap);
	
	/**
	 * Called when no more provenance is to be recorded.
	 */
	public abstract void finish(OutputStream out);
}