package edu.utep.trustlab.visko.execution.provenance;

import java.io.OutputStream;

import org.mindswap.owl.OWLValue;
import org.mindswap.owls.process.variable.Input;
import org.mindswap.query.ValueMap;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.sail.memory.MemoryStore;

import edu.rpi.tw.data.rdf.sesame.vocabulary.DCAT;
import edu.rpi.tw.data.rdf.sesame.vocabulary.DCTerms;
import edu.rpi.tw.data.rdf.sesame.vocabulary.HartigPROV;
import edu.rpi.tw.data.rdf.sesame.vocabulary.PML3;
import edu.rpi.tw.data.rdf.sesame.vocabulary.PROVO;
import edu.rpi.tw.data.rdf.sesame.vocabulary.RDF;
import edu.rpi.tw.data.rdf.utils.pipes.Constants;
import edu.rpi.tw.string.NameFactory;
import edu.utep.trustlab.contentManagement.ContentManager;
import edu.utep.trustlab.visko.execution.PipelineExecutorJob;
import edu.utep.trustlab.visko.ontology.model.ViskoModel;
import edu.utep.trustlab.visko.ontology.viskoOperator.Transformer;
import edu.utep.trustlab.visko.ontology.viskoService.Service;

/**
 * To log PipelineExecutor's provenance as PROV-O.
 */
public class PROVPipelineExecutionProvenanceLogger implements PipelineExecutionProvenanceLogger {

	private Repository           repo = null;
	private RepositoryConnection conn = null;
	private ValueFactory vf = ValueFactoryImpl.getInstance();
	private static final URI a = RDF.a;
	
	private String base = ContentManager.getProvenanceContentManager().getBaseURL(); // "http://example.org/visko/provenance/";
	
	// The VisKo query that could lead to one or more pipelines
	// (which each produce a visualization dataset)
	private URI queryR              = null;
	
	private URI pipelineR           = null;
	private int pipelineCount       = 0;
	
	// The Resources involved in the initial step of the current pipeline.
	private URI initialDatasetR     = null;
	private URI initialServiceR     = null;
	private URI initialServiceCallR = null;
	
	/**
	 * 
	 */
	public PROVPipelineExecutionProvenanceLogger() {
		this.repo = new SailRepository(new MemoryStore());
		try { 
			repo.initialize();
			this.conn = repo.getConnection();
		}catch(Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * c.f. PMLQueryLogger's setViskoQuery
	 */
	@Override
	public void recordVisKoQuery(String query) {
		String hash = NameFactory.getMD5(query);
		this.queryR = vf.createURI(base+"query/"+hash);
		try {
			conn.add(queryR, RDF.a,              PML3.Query);
			conn.add(queryR, RDF.a,              PROVO.Entity);
			conn.add(queryR, DCTerms.identifier, vf.createLiteral(hash));
			conn.add(queryR, PROVO.value,        vf.createLiteral(query));
		} catch (RepositoryException e) {
			e.printStackTrace();
		}
	}
	
	
	@Override
	public void recordPipelineStart() {
		pipelineCount++;
		System.err.println("START pipeline " + this.pipelineCount);
	}
	
	@Override
	public void recordPipelineEnd(PipelineExecutorJob job) {

		if( pipelineCount > 0 ) {
			System.err.println("END pipeline " + this.pipelineCount + " " + job);
			try {
				URI resultR = vf.createURI(job.getFinalResultURL());
				conn.add(queryR,  PML3.hasAnswer,     resultR);
				
				conn.add(resultR, RDF.a,              DCAT.Dataset);
				conn.add(resultR, RDF.a,              PROVO.Entity);
				conn.add(resultR, RDF.a,              PROVO.Entity);
			} catch (RepositoryException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * c.f. PMLNodesetLogger's captureInitialDataset
	 */
	@Override
	public void recordInitialDataset(String datasetURL, Service initialService) {
		this.initialDatasetR     = vf.createURI(datasetURL);
		this.initialServiceR     = vf.createURI(initialService.getURI());
		this.initialServiceCallR = vf.createURI(base+
												NameFactory.getMD5(datasetURL)+"/to/"+
												NameFactory.getMD5(initialService.getURI())+"/at/"+
												NameFactory.getMillisecond(""));
		try {
			conn.add(initialDatasetR, a, DCAT.Dataset);
			conn.add(initialDatasetR, a, PROVO.Entity);
			
			conn.add(initialServiceR, a, PROVO.SoftwareAgent);
			conn.add(initialServiceR, a, PROVO.Agent);
		} catch (RepositoryException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * c.f. PMLNodesetLogger's captureProcessingStep
	 */
	@Override
	public void recordServiceInvocation(Service service, 
										String inDatasetURL, String outDatasetURL, 
										ValueMap<Input, OWLValue> inputValueMap) {
		
		URI serviceR    = vf.createURI(service.getURI());
		URI inDatasetR  = vf.createURI(inDatasetURL);
		URI outDatasetR = vf.createURI(outDatasetURL);
		
		URI serviceCallR = vf.createURI(base+
				NameFactory.getMD5(inDatasetURL)+"/to/"+
				NameFactory.getMD5(service.getURI())+"/at/"+
				NameFactory.getMillisecond(""));

		try {			
			conn.add(inDatasetR, a, DCAT.Dataset);
			conn.add(inDatasetR, a, PROVO.Entity);
			
			conn.add(serviceR, a, PROVO.SoftwareAgent);
			conn.add(serviceR, a, PROVO.Agent);
			
			conn.add(serviceCallR, a,                       PROVO.Activity);
			conn.add(serviceCallR, a,                       HartigPROV.DataCreation);
			conn.add(serviceCallR, PROVO.wasAssociatedWith, serviceR);

			for( Input var : inputValueMap.getVariables() ) {
				OWLValue value = inputValueMap.getValue(var);
				String valueString = value.toString();
				URI attr = vf.createURI(serviceCallR.stringValue()+"/"+var.getLocalName());
				conn.add(serviceCallR,  PROVO.used, attr);
			}
			for( Input var : inputValueMap.getVariables() ) {
				OWLValue value = inputValueMap.getValue(var);
				String valueString = value.toString();
				
				URI attr = vf.createURI(serviceCallR.stringValue()+"/"+var.getLocalName());
				System.err.println("     "+var+"    " + valueString);
				conn.add(attr, a,                      PROVO.Entity);
				conn.add(attr, PROVO.specializationOf, vf.createURI(var.getURI().toString()));
				conn.add(attr, PROVO.value,            vf.createLiteral(valueString));
			}
			
			conn.add(outDatasetR, a,                     DCAT.Dataset);
			conn.add(outDatasetR, a,                     PROVO.Entity);
			conn.add(outDatasetR, PROVO.wasDerivedFrom,  inDatasetR);
			conn.add(outDatasetR, PROVO.wasAttributedTo, serviceR);
			conn.add(outDatasetR, DCTerms.format,        vf.createURI(
			   new Transformer(service.getConceptualOperator().getURI(), new ViskoModel())
										                                .getOutputFormat().getURI())
		    );
			
			conn.add(vf.createURI(PMLResourceURI.RULE_DIRECT_ASSERTION),         a, PROVO.Plan);
			conn.add(vf.createURI(PMLResourceURI.ENGINE_VISKO_PARAMETER_BINDER), a, PROVO.SoftwareAgent);
			
		}catch (RepositoryException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void finish(OutputStream out) {
		try {
			conn.add(initialServiceCallR, a,                PROVO.Activity);
			conn.add(initialServiceCallR, a,                HartigPROV.DataCreation);
			conn.add(initialServiceCallR, a,                HartigPROV.DataCreation);
			conn.add(initialServiceCallR, PROVO.used,              initialDatasetR);
			conn.add(initialServiceCallR, PROVO.wasAssociatedWith, initialServiceR);
			conn.add(initialServiceCallR, PROVO.wasInfluencedBy,   queryR);
			
			conn.setNamespace("dcterms",    DCTerms.BASE_URI);
			conn.setNamespace("dcat",       DCAT.BASE_URI);
			conn.setNamespace("prov",       PROVO.BASE_URI);
			conn.setNamespace("hartigprov", HartigPROV.BASE_URI);
			conn.setNamespace("pml",        PML3.BASE_URI);
			conn.setNamespace("",           base);
			conn.commit();
			
			conn.export(Constants.handlerForFileExtension("ttl",out));
		}catch( RepositoryException e) {
			e.printStackTrace();
		}catch( RDFHandlerException e) {
			e.printStackTrace();
		}
	}


}