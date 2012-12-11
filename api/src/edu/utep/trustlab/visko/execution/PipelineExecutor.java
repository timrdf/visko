/*
Copyright (c) 2012, University of Texas at El Paso
All rights reserved.
Redistribution and use in source and binary forms, with or without modification, are permitted
provided that the following conditions are met:

    -Redistributions of source code must retain the above copyright notice, this list of conditions
     and the following disclaimer.
    -Redistributions in binary form must reproduce the above copyright notice, this list of conditions
     and the following disclaimer in the documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.*/

package edu.utep.trustlab.visko.execution;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.ArrayList;

import org.mindswap.exceptions.ExecutionException;
import org.mindswap.owl.OWLDataValue;
import org.mindswap.owl.OWLFactory;
import org.mindswap.owl.OWLKnowledgeBase;
import org.mindswap.owl.OWLValue;
import org.mindswap.owls.OWLSFactory;
import org.mindswap.owls.process.Process;
import org.mindswap.owls.process.execution.ProcessExecutionEngine;
import org.mindswap.owls.process.variable.Input;
import org.mindswap.owls.process.variable.Output;
import org.mindswap.owls.service.Service;
import org.mindswap.query.ValueMap;

import edu.utep.trustlab.contentManagement.ContentManager;
import edu.utep.trustlab.visko.execution.provenance.PMLNodesetLogger;
import edu.utep.trustlab.visko.execution.provenance.PMLQueryLogger;
import edu.utep.trustlab.visko.execution.provenance.PROVPipelineExecutionProvenanceLogger;
import edu.utep.trustlab.visko.execution.provenance.PipelineExecutionProvenanceLogger;
import edu.utep.trustlab.visko.util.OWLSParameterBinder;

/**
 * 
 */
public class PipelineExecutor implements Runnable {    

    private PipelineExecutorJob    job;
    private ProcessExecutionEngine exec;
    private Thread t;

    /*
     * These two loggers are used to log the provenance of the current job in PML-2.
     */
    private PMLNodesetLogger traceLogger;
    private PMLQueryLogger   queryLogger;
    /*
     * As new Jobs are provided with {@link #setJob(PipelineExecutorJob)}, the previous jobs
     * are stored in this collection.
     */
    private ArrayList<PMLNodesetLogger> nodesetLoggers; // TODO: comment this.

    /*
     * This logger is used to log the provenance of all Jobs using PROV-O.
     */
    private PipelineExecutionProvenanceLogger provLogger = null;

    // Use our own interrupt facility, since calling Thread.interrupt() leaves Jena
    // in a crappy unusable state.
    private boolean isScheduledForTermination;

    /**
     * 
     * @param pipelineJob a Job to execute.
     */
    public PipelineExecutor(PipelineExecutorJob pipelineJob) {
        super();
        
        job = pipelineJob;
                
        if(job.getProvenanceLogging()) {
            traceLogger = new PMLNodesetLogger();
            provLogger  = new PROVPipelineExecutionProvenanceLogger();
        }

        job.getJobStatus().setTotalServiceCount(job.getPipeline().size());
    }

    public PipelineExecutor() {

        nodesetLoggers = new ArrayList<PMLNodesetLogger>();
        
        //always keep query alive because jobs may or may not log provenance
        queryLogger = new PMLQueryLogger();
        
        exec = OWLSFactory.createExecutionEngine();    
        isScheduledForTermination = false;
    }
    
    /**
     * Because the PipelineExecutor is synchronous, calling this method signals that the previous
     * PipelineExecutorJob has completed and 'pipelineJob' is the next one to perform.
     * 
     * @param pipelineJob a new Job to execute (replacing the already-finished old Job).
     */
    public void setJob(PipelineExecutorJob pipelineJob) {
        
        job = pipelineJob;
        
        if( job.getProvenanceLogging() ) {
            traceLogger = new PMLNodesetLogger(); // TODO: why is this not done in #run(), right next to the array insertion?
        }
        
        job.getJobStatus().setTotalServiceCount(job.getPipeline().size());
    }
    
    public PipelineExecutorJob getJob(){
        return job;
    }
    
    public boolean isAlive(){
        return t.isAlive();
    }
    
    public void scheduleForTermination(){
        isScheduledForTermination = true;
    }
    
    public boolean isScheduledForTermination(){
        return isScheduledForTermination;
    }

    public void process(){ // TODO: How many methods can you have with these synonyms: process, run, executePipeline? (apparently, 3)
        if(job.getJobStatus().getPipelineState() ==  PipelineExecutorJobStatus.PipelineState.NEW){
            t = new Thread(this);
            t.setDaemon(true);
            t.start();
        }    
    }
    
    @Override
    public void run() {
        try {
            if( !job.getPipeline().hasInputData() ) {
                job.getJobStatus().setPipelineState(PipelineExecutorJobStatus.PipelineState.NODATA);
            }else if( job.getPipeline().isEmpty() ) {
                job.getJobStatus().setPipelineState(PipelineExecutorJobStatus.PipelineState.EMPTYPIPELINE);
            }else {
                executePipeline();
            
                if(job.getProvenanceLogging()) {
                    nodesetLoggers.add(traceLogger);
                }
                
                job.getJobStatus().setPipelineState(PipelineExecutorJobStatus.PipelineState.COMPLETE);
            }
        }catch(Exception e) {
            e.printStackTrace();
            job.getJobStatus().setPipelineState(PipelineExecutorJobStatus.PipelineState.ERROR);
        }
    }
   
    private void executePipeline() throws ExecutionException {     

    	if( job.getProvenanceLogging() ) {
    		if( provLogger == null ) {
    			provLogger = new PROVPipelineExecutionProvenanceLogger();
    		}
            provLogger.recordVisKoQuery(job.getPipeline().getParentPipelineSet().getQuery().toString());
    		provLogger.recordPipelineStart();
    	}

    	job.getJobStatus().setPipelineState(PipelineExecutorJobStatus.PipelineState.RUNNING);
    	String resultURL = job.getPipeline().getArtifactURL();

    	System.out.println(job.getJobStatus());

    	for( int i = 0; i < job.getPipeline().size(); i++) {

    		edu.utep.trustlab.visko.ontology.viskoService.Service viskoService = job.getPipeline().getService(i);

    		// Capture initial dataset
    		if( job.getProvenanceLogging() && i == 0 ) {
    			traceLogger.captureInitialDataset(resultURL, job.getPipeline().getService(i));
    			provLogger.recordInitialDataset(resultURL, job.getPipeline().getService(i));
    		}

    		job.getJobStatus().setCurrentService(viskoService.getOWLSService().getURI(), i);
    		System.out.println(job.getJobStatus());

    		resultURL = executeService(viskoService, resultURL, i);

    		if(isScheduledForTermination){
    			System.out.println("This thread's execution was interrupted and will quit!");
    			job.getJobStatus().setPipelineState(PipelineExecutorJobStatus.PipelineState.INTERRUPTED);
    			break;
    		}
    	}

        job.setFinalResultURL(resultURL);
        
    	if( job.getProvenanceLogging() ) {
            provLogger.recordPipelineEnd(job);
    	}
    }
    
    public String dumpProvenance() {

        queryLogger.setViskoQuery(job.getPipeline().getParentPipelineSet().getQuery().toString());

        for( PMLNodesetLogger traceLogger : nodesetLoggers ) {    
            // Add answer to query
            queryLogger.addAnswer(traceLogger.dumpTrace());        
        }
        for ( int i=0; i < nodesetLoggers.size(); i++ ) {
            //provLogger.
        }
        
        // Dump query
        String pmlQueryURI = queryLogger.dumpPMLQuery();
        
        // Set URIs on Job
        job.setPMLQueryURI(pmlQueryURI);
        
        // Hack to serialize PROV (need to coordinate with Nick on how get get ahold of the filename)
        try {
            String path = ContentManager.getProvenanceContentManager().getBaseURL()
                                         .replaceFirst("file:///", "/").replaceFirst("file://","/");
            FileOutputStream fos = new FileOutputStream(new File(path+"prov.ttl"), false);
            provLogger.finish(fos);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return pmlQueryURI;
    }
    
    /**
     * 
     * @param viskoService
     * @param inputDataURL
     * @param serviceIndex
     * @return
     * @throws ExecutionException
     */
    private String executeService(edu.utep.trustlab.visko.ontology.viskoService.Service viskoService, 
                                  String inputDataURL, int serviceIndex) throws ExecutionException {        
        
        OWLKnowledgeBase kb = OWLFactory.createKB();
        Service service = viskoService.getOWLSService().getIndividual();
        Process process = service.getProcess();

        ValueMap<Input, OWLValue> inputs = OWLSParameterBinder.buildInputValueMap(process, inputDataURL, job.getPipeline().getParameterBindings(), kb);
        
        String outputDataURL = null;
        
        if (inputs != null){        
            ValueMap<Output, OWLValue> outputs;
            
            if(job.isSimulated())
                outputDataURL = ServiceSimulator.exec();
            
            else{
                outputs = exec.execute(process, inputs, kb);
                OWLDataValue out = (OWLDataValue) outputs.getValue(process.getOutput());
                outputDataURL =  out.toString();
            }

            if(job.getProvenanceLogging()) {
                traceLogger.captureProcessingStep(viskoService, inputDataURL, outputDataURL, inputs);
                provLogger.recordServiceInvocation(viskoService, inputDataURL, outputDataURL, inputs);
            }
        }
        return outputDataURL;
    }
}
