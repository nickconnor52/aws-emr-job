package com.superh.awsemr.spike.fdajoin;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

import com.superh.awsemr.spike.avro.FDAApplication;

/**
 * This class represents a function that converts an FDA Application that
 * is represented by a row in tab-delimited file into an instance of
 * an Avro-based FDAApplication object.
 * <p>
 * @author Bernard T. French
 *
 */
public class ConvertFDAApplicationCsvToAvro extends DoFn<String, Pair<String, FDAApplication>> {

	private static final long serialVersionUID = -6017133543767533016L;

	@Override
	public void process(String appLine, Emitter<Pair<String, FDAApplication>> emitter) {
		
		/*
		 * This is the form of the application csvLine
		 * ApplNo  ApplType        ApplPublicNotes SponsorName
		 * 000159  NDA             LILLY
		 * 
		 * Extract the fields from the CSV String
		 */
		String[] appFields = appLine.split("\\|");
		String appNumber = appFields[0];
		String appType = appFields[1];
		String appNotes = appFields[2];
		String sponsor = appFields[3];
		
		/*
		 * Create the FDA Application Instance and Populate its fields
		 */
		
		FDAApplication application = new FDAApplication();
		application.setApplNo(appNumber);
		application.setApplType(appType);;
		application.setAppPublicNotes(appNotes);
		application.setSponsorName(sponsor);
		
		/*
		 * We are going to emit the application number. We will use the application number as the
		 * key to join information associated with this application.
		 */
		
		emitter.emit(Pair.of(appNumber, application));
		
	}
}
