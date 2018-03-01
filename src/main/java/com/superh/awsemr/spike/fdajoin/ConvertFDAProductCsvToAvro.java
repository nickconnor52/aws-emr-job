package com.superh.awsemr.spike.fdajoin;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

import com.superh.awsemr.spike.avro.FDAProduct;

/**
 * This class represents a function that converts an FDA Product that
 * is represented by a row in tab-delimited file into an instance of
 * an Avro-based FDAProduct object.
 * <p>
 * @author Bernard T. French
 *
 */

public class ConvertFDAProductCsvToAvro extends DoFn<String, Pair<String, FDAProduct>> {

	private static final long serialVersionUID = -678168408413130298L;

	@Override
	public void process(String productLine, Emitter<Pair<String, FDAProduct>> emitter) {

		/*
		 * This is the form of the product CSV line
		 * 
		 * ApplNo  ProductNo       Form    Strength        ReferenceDrug   DrugName        ActiveIngredient        ReferenceStandard
		 * 000004  004     SOLUTION/DROPS;OPHTHALMIC       1%      0       PAREDRINE       HYDROXYAMPHETAMINE HYDROBROMIDE 0
		 *
		 * Extract the fields from the CSV String 
		 * 
		 */
		
		String[] productFields = productLine.split("\\|");
		String appNumber = productFields[0];
		String prodNumber = productFields[1];
		String form = productFields[2];
		String strength = productFields[3];
		String refDrug = productFields[4];
		String drugName = productFields[5];
		String activeIngredient = productFields[6];

		
		/*
		 * Create the FDA Product Instance and Populate it fields
		 */
		
		FDAProduct product = new FDAProduct();
		product.setApplNo(appNumber);
		product.setProductNo(prodNumber);
		product.setForm(form);
		product.setStrength(strength);
		product.setReferenceDrug(refDrug);
		product.setDrugName(drugName);
		product.setActiveIngredient(activeIngredient);
		
		
		/*
		 * We are going to emit the application number for this product as the Key. We will use this
		 * Key to join the product with its Application through the application number.
		 */
		
		emitter.emit(Pair.of(appNumber, product));
	}

}
