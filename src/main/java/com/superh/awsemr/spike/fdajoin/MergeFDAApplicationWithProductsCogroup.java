package com.superh.awsemr.spike.fdajoin;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

import com.superh.awsemr.spike.avro.FDAApplication;
import com.superh.awsemr.spike.avro.FDAProduct;

/**
 * This DoFn takes an incoming Pair, where the Key of the Pair is the
 * FDA ApplicationNumber, the value of the pair is itself a Pair
 * consisting of a Collection of FDAApplications, and a Collection
 * of FDAProducts.
 * <p>
 * The Collection of FDAApplications has only a single member in the
 * collection due to the fact that there is a one-to-many relationship
 * between an FDAApplication and its associated FDAProducts.
 * <p>
 * The DoFn emits as a Key the FDA Application Number and as a value
 * the FDA Application, where the products have been merged into the
 * FDAApplication instance.
 * <p>
 * @author Bernard T. French
 *
 */
public class MergeFDAApplicationWithProductsCogroup extends DoFn<Pair<String, Pair<Collection<FDAApplication>, Collection<FDAProduct>>>, Pair<String, FDAApplication>> {

	private static final long serialVersionUID = 7359882975562538986L;

	@Override
	public void process(Pair<String, Pair<Collection<FDAApplication>, Collection<FDAProduct>>> fdaAppProdCollections,
			Emitter<Pair<String, FDAApplication>> emitter) {
		
		System.out.println("---- FDA MERGER ----");

		/*
		 * Get the application number and the FDAApplication and FDAProduct
		 * Collections.
		 */
		
		String fdaAppNumber = fdaAppProdCollections.first();
		Pair<Collection<FDAApplication>, Collection<FDAProduct>> appProductPair = fdaAppProdCollections.second();
		Collection<FDAApplication> apps = appProductPair.first();
		Collection<FDAProduct> products = appProductPair.second();
		
		/*
		 * Get the only FDAApplication instance within the Collection
		 */
		
		FDAApplication application = new ArrayList<FDAApplication>(apps).get(0);
		
		/*
		 * Merge the FDAProduct collection with the FDAApplication
		 */
		
		application.setProducts(new ArrayList<FDAProduct>(products));
		
		/*
		 * Emit the Application Number, FDAApplication pair.
		 */
		System.out.println("FDA APP NUMBER --> " + fdaAppNumber);
		emitter.emit(Pair.of(fdaAppNumber, application));
	}
}
