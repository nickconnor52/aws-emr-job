package com.superh.awsemr.spike.fdajoin.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.avro.Avros;
import org.junit.Before;
import org.junit.Test;

import com.superh.awsemr.spike.avro.FDAApplication;
import com.superh.awsemr.spike.avro.FDAProduct;
import com.superh.awsemr.spike.fdajoin.ConvertFDAApplicationCsvToAvro;
import com.superh.awsemr.spike.fdajoin.ConvertFDAProductCsvToAvro;
import com.superh.awsemr.spike.fdajoin.MergeFDAApplicationWithProductsCogroup;

/**
 * This Test verifies the CoGroup strategy used to merge FDA Products with their
 * associated FDA Application. Applications and Products are in separate files
 * and require merging in order to build out the complete view of an FDA
 * Application.
 * <p>
 * @author Bernard T. French
 *
 */
public class CogroupFDAApplicationWithProductsTest {

	private String fdaApplicationPath;
	private String fdaProductPath;
	
	@Before
	public void setUp() throws Exception {
		URL fdaApplicationUrl = CogroupFDAApplicationWithProductsTest.class.getResource("/FDAApplications.txt");
		URL fdaProductUrl = CogroupFDAApplicationWithProductsTest.class.getResource("/FDAProducts.txt");
		fdaApplicationPath = fdaApplicationUrl.getFile();
		fdaProductPath = fdaProductUrl.getFile();
	}

	@Test
	public void testFDAApplicationProductRecordJoin() {
		
		/*
		 * Create a pipeline and read in two sources, The FDAApplication and
		 * the FDAProduct file. These files are tab-delimited files where each
		 * row represents an application record or a product record.
		 * 
		 * There is a one-to-many relationship between an application record
		 * and the products.
		 */
		
		Pipeline pipeline = MemPipeline.getInstance();
		PCollection<String> fdaApplications = pipeline.readTextFile(fdaApplicationPath);
		PCollection<String> fdaProducts = pipeline.readTextFile(fdaProductPath);
		
		/*
		 * Convert a Row in the FDAApplication file into an Avro-based instance
		 * of the FDAApplication object. The Application Number is emitted
		 * as the Key within the DoFn conversion.
		 */
		
		ConvertFDAApplicationCsvToAvro appToAvro = new ConvertFDAApplicationCsvToAvro();
		PTable<String, FDAApplication> applicationTable = fdaApplications.parallelDo(appToAvro, Avros.tableOf(Avros.strings(), Avros.records(FDAApplication.class)));
		
		/*
		 * Convert a row in the FDAProduct file into an Avro-based instance
		 * of the FDAProduct object. The Application Number associated with
		 * the FDAProduct is emitted as the Key within the DoFn conversion.
		 */
		ConvertFDAProductCsvToAvro productToAvro = new ConvertFDAProductCsvToAvro();
		PTable<String, FDAProduct> productTable = fdaProducts.parallelDo(productToAvro, Avros.tableOf(Avros.strings(), Avros.records(FDAProduct.class)));
		
		/*
		 * Execute a CoGroup join on the applicationTable and the productTable
		 * The CoGroup results results in the Application Number as the Key and the
		 * Collection of FDAApplications associated with that Key and a Collection
		 * of FDAProducts associated with that Key.
		 * 
		 * The Application Number and FDAApplication is a unique pair, thus
		 * in the Collection of FDAApplications there is only one application, and
		 * this application is associated with the collection of products.
		 * 
		 * This is due to the fact that there is a one-to-many relationship
		 * between an FDAApplication and its associated FDAProducts.
		 */
		
		PTable<String, Pair<Collection<FDAApplication>, Collection<FDAProduct>>> appToProductCogroup = applicationTable.cogroup(productTable);
		
		/*
		 * Merge the FDAProduct Collection with the single FDAApplication
		 * with a DoFn.
		 */
		
		MergeFDAApplicationWithProductsCogroup productMerger = new MergeFDAApplicationWithProductsCogroup();
		PTable<String, FDAApplication> fdaApplication = appToProductCogroup.parallelDo(productMerger, Avros.tableOf(Avros.strings(), Avros.records(FDAApplication.class)));
		
		/*
		 * We're going to materialize the PTable so that we can perform our unit tests. We would not materialize
		 * the PTable when we're actually running the Hadoop Job. This makes it convenient to to test the
		 * values in the objects.
		 */
		
		Map<String, FDAApplication> appNumberToApplication = fdaApplication.materializeToMap();
		
		FDAApplication application;
		List<FDAProduct> products;
		
		/*
		 * Grab application with number 000004 and test its values.
		 */
		
		application = appNumberToApplication.get("000004");
		
		assertEquals("000004", application.getApplNo());
		assertEquals("NDA", application.getApplType());
		assertEquals("PHARMICS", application.getSponsorName());
		
		/*
		 * Grab the products associated with application number 000004
		 * We should have only one product associated with this application.
		 * Check the product values to make sure they are correct.
		 */
		
		products = application.getProducts();
		assertTrue(products.size() == 1);
		assertEquals("004", products.get(0).getProductNo());
		assertEquals("HYDROXYAMPHETAMINE HYDROBROMIDE", products.get(0).getActiveIngredient());
		
		/*
		 * Grab application with number 000734 and test its values.
		 */
		
		application = appNumberToApplication.get("000734");
		assertEquals("000734", application.getApplNo());
		assertEquals("NDA", application.getApplType());
		assertEquals("LILLY", application.getSponsorName());
		
		/*
		 * Grab the products associated with application number 000734.
		 * We should have 3 products associated with this application.
		 * Grab product with product number 002 and test its values.
		 */
		
		products = application.getProducts();
		assertTrue(products.size() == 3);
		for (FDAProduct product : products) {
			if (product.getProductNo().equals("001")) {
				assertEquals("HISTAMINE PHOSPHATE", product.getActiveIngredient());
				assertEquals("INJECTABLE;INJECTION", product.getForm());	
				assertEquals("EQ 1MG BASE/ML", product.getStrength());
			} else if (product.getProductNo().equals("002")) {
				assertEquals("HISTAMINE PHOSPHATE", product.getActiveIngredient());
				assertEquals("INJECTABLE;INJECTION", product.getForm());
				assertEquals("EQ 0.2MG BASE/ML", product.getStrength());
			} else if (product.getProductNo().equals("003")) {
				assertEquals("HISTAMINE PHOSPHATE", product.getActiveIngredient());
				assertEquals("INJECTABLE;INJECTION", product.getForm());
				assertEquals("EQ 0.1MG BASE/ML", product.getStrength());
			}
		}
		
	}

}
