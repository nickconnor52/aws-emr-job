package com.superh.awsemr.spike.fdajoin;

import java.util.Collection;

import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.superh.awsemr.spike.avro.FDAApplication;
import com.superh.awsemr.spike.avro.FDAProduct;

public class FDAJoinCrunchJob extends Configured implements Tool {
	
	public static final String FDA_APPLICATION_CSV_INPUT_KEY = "fda.application.csv.input";
	public static final String FDA_PRODUCT_CSV_INPUT_KEY = "fda.product.csv.input";
	public static final String FDA_APPLICATION_AVRO_OUTPUT_KEY = "fda.application.avro.output";

	private String fdaApplicationInputPath;
	private String fdaProductInputPath;
	private String fdaApplicationOutputPath;
	
	@Override
	public int run(String[] arg0) throws Exception {
		
		Configuration conf = getConf();
		fdaApplicationInputPath = conf.get(FDA_APPLICATION_CSV_INPUT_KEY);
		if (fdaApplicationInputPath == null) {
			throw new RuntimeException(FDA_APPLICATION_CSV_INPUT_KEY + " is a required argument.");
		}
		fdaProductInputPath = conf.get(FDA_PRODUCT_CSV_INPUT_KEY);
		if (fdaProductInputPath == null) {
			throw new RuntimeException(FDA_PRODUCT_CSV_INPUT_KEY + " is a required argument");
		}
		fdaApplicationOutputPath = conf.get(FDA_APPLICATION_AVRO_OUTPUT_KEY);
		if (fdaApplicationOutputPath == null) {
			throw new RuntimeException(FDA_APPLICATION_AVRO_OUTPUT_KEY + " is a required argument.");
		}
		
		/*
		 * Create the pipeline
		 */
		
		Pipeline pipeline = new MRPipeline(FDAJoinCrunchJob.class, conf);
		
		/*
		 * Read in the FDA Application CSV file and for each row convert it into an Avro-based
		 * instance of the FDA Application.
		 * 
		 * The DoFn CnvertFDAApplicationCsvToAvro will emit a Pair consisting of the
		 * application number as the Key and the FDA Application instance as the value.
		 * 
		 * We will join all information associated with a specific FDA Application through
		 * the applications application Number. The application number will be the key
		 * we execute a CoGroup on within the pipeline.
		 */
		
		PCollection<String> applications = pipeline.read(From.textFile(fdaApplicationInputPath));
		ConvertFDAApplicationCsvToAvro AppCsvToAvroConverter = new ConvertFDAApplicationCsvToAvro();
		PTable<String, FDAApplication> appNumberToAppTable = applications.parallelDo(AppCsvToAvroConverter, Avros.tableOf(Avros.strings(), Avros.records(FDAApplication.class)));
		
		/*
		 * Read in the FDA Product CSV file and for each row convert it into an Avro-based
		 * instance of the FDA Product.
		 * 
		 * The DoFn ConvertFDAProductCsvAvro will emit a Pair consisting of the application
		 * number as the Key and the FDA Product instance as the value.
		 * 
		 * We will use the application number as the CoGroup Key. This will allow us to join
		 * all of the products associated with a specific FDA Application. There is a
		 * one-to-many relationship between the application and its products.
		 * 
		 */
		
		PCollection<String> products = pipeline.read(From.textFile(fdaProductInputPath));
		ConvertFDAProductCsvToAvro  prodCsvToAvroConverter = new ConvertFDAProductCsvToAvro();
		PTable<String, FDAProduct> appNumberToProdTable = products.parallelDo(prodCsvToAvroConverter, Avros.tableOf(Avros.strings(), Avros.records(FDAProduct.class)));

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
		
		
		PTable<String, Pair<Collection<FDAApplication>, Collection<FDAProduct>>> appToProductCogroup = appNumberToAppTable.cogroup(appNumberToProdTable);
		
		/*
		 * Merge the FDAProduct Collection with the single FDAApplication
		 * with a DoFn.
		 */
		
		MergeFDAApplicationWithProductsCogroup productMerger = new MergeFDAApplicationWithProductsCogroup();
		PTable<String, FDAApplication> fdaApplication = appToProductCogroup.parallelDo(productMerger, Avros.tableOf(Avros.strings(), Avros.records(FDAApplication.class)));
		
		/*
		 * Write out the results
		 */
		
		pipeline.write(fdaApplication.values(), To.avroFile(fdaApplicationOutputPath));
				
		/*
		 * Check the result to see if the pipeline succeeded.
		 */
		
		PipelineResult result = pipeline.done();
		
		if (result.succeeded()) {
			return 0;
		} else {
			System.out.println("Pipeline failed... You did something wrong...");
			return 1;
		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new FDAJoinCrunchJob(), args);
		System.exit(exitCode);
	}
}
