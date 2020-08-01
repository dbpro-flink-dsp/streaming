/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package impro;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

/**
 *
 * @author Ariane Ziehn, Marcela Charfuelan
 *
 * In this example a set of transactions are studied in batch mode.
 * the tasks are:
 *  - Find the invoice with the biggest number of items
 *  - Find the 5 items more sold per country
 *
 * Run with:
 *    --input ./src/main/resources/OnlineRetail-short.csv
 *
 *
 */
public class BatchJob {

	public static void main(String[] args) throws Exception {

		//final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the batch execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		/**
		 * Read and clean the data
		 * 1. add path of the data
		 */
		//String dataPath =  params.get("input");
		String dataPath = "./src/main/resources/OnlineRetail-short.csv";
		/*
		 * 2. Check how your data is organized:
		 * InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country
		 * we have 8 cloumns .includeFields("00000000")
		 * the seperator is "," .fieldDelimiter(",")
		 * which columns are interesting?
		 * ATTENTION: in this data certain rows contains missing values, that is why we need to filter
		 */
		DataSet<Tuple4<String, String, Integer, String>> csvInput = env
				.readCsvFile(dataPath).fieldDelimiter(",")
				.includeFields("11010001").ignoreFirstLine()
				.types(String.class, String.class, Integer.class, String.class)
				.filter(new ReadInFilter());

		csvInput.print();
		System.out.println("csvInput.count=" + csvInput.count());

		/*
		 * 3. Find the invoice with the biggest number of items
		 * We have created a Flink dataset from the input OnlineRetail with the
		 * following information:
		 *     Tuple4<InvoiceNo, StockCode, Quantity, Country>
		 * In the next step we create a List of transactions, with the format:
		 *     Tuple3<InvoiceNo, Number of items in this invoice, List of items in this invoice>
		 */
		DataSet<Tuple3<String, Integer, String>> transactionList = csvInput
				.groupBy(0)
				.reduceGroup(new GroupItemsPerBill());

		transactionList.print();
		// Print the max
		transactionList.maxBy(1).print();
		System.out.println("\n****** Number of transactions:" +  transactionList.count());


		/**
		 * 4. - Find the 5 items more sold per country
		 *  csvInput contains:
		 *  Tuple4<InvoiceNo, StockCode, Quantity, Country>
		 */
		DataSet<Tuple4<String, String, Integer, String>> itemsMoreSold = csvInput
				// first we group by the StockCode or item code
				.groupBy(1)
				// each group corresponds to one StockCode, so we sum the Quantities in eac group
				.sum(2)
				// then we group again by country
				.groupBy(3)
				// sort the group
				.sortGroup(2, Order.DESCENDING)
				// select the first 5 of each sorted group
				.first(5);

		itemsMoreSold.print();



	} // end main method

	public static final class ReadInFilter implements FilterFunction<Tuple4<String, String, Integer, String>> {
		//private static final long serialVersionUID = 1L;

		@Override
		public boolean filter(Tuple4<String, String, Integer, String> value) {
			if (value.f0 != null && value.f0 != "")
				if (value.f1 != null && value.f1 != "")
					//if (value.f2.matches("[0-9:]+"))
					return true;
			return false;
		}
	}

	public static class GroupItemsPerBill implements GroupReduceFunction<Tuple4<String, String, Integer, String>, Tuple3<String, Integer, String>> {
		//private static final long serialVersionUID = 1L;

		@Override
		public void reduce(Iterable<Tuple4<String, String, Integer, String>> transactions, Collector<Tuple3<String,Integer, String>> out) {

			// transactions contains all the purchased items with a corresponding InvoiceNo
			// collect here all the products corresponding to the same InvoiceNo
			String invoiceNumber="";
			String groupItems = "";
			int numItems = 0;

			// f0 f1 f2
			// Tuple4<transaction number, product, quantity, country>
			for (Tuple4<String, String, Integer, String> trans : transactions) {
				if(numItems==0){
					invoiceNumber = trans.f0;
					groupItems += trans.f1 + ", ";
				} else {
					groupItems += trans.f1 + ", ";
				}
				numItems++;

			}
			out.collect(new Tuple3<String,Integer, String>(invoiceNumber, numItems, groupItems));
		}

	}


}
