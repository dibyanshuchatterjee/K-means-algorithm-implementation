package edu.rit.ibd.a7;

import ch.obermuhlner.math.big.BigDecimalMath;
import ch.obermuhlner.math.big.DefaultBigDecimalMath;
import com.mongodb.BasicDBObject;
import com.mongodb.Mongo;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.types.Decimal128;

import javax.print.Doc;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SilhouetteCoefficient {
	public enum Distance {Manhattan, Euclidean};
	public enum Mean {Arithmetic, Geometric};
	static BigDecimal Asum = BigDecimal.ZERO;
	static BigDecimal AsumLog = BigDecimal.ZERO;
	static int aTotal = 0;
	static Map<String, BigDecimal> Dsum = new HashMap<>();
	static Map<String, BigDecimal> DsumLog = new HashMap<>();
	static Map<String, Integer> dTotal = new HashMap<>();
	static String ongoingLabel = "";


	public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String mongoCol = args[2];
		final String pointId = args[3];
		final Distance distance = Distance.valueOf(args[4]);
		final Mean mean = Mean.valueOf(args[5]);

		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);

		MongoCollection<Document> collection = db.getCollection(mongoCol);

		collection.createIndex(new Document("_id",1));

		computeDistance(collection, pointId, distance);
		Map<String, BigDecimal> result = new HashMap<>(computeMean(mean));
		setDocs(result, collection, pointId);

		client.close();
	}

	public static void computeDistance(MongoCollection<Document> collection, String pointId, Distance distance){

		BasicDBObject where = new BasicDBObject();
		where.put("_id", pointId);
		Document pointDoc = collection.find(where).first();
		String currentLabel = pointDoc.getString("label");
		ongoingLabel = currentLabel;
		for (Document point : collection.find(Document.parse("{_id:/^p\\_.*/}")).sort(Document.parse("{_id:1}")).batchSize(500)){
			if (Objects.equals(point.getString("label"), currentLabel))
				computeA(pointDoc, point, distance);
			computeDs(pointDoc, point, distance);
		}
	}

	public static void computeA(Document givenPoint, Document extractedPoint, Distance distance){

		Document givenPointDims = givenPoint.get("point", Document.class);
		Document extractedPointDims = extractedPoint.get("point", Document.class);

		switch (distance){
			case Euclidean -> {
				BigDecimal summation = new BigDecimal(0);
				for (int i = 0; i< givenPointDims.size(); i++){
					String dim = "dim_" + i;
					BigDecimal gpDim = new BigDecimal(String.valueOf(givenPointDims.get(dim, Decimal128.class)));
					BigDecimal epDim = new BigDecimal(String.valueOf(extractedPointDims.get(dim, Decimal128.class)));
					BigDecimal diff = gpDim.subtract(epDim, MathContext.DECIMAL128);
					BigDecimal squared = diff.pow(2, MathContext.DECIMAL128);
					summation = summation.add(squared, MathContext.DECIMAL128);
				}
				summation = summation.sqrt(MathContext.DECIMAL128);
				Asum = Asum.add(summation);
				BigDecimal tempMin = summation.min(BigDecimal.ZERO);
				if (!tempMin.equals(summation))
					AsumLog = AsumLog.add(BigDecimalMath.log(summation, MathContext.DECIMAL128), MathContext.DECIMAL128);
				aTotal += 1;

			}
			case Manhattan -> {
				BigDecimal summation = new BigDecimal(0);
				for (int i = 0; i< givenPointDims.size(); i++){
					String dim = "dim_" + i;

					BigDecimal gpDim = new BigDecimal(String.valueOf(givenPointDims.get(dim, Decimal128.class)));
					BigDecimal epDim = new BigDecimal(String.valueOf(extractedPointDims.get(dim, Decimal128.class)));
					BigDecimal diff = gpDim.subtract(epDim, MathContext.DECIMAL128).abs(MathContext.DECIMAL128);
					summation = summation.add(diff, MathContext.DECIMAL128);
				}

				Asum = Asum.add(summation, MathContext.DECIMAL128);

				BigDecimal tempMin = summation.min(BigDecimal.ZERO);
				if (!tempMin.equals(summation))
					AsumLog = AsumLog.add(BigDecimalMath.log(summation, MathContext.DECIMAL128), MathContext.DECIMAL128);
				aTotal += 1;
			}


		}


	}

	public static void computeDs(Document givenPoint, Document extractedPoint, Distance distance){

		Document givenPointDims = givenPoint.get("point", Document.class);
		Document extractedPointDims = extractedPoint.get("point", Document.class);


		switch (distance){
			case Euclidean -> {
				BigDecimal summation = new BigDecimal(0);
				for (int i = 0; i< givenPointDims.size(); i++){
					String dim = "dim_" + i;
					BigDecimal gpDim = new BigDecimal(String.valueOf(givenPointDims.get(dim, Decimal128.class)));
					BigDecimal epDim = new BigDecimal(String.valueOf(extractedPointDims.get(dim, Decimal128.class)));
					BigDecimal diff = gpDim.subtract(epDim, MathContext.DECIMAL128);
					BigDecimal squared = diff.pow(2, MathContext.DECIMAL128);
					summation = summation.add(squared, MathContext.DECIMAL128);
				}
				summation = summation.sqrt(MathContext.DECIMAL128);
				String label = extractedPoint.getString("label");
				if (!Dsum.containsKey(label)){
					Dsum.put(label,summation);
					dTotal.put(label,1);
					BigDecimal tempMin = summation.min(BigDecimal.ZERO);
					if (!tempMin.equals(summation))
						DsumLog.put(label,BigDecimalMath.log(summation, MathContext.DECIMAL128));
				}
				else {
					BigDecimal temp = Dsum.get(label);
					temp = temp.add(summation, MathContext.DECIMAL128);
					BigDecimal tempLog = DsumLog.get(label);
					BigDecimal tempMin = summation.min(BigDecimal.ZERO);
					if (!tempMin.equals(summation))
						tempLog = tempLog.add(BigDecimalMath.log(summation, MathContext.DECIMAL128), MathContext.DECIMAL128);
					int totalVal = dTotal.get(label);
					Dsum.put(label,temp);
					dTotal.put(label,totalVal+1);
					DsumLog.put(label,tempLog);
				}

			}
			case Manhattan -> {
				BigDecimal summation = new BigDecimal(0);
				for (int i = 0; i< givenPointDims.size(); i++){
					String dim = "dim_" + i;

					BigDecimal gpDim = new BigDecimal(String.valueOf(givenPointDims.get(dim, Decimal128.class)));
					BigDecimal epDim = new BigDecimal(String.valueOf(extractedPointDims.get(dim, Decimal128.class)));
					BigDecimal diff = gpDim.subtract(epDim, MathContext.DECIMAL128).abs(MathContext.DECIMAL128);
					summation = summation.add(diff, MathContext.DECIMAL128);
				}

				String label = extractedPoint.getString("label");
				if (!Dsum.containsKey(label)){
					Dsum.put(label,summation);
					dTotal.put(label,1);
					BigDecimal tempMin = summation.min(BigDecimal.ZERO);
					if (!tempMin.equals(summation))
						DsumLog.put(label,BigDecimalMath.log(summation, MathContext.DECIMAL128));
				}
				else {
					BigDecimal temp = Dsum.get(label);
					temp = temp.add(summation, MathContext.DECIMAL128);
					BigDecimal tempLog = DsumLog.get(label);
					BigDecimal tempMin = summation.min(BigDecimal.ZERO);
					if (!tempMin.equals(summation))
						tempLog = tempLog.add(BigDecimalMath.log(summation, MathContext.DECIMAL128), MathContext.DECIMAL128);
					int totalVal = dTotal.get(label);
					Dsum.put(label,temp);
					dTotal.put(label,totalVal+1);
					DsumLog.put(label,tempLog);
				}

			}


		}

	}

	public static Map<String, BigDecimal> computeMean(Mean mean){

		Map<String, BigDecimal> result = new HashMap<>();
		computeForA(mean, result);
		comuteForD(mean, result);
		return result;

	}

	public static void computeForA(Mean mean, Map<String, BigDecimal> result ){

		switch (mean){
			case Arithmetic -> {
				BigDecimal m = Asum.multiply(new BigDecimal(aTotal-1).pow(-1, MathContext.DECIMAL128), MathContext.DECIMAL128);
				result.put("a",m);
			}
			case Geometric -> {
				BigDecimal divided = AsumLog.multiply(new BigDecimal(aTotal-1).pow(-1, MathContext.DECIMAL128), MathContext.DECIMAL128);
				BigDecimal res = BigDecimalMath.exp(divided, MathContext.DECIMAL128);
				result.put("a",res);
			}
		}

	}

	public static void comuteForD(Mean mean, Map<String, BigDecimal> result ){

		switch (mean){
			case Arithmetic -> {

				for (Map.Entry<String, BigDecimal> entry : Dsum.entrySet()){
					int total = dTotal.get(entry.getKey());
					BigDecimal sum = entry.getValue();
					BigDecimal m = sum.multiply(new BigDecimal(total).pow(-1, MathContext.DECIMAL128), MathContext.DECIMAL128);
					String temp = entry.getKey();
					String d_i_temp = temp.substring(2);
					String d_i = "d_" + d_i_temp;
					String c_i_temp = ongoingLabel.substring(2);
					if (!d_i_temp.equals(c_i_temp))
						result.put(d_i,m);
				}
			}
			case Geometric -> {
				for (Map.Entry<String, BigDecimal> entry : DsumLog.entrySet()){
					int total = dTotal.get(entry.getKey());
					BigDecimal sum = entry.getValue();
					BigDecimal m = sum.multiply(new BigDecimal(total).pow(-1, MathContext.DECIMAL128), MathContext.DECIMAL128);
					String temp = entry.getKey();
					String d_i_temp = temp.substring(2);
					String d_i = "d_" + d_i_temp;
					String c_i_temp = ongoingLabel.substring(2);
					if (!d_i_temp.equals(c_i_temp))
						result.put(d_i,m);
				}

			}
		}
	}

	public static void setDocs(Map<String, BigDecimal> result, MongoCollection<Document> collection, String pointId){
		for (Map.Entry<String, BigDecimal> entry : result.entrySet()){
			BasicDBObject setQuery = new BasicDBObject().append("$set", new BasicDBObject().append(entry.getKey(), entry.getValue()));
			collection.updateOne(Filters.eq("_id", pointId), setQuery);
		}
	}


	private static MongoClient getClient(String mongoDBURL) {
		MongoClient client = null;
		if (mongoDBURL.equals("None"))
			client = new MongoClient();
		else
			client = new MongoClient(new MongoClientURI(mongoDBURL));
		return client;
	}

}
