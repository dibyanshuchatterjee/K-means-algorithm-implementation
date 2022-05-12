package edu.rit.ibd.a7;

import ch.obermuhlner.math.big.BigComplex;
import ch.obermuhlner.math.big.BigComplexMath;
import ch.obermuhlner.math.big.BigDecimalMath;
import ch.obermuhlner.math.big.DefaultBigDecimalMath;
import com.google.common.collect.Sets;
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
import java.util.*;

public class AssignPointsToCentroids {
	public enum Distance {Manhattan, Euclidean};
	public enum Mean {Arithmetic, Geometric};

	static Map<String, Map<String, Decimal128>> centroids = new HashMap<>();
	static Map<String, Decimal128> SSE = new HashMap<>();
	static Map<String, Integer> totalCount = new HashMap<>();

	public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String mongoCol = args[2];
		final Distance distance = Distance.valueOf(args[3]);
		final Mean mean = Mean.valueOf(args[4]);

		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);

		MongoCollection<Document> collection = db.getCollection(mongoCol);

		collection.createIndex(new Document("_id",1));
		initializeMap(collection);
		calculateDistance(collection, distance);
		setCentroids(collection);
		newCentroids(collection,mean);

		client.close();
	}

	public static void initializeMap(MongoCollection<Document> collection){
		/**
		 * 1. adding contents of documents in map(points and centroids)
		 * 2. Map format - p_i, dim_j, Decimal128
		 * 3. finding docs first with speced ids and storing in map
		 * */

		for (Document doc : collection.find(Document.parse("{_id:/^c\\_.*/}")).sort(Document.parse("{_id:1}")).batchSize(500)){
				//if it is centroid id
				String _id = doc.getString("_id");
				Document cenDoc = doc.get("centroid", Document.class);
				int docLen = cenDoc.size();
				Map<String, Decimal128> innerMap = new HashMap<>();
				for (int i = 0 ; i<docLen; i++){
					innerMap.put("dim_"+i, cenDoc.get("dim_"+i, Decimal128.class));
				}
				centroids.put(_id, innerMap);
		}
		for (String _ids : centroids.keySet()){
			totalCount.put(_ids,0);
			SSE.put(_ids, new Decimal128(0));
		}
	}

	public static void calculateDistance(MongoCollection<Document> collection, Distance distance){
//		FindIterable<Document> iterable = collection.find().batchSize(500);
		for (Document docs : collection.find(Document.parse("{_id:/^p\\_.*/}")).sort(Document.parse("{_id:1}")).batchSize(500)){
			Map<String, Decimal128> distMap = new HashMap<>();
				Document pointDoc = docs.get("point", Document.class);
				String closestCen = "";
				switch (distance) {
					case Euclidean -> {
						distMap.putAll(evaluateEuclidean(pointDoc));
						closestCen = findClosestCentroid(distMap);
					}
					case Manhattan -> {
						distMap.putAll(evaluateManhattan(pointDoc));
						closestCen = findClosestCentroid(distMap);
					}
				}
				totalCount.put(closestCen,totalCount.get(closestCen) + 1);

//			if (!totalCount.containsKey(closestCen)){
//				totalCount.put(closestCen,1);
//			}
//			else {
//				int value = totalCount.get(closestCen);
//				value += 1;
//				totalCount.put(closestCen, value);
//			}

				BigDecimal temp = new BigDecimal(String.valueOf(SSE.get(closestCen)));
				temp = temp.add(new BigDecimal(String.valueOf(distMap.get(closestCen))).
						pow(2, MathContext.DECIMAL128), MathContext.DECIMAL128);
				SSE.put(closestCen, new Decimal128(temp));
				addLabel(closestCen, collection, docs);
		}

	}

	public static Map<String, Decimal128> evaluateEuclidean(Document docs){

		Map<String, Decimal128> distMap = new HashMap<>();

		for (String centroid : centroids.keySet()){
			BigDecimal summation = new BigDecimal(0);
			for (String dims : centroids.get(centroid).keySet()){
//					BigDecimal centroidDim = new BigDecimal(String.valueOf(centroids.get(centroid).get(dims)));
//					BigDecimal pointDim = new BigDecimal(String.valueOf(docs.get(dims, Decimal128.class)));
					BigDecimal centroidDim = centroids.get(centroid).get(dims).bigDecimalValue();
					BigDecimal pointDim = docs.get(dims, Decimal128.class).bigDecimalValue();

					summation = summation.add(centroidDim.
							subtract(pointDim, MathContext.DECIMAL128).
							pow(2, MathContext.DECIMAL128), MathContext.DECIMAL128);
			}
			summation = summation.sqrt(MathContext.DECIMAL128);
			distMap.put(centroid,new Decimal128(summation));

		}

		return distMap;
	}

	public static Map<String, Decimal128> evaluateManhattan(Document docs){

		Map<String, Decimal128> distMap = new HashMap<>();

		for (String centroid : centroids.keySet()){
			BigDecimal summation = new BigDecimal(0);
			for (String dims : centroids.get(centroid).keySet()){

//					BigDecimal centroidDim = new BigDecimal(String.valueOf(centroids.get(centroid).get(dims)));
//					BigDecimal pointDim = new BigDecimal(String.valueOf(docs.get(dims, Decimal128.class)));
					BigDecimal centroidDim = centroids.get(centroid).get(dims).bigDecimalValue();
					BigDecimal pointDim = docs.get(dims, Decimal128.class).bigDecimalValue();

				BigDecimal difference = centroidDim.subtract(pointDim, MathContext.DECIMAL128).abs();
				summation = summation.add(difference, MathContext.DECIMAL128);
//				summation = summation.add(centroidDim.
//							subtract(pointDim, MathContext.DECIMAL128).
//							abs(MathContext.DECIMAL128), MathContext.DECIMAL128);
			}

			distMap.put(centroid,new Decimal128(summation));
		}
		return distMap;
	}

	public static String findClosestCentroid(Map<String, Decimal128> distMap){
//		Decimal128 min = Collections.min(distMap.values());
////		System.out.println("testttt " + distMap);
//		for (Map.Entry<String, Decimal128> entry : distMap.entrySet()){
//			if (Objects.equals(entry.getValue(), min)) {
//				return entry.getKey();
//			}
//		} // for a point {centroid,distnace}
		List<String> lis = new ArrayList<>(distMap.keySet());
		Decimal128 minimum = distMap.get(lis.get(0));
		String kk = lis.get(0);
		//System.out.println(distMap);
		for (Map.Entry<String, Decimal128> entry : distMap.entrySet()){
			//System.out.println(minimum   +"->"+ entry.getValue() +"----"+minimum.compareTo(entry.getValue()));
			if (minimum.compareTo(entry.getValue()) > 0){
				//System.out.println(minimum   +"->"+ entry.getValue() +"----+++ "+minimum.compareTo(entry.getValue()));
				minimum = entry.getValue();
				kk = entry.getKey();
				//System.out.println("reached rrr" + kk);
			}
		}

//		for (Map.Entry<String, Decimal128> entry : distMap.entrySet()){
//			if (minimum.compareTo(entry.getValue()) == 0) {
//				return entry.getKey();//return centroid
//			}
//		}

		return kk;
	}

	public static void addLabel(String clostestCen, MongoCollection<Document> collection, Document doc){

		BasicDBObject setQuery = new BasicDBObject().append("$set", new BasicDBObject().append("label", clostestCen));

		collection.updateOne(Filters.eq("_id", doc.getString("_id")), setQuery);

	}

	public static void setCentroids(MongoCollection<Document> collection){

		for (String centroid : centroids.keySet()){

			BasicDBObject settotal_points = new BasicDBObject().append("$set", new BasicDBObject().
					append("total_points", totalCount.get(centroid)));
			//System.out.println(totalCount.get(centroid) + "-c "+ centroid);
			collection.updateOne(Filters.eq("_id", centroid), settotal_points);

			BasicDBObject setsse = new BasicDBObject().append("$set", new BasicDBObject().
					append("sse", SSE.get(centroid)));

			collection.updateOne(Filters.eq("_id", centroid), setsse);

			BasicDBObject setreinitialize = new BasicDBObject();
			if (totalCount.get(centroid) > 0){
				setreinitialize.append("$set", new BasicDBObject().
						append("reinitialize",false));
			}
			else {
				setreinitialize.append("$set", new BasicDBObject().
						append("reinitialize",true));
				totalCount.remove(centroid);
			}
			collection.updateOne(Filters.eq("_id", centroid), setreinitialize);

		}

	}

	public static void newCentroids(MongoCollection<Document> collection, Mean mean){

		/**
		 * Finding arithmatic/geometric mean of each point derived
		 * */
		for (String centroid : totalCount.keySet()){
			BasicDBObject where = new BasicDBObject();
			where.put("label", centroid);
			FindIterable<Document> points = collection.find(where).batchSize(500);

			HashMap<String, BigDecimal> meanMap = new HashMap<>();
			int total = 0;
			for (Document point : points){
				total += 1;
				Document pointDim = point.get("point", Document.class);
				getEssentialsForMean(mean, pointDim, meanMap);
			}
			switch (mean){
				case Arithmetic -> {
					Document innerDoc = new Document();
					for (String field : meanMap.keySet()){
					BigDecimal dim = meanMap.get(field).multiply(new BigDecimal(total).
								pow(-1, MathContext.DECIMAL128), MathContext.DECIMAL128);
					innerDoc.append(field,dim);

					}
					Document d = new Document();
					d.append("_id","new_" + centroid).append("centroid",innerDoc);
					collection.insertOne(d);
				}
				case Geometric -> {
					Document innerDoc = new Document();
					for (String field : meanMap.keySet()){
						BigDecimal dim = meanMap.get(field).multiply(new BigDecimal(total).
								pow(-1, MathContext.DECIMAL128), MathContext.DECIMAL128);
						BigDecimal newDim = DefaultBigDecimalMath.exp(dim);
						innerDoc.append(field,newDim);

					}
					Document d = new Document();
					d.append("_id","new_" + centroid).append("centroid",innerDoc);
					collection.insertOne(d);
				}
			}
		}

	}

	public static void getEssentialsForMean(Mean mean, Document point, HashMap<String, BigDecimal> meanMap){

		switch (mean){
			case Arithmetic -> {
				for (int i = 0; i<point.size(); i++){
					String field = "dim_" + i;
						if (!meanMap.containsKey(field)){
							meanMap.put(field, new BigDecimal(String.valueOf(point.get(field, Decimal128.class))));
						}
						else {
							BigDecimal temp = meanMap.get(field);
							BigDecimal sum = temp.add(new BigDecimal(String.valueOf(point.get(field, Decimal128.class))), MathContext.DECIMAL128);
							meanMap.put(field, sum);
						}
				}

			}
			case Geometric -> {
				for (int i = 0; i<point.size(); i++){
					String field = "dim_" + i;
					if (!meanMap.containsKey(field)){
						meanMap.put(field, DefaultBigDecimalMath.log(new BigDecimal(String.valueOf(point.get(field, Decimal128.class)))));
					}
					else {
						BigDecimal temp = meanMap.get(field);
						BigDecimal sum = temp.add(DefaultBigDecimalMath.log(new BigDecimal(String.valueOf(point.get(field, Decimal128.class)))), MathContext.DECIMAL128);
						meanMap.put(field, sum);
					}
				}

			}
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
