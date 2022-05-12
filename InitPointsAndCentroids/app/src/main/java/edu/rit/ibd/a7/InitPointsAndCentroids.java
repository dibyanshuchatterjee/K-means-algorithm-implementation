package edu.rit.ibd.a7;

import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.*;
import java.util.*;
import java.util.logging.Filter;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.types.Decimal128;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import javax.print.Doc;

public class InitPointsAndCentroids {
	public enum Scaling {None, MinMax, Mean, ZScore}


	public static void main(String[] args) throws Exception {
		final String jdbcURL = args[0];
		final String jdbcUser = args[1];
		final String jdbcPwd = args[2];
		final String sqlQuery = args[3];
		final String mongoDBURL = args[4];
		final String mongoDBName = args[5];
		final String mongoCol = args[6];
		final Scaling scaling = Scaling.valueOf(args[7]);
		final int k = Integer.valueOf(args[8]);
		
		Connection con = DriverManager.getConnection(jdbcURL, jdbcUser, jdbcPwd);
		
		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		
		MongoCollection<Document> collection = db.getCollection(mongoCol);


		calculateLimits(con, sqlQuery, collection);
		collection.createIndex(new Document("_id",1));
		setPoints(con, sqlQuery, collection, scaling);
		setCentroid(k,collection);


		client.close();
		con.close();
	}
	
	private static Decimal128 readAttribute(ResultSet rs, String label) throws SQLException {
		// From: https://stackoverflow.com/questions/9482889/set-specific-precision-of-a-bigdecimal
		BigDecimal x = rs.getBigDecimal(label);
		x = x.setScale(x.scale() + MathContext.DECIMAL128.getPrecision() - x.precision(), MathContext.DECIMAL128.getRoundingMode());
		return new Decimal128(x);
	}

	public static void calculateLimits(Connection con, String query,MongoCollection<Document> collection) throws SQLException {

		PreparedStatement st = con.prepareStatement(query);
		st.setFetchSize(/* Batch size */ 500);
		ResultSet rs = st.executeQuery();
		List<String> attributes = new ArrayList<>(getColoumnName(rs));
		rs.close();
		st.close();
		Document dInit = new Document();
		dInit.append("_id","limits");
		collection.insertOne(dInit);
		for(String dims : attributes){
			PreparedStatement stNew = con.prepareStatement(query);
			stNew.setFetchSize(/* Batch size */ 500);
			ResultSet rsNew = stNew.executeQuery();
			int total = 1;
			rsNew.next();
			BigDecimal sum = rsNew.getBigDecimal(dims);
			sum = sum.setScale(sum.scale() + MathContext.DECIMAL128.getPrecision() - sum.precision(), MathContext.DECIMAL128.getRoundingMode());

			BigDecimal min = rsNew.getBigDecimal(dims);
			min = min.setScale(min.scale() + MathContext.DECIMAL128.getPrecision() - min.precision(), MathContext.DECIMAL128.getRoundingMode());

			BigDecimal max = rsNew.getBigDecimal(dims);
			max = min.setScale(max.scale() + MathContext.DECIMAL128.getPrecision() - max.precision(), MathContext.DECIMAL128.getRoundingMode());


			while (rsNew.next()){
				sum = sum.add(rsNew.getBigDecimal(dims),MathContext.DECIMAL128);
				total++;

				if (min.compareTo(rsNew.getBigDecimal(dims)) > 0)
					min = new BigDecimal(String.valueOf(readAttribute(rsNew,dims)));
				if (max.compareTo(rsNew.getBigDecimal(dims)) < 0)
					max = new BigDecimal(String.valueOf(readAttribute(rsNew,dims)));
			}
			rsNew.close();
			stNew.close();

			PreparedStatement stAgain = con.prepareStatement(query);
			stAgain.setFetchSize(/* Batch size */ 500);
			ResultSet rsAgain = stAgain.executeQuery();

//			BigDecimal mean = sum.divide(new BigDecimal(total), MathContext.DECIMAL128);
			BigDecimal mean = sum.multiply(new BigDecimal(total).pow(-1, MathContext.DECIMAL128), MathContext.DECIMAL128);
			Decimal128 std = calculateSTD(rsAgain, mean, total, dims);
			Decimal128 meanToPut = new Decimal128(mean);
			Decimal128 minToPut = new Decimal128(min);
			Decimal128 maxToPut = new Decimal128(max);
			rsAgain.close();
			stAgain.close();


			Document dimDoc = addLimitsDoc(collection, std, meanToPut, minToPut, maxToPut, dims);
			BasicDBObject setQuery = new BasicDBObject().append("$set", new BasicDBObject().append(dims, dimDoc));


			collection.updateOne(Filters.eq("_id", "limits"), setQuery);

		}
	}

	public static List<String> getColoumnName(ResultSet rs) throws SQLException {

		List<String> attributes = new ArrayList<>();
		for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++){
			if (!Objects.equals(rs.getMetaData().getColumnName(i), "id"))
				attributes.add(rs.getMetaData().getColumnName(i));
		}

		return attributes;
	}
	public static Decimal128 calculateSTD(ResultSet rs, BigDecimal mean, double total, String dims) throws SQLException {

//		rs.next();
//		BigDecimal summation = rs.getBigDecimal(dims);
//		summation = summation.setScale(summation.scale() + MathContext.DECIMAL128.getPrecision() - summation.precision(), MathContext.DECIMAL128.getRoundingMode());
		BigDecimal summation = new BigDecimal(0);

		while (rs.next()){
			BigDecimal subracted = rs.getBigDecimal(dims).subtract(mean, MathContext.DECIMAL128);
			subracted = subracted.pow(2,MathContext.DECIMAL128);
			summation = summation.add(subracted, MathContext.DECIMAL128);

		}

		BigDecimal res = summation.multiply(new BigDecimal(total).pow(-1, MathContext.DECIMAL128), MathContext.DECIMAL128);
		res = res.sqrt(MathContext.DECIMAL128);


		return new Decimal128(res);
	}

	public static Document addLimitsDoc(MongoCollection<Document> collection, Decimal128 std, Decimal128 meanToPut, Decimal128 minToPut, Decimal128 maxToPut, String dims){

		Document d = new Document();
		d.append("min",minToPut).append("max", maxToPut).append("mean", meanToPut).append("std", std);
//		Document dMain = new Document();
//		dMain.append(dims,d);

		return d;


	}

	public static void setPoints(Connection con, String sql, MongoCollection<Document> collection, Scaling scaling) throws SQLException {

		BasicDBObject where = new BasicDBObject();
		where.put("_id", "limits");
		Document limDoc = collection.find(where).first(); //extracting doc that has limits

		//create a map for dim_i and min - val,...

		Map<String, Map<String, Decimal128>> map = new HashMap<>(); //this map stores limits

		PreparedStatement st = con.prepareStatement(sql);
		st.setFetchSize(/* Batch size */ 500);
		ResultSet rs = st.executeQuery();
		List<String> dim = new ArrayList<>(getColoumnName(rs));

		for (String dims : dim){ //loop to populate the map

			Map<String, Decimal128> innerMap = new HashMap<>();

			Document dimDoc = limDoc.get(dims, Document.class);
			Decimal128 min = dimDoc.get("min", Decimal128.class);
			Decimal128 max = dimDoc.get("max", Decimal128.class);
			Decimal128 mean = dimDoc.get("mean", Decimal128.class);
			Decimal128 std = dimDoc.get("std", Decimal128.class);

			innerMap.put("min",min);
			innerMap.put("max",max);
			innerMap.put("mean",mean);
			innerMap.put("std",std);

			map.put(dims,innerMap);

		}

		while (rs.next()){ //loop to populate the collection

			long id = rs.getLong("id");
			String _id = "p_" + id;
			Document d = new Document();
			d.append("_id",_id);
			collection.insertOne(d);

			Document dToInsert = new Document();
			for (String dims : dim){ //loop to fetch each dim_i
				Decimal128 dim_i = readAttribute(rs,dims);
				Decimal128 scaledDim = scaleDims(dim_i, map, dims, scaling);
				dToInsert.append(dims,scaledDim);
			}
			BasicDBObject setQuery = new BasicDBObject().append("$set", new BasicDBObject().append("point", dToInsert));

			collection.updateOne(Filters.eq("_id", _id), setQuery);

		}

		rs.close();
		st.close();


	}

	public static Decimal128 scaleDims(Decimal128 dim_i, Map<String, Map<String, Decimal128>> map, String dims, Scaling scaling){
		Map<String, Decimal128> innerMap = map.get(dims);

		BigDecimal dim = new BigDecimal(String.valueOf(dim_i));

		switch(scaling) {
			case MinMax:
				dim = dim.subtract(new BigDecimal(String.valueOf(innerMap.get("min")))).
						divide(new BigDecimal(String.valueOf(innerMap.get("max"))).
								subtract(new BigDecimal(String.valueOf(innerMap.get("min")))),MathContext.DECIMAL128);
				return new Decimal128(dim);
			case Mean:
				dim = dim.subtract(new BigDecimal(String.valueOf(innerMap.get("mean")))).
						divide(new BigDecimal(String.valueOf(innerMap.get("max"))).
								subtract(new BigDecimal(String.valueOf(innerMap.get("min")))),MathContext.DECIMAL128);
				return new Decimal128(dim);
			case ZScore:
				dim = dim.subtract(new BigDecimal(String.valueOf(innerMap.get("mean")))).
						divide(new BigDecimal(String.valueOf(innerMap.get("std"))), MathContext.DECIMAL128);
				return new Decimal128(dim);

		}

		return dim_i;
	}

	public static void setCentroid(int k, MongoCollection<Document> collection){

		for (int i = 0; i<k; i++){
			String _id = "c_" + i;
			Document d = new Document();
			d.append("_id",_id).append("centroid",new Document());
			collection.insertOne(d);
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
