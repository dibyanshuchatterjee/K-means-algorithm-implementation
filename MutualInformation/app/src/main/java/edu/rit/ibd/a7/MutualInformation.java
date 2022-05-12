package edu.rit.ibd.a7;

import ch.obermuhlner.math.big.BigDecimalMath;
import com.mongodb.BasicDBObject;
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

public class MutualInformation {

	static Map<Integer, Set<String>> U_Map = new HashMap<>();
	static Map<Integer, Set<String>> V_Map = new HashMap<>();
	static List<Integer> a = new ArrayList<>();
	static List<Integer> b = new ArrayList<>();
	static List<Integer> c = new ArrayList<>();
	static int n = 0;
	static BigDecimal HU = BigDecimal.ZERO;
	static BigDecimal HV = BigDecimal.ZERO;
	static BigDecimal mi = BigDecimal.ZERO;
	static BigDecimal emi = BigDecimal.ZERO;


	public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String mongoCol = args[2];
		final int R = Integer.valueOf(args[3]);
		final int C = Integer.valueOf(args[4]);

		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);

		MongoCollection<Document> collection = db.getCollection(mongoCol);

		collection.createIndex(new Document("_id",1));
		initializeMap(R,C);
		evaluateAandB(collection);
		setAandB(); //fix 0 issue
		Map<Integer, Set<String>> U_MapCopy = new HashMap<>(U_Map);
		Map<Integer, Set<String>> V_MapCopy = new HashMap<>(V_Map);

		contingencyMatrix(R,C, U_MapCopy,V_MapCopy);
		U_MapCopy.clear();
		V_MapCopy.clear();

		n = evaluateN();

		Map<Integer, BigDecimal> HUMap = new HashMap<>(evaluateHU());
		Map<Integer, BigDecimal> HVMap = new HashMap<>(evaluateHV());
		evaluateMI(HUMap, HVMap);
		HUMap.clear();
		HVMap.clear();
		List<BigDecimal> factorials = new ArrayList<>(storeFactorials(n));
		System.out.println("fact test " + factorials.get(n));

//		System.out.println("fact " + factorials);
		evaluateEMI(factorials);
		setDocs(collection);

		client.close();
	}

	public static void evaluateAandB(MongoCollection<Document> collection){

		for (Document doc : collection.find(Document.parse("{_id:/^p\\_.*/}")).sort(Document.parse("{_id:1}")).batchSize(500)){

			int label_U = doc.getInteger("label_u");
			int label_V = doc.getInteger("label_v");
			String pointId = doc.getString("_id");

			if (!U_Map.containsKey(label_U)){
				Set<String> innerSet = new HashSet<>();
				innerSet.add(pointId);
				U_Map.put(label_U, innerSet);
			}
			else {
				Set<String> innerSet = new HashSet<>(U_Map.get(label_U));
				innerSet.add(pointId);
				U_Map.put(label_U, innerSet);
			}
			if (!V_Map.containsKey(label_V)){
				Set<String> innerSet = new HashSet<>();
				innerSet.add(pointId);
				V_Map.put(label_V, innerSet);
			}
			else {
				Set<String> innerSet = new HashSet<>(V_Map.get(label_V));
				innerSet.add(pointId);
				V_Map.put(label_V, innerSet);
			}
		}
	}

	public static void setAandB(){
		for (Integer u_i : U_Map.keySet()){
			int u_iSize = U_Map.get(u_i).size();
			a.add(u_iSize);
		}
		for (Integer v_i : V_Map.keySet()){
			int v_iSize = V_Map.get(v_i).size();
			b.add(v_iSize);
		}
	}

	public static void contingencyMatrix(int R, int C, Map<Integer, Set<String>> U_MapCopy, Map<Integer, Set<String>> V_MapCopy){

		int [][] contMatr = new int[R][C];
		int i = 0;
		for (Integer u : U_MapCopy.keySet()){
			int j = 0;
			for (Integer v : V_MapCopy.keySet()){
				int count = 0;
				Set<String> tempForU = U_MapCopy.get(u);
				Set<String> tempForV = V_MapCopy.get(v);
				for (String str1 : tempForU){
					for (String str2 : tempForV){
						if (str1.equals(str2)){
							count++;
						}
					}
				}
					contMatr[i][j] = count;
				j++;
			}
			i++;
		}
		evaluateC(contMatr);
	}

	public static void evaluateC(int [][] contMatr){
		int rows = contMatr.length;
		int cols = contMatr[0].length;
		for (int i = 0; i<rows; i++){
			for (int j = 0; j<cols; j++){
				c.add(contMatr[i][j]);
			}
		}

	}

	public static int evaluateN(){
		int n = 0;
		Map<Integer, Set<String>> U_MapCopy = new HashMap<>(U_Map);
		Map<Integer, Set<String>> V_MapCopy = new HashMap<>(V_Map);
		for (Integer i : U_MapCopy.keySet()){
			n += U_MapCopy.get(i).size();
		}
		return n;
	}

	public static Map<Integer, BigDecimal> evaluateHU(){
		Map<Integer, BigDecimal> HUMap = new HashMap<>(evaluateProbability_U());
		BigDecimal summation = BigDecimal.ZERO;
		for (Map.Entry<Integer, BigDecimal> entry : HUMap.entrySet()){
			if (entry.getValue().compareTo(BigDecimal.ZERO)>0)
			summation = summation.subtract(entry.getValue().
					multiply(
							BigDecimalMath.log(entry.getValue(), MathContext.DECIMAL128),
							MathContext.DECIMAL128), MathContext.DECIMAL128);
		}
		HU = summation;
		return HUMap;
	}

	public static Map<Integer, BigDecimal> evaluateProbability_U(){
		Map<Integer, BigDecimal> result = new HashMap<>();
		for (Map.Entry<Integer, Set<String>> entry : U_Map.entrySet()){

			BigDecimal probability = new BigDecimal(entry.getValue().size()).
					divide(new BigDecimal(n), MathContext.DECIMAL128);
			result.put(entry.getKey(),probability);
		}
		return result;
	}

	public static Map<Integer, BigDecimal> evaluateHV(){

		Map<Integer, BigDecimal> HVMap = new HashMap<>(evaluateProbability_V());
		BigDecimal summation = BigDecimal.ZERO;
		for (Map.Entry<Integer, BigDecimal> entry : HVMap.entrySet()){
			if (entry.getValue().compareTo(BigDecimal.ZERO)>0)
			summation = summation.subtract(entry.getValue().
					multiply(
							BigDecimalMath.log(entry.getValue(), MathContext.DECIMAL128),
							MathContext.DECIMAL128), MathContext.DECIMAL128);
		}
		HV = summation;
		return HVMap;
	}

	public static Map<Integer, BigDecimal> evaluateProbability_V(){
		Map<Integer, BigDecimal> result = new HashMap<>();
		for (Map.Entry<Integer, Set<String>> entry : V_Map.entrySet()){
			BigDecimal probability = new BigDecimal(entry.getValue().size()).divide(new BigDecimal(n), MathContext.DECIMAL128);
			result.put(entry.getKey(),probability);
		}
		return result;
	}

	public static void evaluateMI(Map<Integer, BigDecimal> HUMap, Map<Integer, BigDecimal> HVMap){
		BigDecimal summation = BigDecimal.ZERO;
		for (Integer u : U_Map.keySet()){
			for (Integer v : V_Map.keySet()){
				BigDecimal probUV = BigDecimal.ZERO;
//				Set<String> tempForU = U_Map.get(u);
//				Set<String> tempForV = V_Map.get(v);
//				boolean check = tempForU.retainAll(tempForV);
				int count = 0;
				Set<String> tempForU = U_Map.get(u);
				Set<String> tempForV = V_Map.get(v);
				for (String str1 : tempForU){
					for (String str2 : tempForV){
						if (str1.equals(str2)){
							count++;
						}
					}
				}
				probUV = new BigDecimal(count).divide(new BigDecimal(n), MathContext.DECIMAL128);
				if (probUV.compareTo(BigDecimal.ZERO)>0) {
					BigDecimal p_u = HUMap.get(u);
					BigDecimal p_v = HVMap.get(v);
					BigDecimal div = probUV.
							divide(p_u.
									multiply(p_v, MathContext.DECIMAL128), MathContext.DECIMAL128);
					BigDecimal res = BigDecimalMath.log(div, MathContext.DECIMAL128);
					BigDecimal mul = probUV.multiply(res, MathContext.DECIMAL128);
					summation = summation.add(mul, MathContext.DECIMAL128);
				}
			}
		}
		mi = summation;
	}

	public static void evaluateEMI(List<BigDecimal> factorials){
		BigDecimal summation = BigDecimal.ZERO;
		for (Map.Entry<Integer, Set<String>> r : U_Map.entrySet()){
			for (Map.Entry<Integer, Set<String>> c : V_Map.entrySet()){
				int a_i = r.getValue().size();
				int b_j = c.getValue().size();
				int k = Math.max(a_i + b_j - n,0);
				int upperBound = Math.min(a_i,b_j);
				for (int i = k; i<upperBound; i++){
					BigDecimal div = getFirstComponent(k);
					BigDecimal logged = getSecondComponent(k,a_i,b_j);
					BigDecimal A_I_fact = new BigDecimal(String.valueOf(factorials.get(a_i)));

					BigDecimal B_J_fact = new BigDecimal(String.valueOf(factorials.get(b_j)));

					BigDecimal N_Minus_Ai = new BigDecimal(n).subtract(new BigDecimal(a_i), MathContext.DECIMAL128);
					BigDecimal N_Minus_Ai_Fact = new BigDecimal(String.valueOf(factorials.get(N_Minus_Ai.intValueExact())));

					BigDecimal NminusB_jTemp = new BigDecimal(n).subtract(new BigDecimal(b_j), MathContext.DECIMAL128);
					BigDecimal N_Minus_Bj_Fact = new BigDecimal(String.valueOf(factorials.get(NminusB_jTemp.intValueExact())));

					BigDecimal NFact = new BigDecimal(String.valueOf(factorials.get(n)));

					BigDecimal KFact = new BigDecimal(String.valueOf(factorials.get(k)));

					BigDecimal A_iMinusKTemp = new BigDecimal(a_i).subtract(new BigDecimal(k), MathContext.DECIMAL128);
					BigDecimal A_iMinusKTemp_Fact = new BigDecimal(String.valueOf(factorials.get(A_iMinusKTemp.intValueExact())));

					BigDecimal B_jMinusKTemp = new BigDecimal(b_j).subtract(new BigDecimal(k), MathContext.DECIMAL128);
					BigDecimal B_jMinusKTemp_Fact = new BigDecimal(String.valueOf(factorials.get(B_jMinusKTemp.intValueExact())));

					BigDecimal lastSubTemp = new BigDecimal(n).
							subtract(new BigDecimal(a_i), MathContext.DECIMAL128).
							subtract(new BigDecimal(b_j), MathContext.DECIMAL128).
							add(new BigDecimal(k), MathContext.DECIMAL128);
					BigDecimal lastSubTemp_Fact = new BigDecimal(String.valueOf(factorials.get(lastSubTemp.intValueExact())));


					BigDecimal mul = A_I_fact.
							multiply(B_J_fact,MathContext.DECIMAL128).
							multiply(N_Minus_Ai_Fact, MathContext.DECIMAL128).
							multiply(N_Minus_Bj_Fact,MathContext.DECIMAL128);

					BigDecimal divBy = NFact.
							multiply(KFact, MathContext.DECIMAL128).
							multiply(A_iMinusKTemp_Fact, MathContext.DECIMAL128).
							multiply(B_jMinusKTemp_Fact, MathContext.DECIMAL128).
							multiply(lastSubTemp_Fact, MathContext.DECIMAL128);
//					System.out.println("mul " + mul + " divBy " + divBy);

					if (divBy.compareTo(BigDecimal.ZERO) > 0) {
						BigDecimal thirdComponentRes = mul.divide(divBy, MathContext.DECIMAL128);
						if (logged != null) {
							BigDecimal res = div.multiply(logged, MathContext.DECIMAL128).multiply(thirdComponentRes, MathContext.DECIMAL128);
							System.out.println("div " + div + " logged " + logged + " TC " + thirdComponentRes);
							summation = summation.add(res, MathContext.DECIMAL128);
						}
					}
				}
			}
		}
		emi = summation;
	}

	public static BigDecimal getFirstComponent(int k){

		return new BigDecimal(k).divide(new BigDecimal(n), MathContext.DECIMAL128);
	}

	public static BigDecimal getSecondComponent(int k, int a_i, int b_i){
		BigDecimal mul1 = new BigDecimal(n).multiply(new BigDecimal(k), MathContext.DECIMAL128);
		BigDecimal mul2 = new BigDecimal(a_i).multiply(new BigDecimal(b_i), MathContext.DECIMAL128);
		BigDecimal div = mul1.divide(mul2, MathContext.DECIMAL128);

		if (div.compareTo(new BigDecimal(0))>0){
			return BigDecimalMath.log(div,MathContext.DECIMAL128);
		}
		return null;
	}

	public static void setDocs(MongoCollection<Document> collection){

		String _id = "mi_info";
		Document document = new Document();
		document.append("_id",_id);
		collection.insertOne(document);

		for (Integer as : a){
			Document d = new Document();
			Decimal128 con = new Decimal128(as);
			collection.updateOne(
					Filters.eq("_id", _id),
					d.append(
							"$push",
							new Document("a", con))
			);
		}

		for (Integer bs : b){
			Document d = new Document();
			Decimal128 con = new Decimal128(bs);
			collection.updateOne(
					Filters.eq("_id", _id),
					d.append(
							"$push",
							new Document("b", con))
			);
		}

		for (Integer cs : c){
			Document d = new Document();
			Decimal128 con = new Decimal128(cs);
			collection.updateOne(
					Filters.eq("_id", _id),
					d.append(
							"$push",
							new Document("c", con))
			);
		}

		BasicDBObject setN = new BasicDBObject().append("$set", new BasicDBObject().
				append("n", n));
		collection.updateOne(Filters.eq("_id", _id), setN);

		BasicDBObject setHU = new BasicDBObject().append("$set", new BasicDBObject().
				append("hu", HU));
		collection.updateOne(Filters.eq("_id", _id), setHU);

		BasicDBObject setHV = new BasicDBObject().append("$set", new BasicDBObject().
				append("hv", HV));
		collection.updateOne(Filters.eq("_id", _id), setHV);

		BasicDBObject setMI = new BasicDBObject().append("$set", new BasicDBObject().
				append("mi", mi));
		collection.updateOne(Filters.eq("_id", _id), setMI);

		BasicDBObject setEMI = new BasicDBObject().append("$set", new BasicDBObject().
				append("emi", emi));
		collection.updateOne(Filters.eq("_id", _id), setEMI);
	}
	public static void initializeMap(int R, int C){
		for (int i = 0; i < R; i++){
			U_Map.put(i,new HashSet<>());
		}
		for (int j = 0; j<C; j++){
			V_Map.put(j,new HashSet<>());
		}
	}

	public static List<BigDecimal> storeFactorials(int n){
		List<BigDecimal> factorial = new ArrayList<>();
		factorial.add(BigDecimal.ONE);
		factorial.add(BigDecimal.ONE);
		for (int i = 2; i<=n; i++){
			factorial.add(i,new BigDecimal(i).multiply(factorial.get(i-1),MathContext.DECIMAL128));
		}
		return factorial;
	}

	public static BigDecimal calculateFact(int num){
		BigDecimal fact = BigDecimal.ONE;
		for (int i = 1; i<= num; i++){
			fact = fact.multiply(new BigDecimal(i),MathContext.DECIMAL128);
		}
		return fact;
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
