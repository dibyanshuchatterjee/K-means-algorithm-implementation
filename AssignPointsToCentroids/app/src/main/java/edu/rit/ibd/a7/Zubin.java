package edu.rit.ibd.a7;

import ch.obermuhlner.math.big.BigDecimalMath;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Decimal128;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.regex.Pattern;

public class Zubin{
    public enum Distance {Manhattan, Euclidean}
    public enum Mean {Arithmetic, Geometric}

    private static LinkedHashMap<String, ArrayList<BigDecimal>> initCentroidInfo(MongoCollection<Document> collection, LinkedHashMap<String, ArrayList<BigDecimal>> info){
        FindIterable<Document> allCentroids = collection.find(Document.parse("{id:/^c\\.*/}")).batchSize(100);
        for (Document d: allCentroids){
            String id = d.getString("_id");
            info.put(id, new ArrayList<>());
            info.get(id).add(BigDecimal.ZERO);
            info.get(id).add(BigDecimal.ZERO);
        }
        System.out.println("Size of HashMap/(No. of centroids) = " + info.size());
        return info;
    }

    private static Decimal128 computeDistance(Document point, Document centroid, Distance distanceMethod){
        BigDecimal distance = BigDecimal.ZERO;
        Document pointCoordinates = point.get("point", Document.class);
        Document centroidCoordinates = centroid.get("centroid", Document.class);
        if(distanceMethod.equals(Distance.Manhattan)){
            int counter = 0;
            int noOfDimensions=0;
            if(null != pointCoordinates){
                noOfDimensions = pointCoordinates.size();
            }

            while(counter < noOfDimensions){
                BigDecimal dim1 = pointCoordinates.get("dim_"+counter, Decimal128.class).bigDecimalValue();
                BigDecimal dim2 = centroidCoordinates.get("dim_"+counter, Decimal128.class).bigDecimalValue();
                BigDecimal difference = dim1.subtract(dim2, MathContext.DECIMAL128).abs();
                distance = distance.add(difference, MathContext.DECIMAL128);
                counter++;
            }
        }
        else if(distanceMethod.equals(Distance.Euclidean)){
            int counter = 0;
            int noOfDimensions = 0;
            if(null != pointCoordinates){
                noOfDimensions = pointCoordinates.size();
            }
            BigDecimal tempDistance = BigDecimal.ZERO;
            while(counter < noOfDimensions) {
                Decimal128 dim1 = pointCoordinates.get("dim_" + counter, Decimal128.class);
                Decimal128 dim2 = centroidCoordinates.get("dim_" + counter, Decimal128.class);
                BigDecimal difference = dim1.bigDecimalValue().subtract(dim2.bigDecimalValue(), MathContext.DECIMAL128);
                BigDecimal differenceSquared = difference.pow(2);
                tempDistance = tempDistance.add(differenceSquared, MathContext.DECIMAL128);

                counter++;
            }
            distance = tempDistance.sqrt(MathContext.DECIMAL128);

        }
        return new Decimal128(distance);

    }

    private static LinkedHashMap<String, ArrayList<BigDecimal>> assignTotalPointsAndLabels(MongoCollection<Document> collection, LinkedHashMap<String, ArrayList<BigDecimal>> result, Distance distance){
		/*Pattern p1 = Pattern.compile("^c");
		Pattern p2 = Pattern.compile("^p");
		Bson filterPoints = Filters.regex("_id", p1);
		Bson filterCentroids = Filters.regex("_id", p2);*/
        FindIterable<Document> pointDocs = collection.find(Document.parse("{id:/^p\\.*/}")).batchSize(250);

        for(Document doc_p: pointDocs){
            String pointID = doc_p.getString("_id");
            Document temp = collection.find(Document.parse("{id:/^c\\.*/}")).first();
            String holdID = temp.getString("_id");
            Decimal128 holdMinDistance = computeDistance(doc_p, temp, distance);
            FindIterable<Document> centroidDocs = collection.find(Document.parse("{id:/^c\\.*/}")).batchSize(100);
            for(Document doc_c: centroidDocs){
                String centroidID = doc_c.getString("_id");
                Decimal128 currentDistance = computeDistance(doc_p, doc_c, distance);
                if(currentDistance.compareTo(holdMinDistance) < 0){
                    holdMinDistance = currentDistance;
                    holdID = centroidID;
                }
            }
            BigDecimal currentTotalPoints = result.get(holdID).get(0);
            BigDecimal currentSSE = result.get(holdID).get(1);
            currentSSE = currentSSE.add(holdMinDistance.bigDecimalValue().pow(2,MathContext.DECIMAL128), MathContext.DECIMAL128);
            currentTotalPoints = currentTotalPoints.add(BigDecimal.ONE);
            result.get(holdID).set(0, currentTotalPoints);
            result.get(holdID).set(1, currentSSE);
            // update the document with _id = pointID, by adding label
            addLabel(collection, pointID, holdID);
        }
        return result;
    }

    private static void addLabel(MongoCollection<Document> collection, String pid, String cid){
        Bson filter = Filters.eq("_id", pid);
        Bson update = Updates.set("label", cid);
        UpdateOptions options = new UpdateOptions().upsert(true);
        collection.updateOne(filter, update, options);
    }

    private static void updateCentroids(MongoCollection<Document> collection, LinkedHashMap<String, ArrayList<BigDecimal>> requiredInfo){
        Pattern p = Pattern.compile("^c");
        Bson filter = Filters.regex("_id", p);
        FindIterable<Document> allCentroids = collection.find(filter).batchSize(100);
        for(Document currentCentroid: allCentroids){
            String currentCentroidID = currentCentroid.getString("_id");
            int total_points = requiredInfo.get(currentCentroidID).get(0).intValue();
            BigDecimal sse = requiredInfo.get(currentCentroidID).get(1);
            List<Bson> updates = new ArrayList<>();
            Bson update1 = Updates.set("total_points", total_points);
            Bson update2 = Updates.set("sse", sse);
            Bson update3;
            if(total_points <= 0){
                update3 = Updates.set("reinitialize", true);
            }
            else update3 = Updates.set("reinitialize", false);
            updates.add(0, update1);
            updates.add(1, update2);
            updates.add(2, update3);
            Bson update = Updates.combine(updates);
            Bson filterCentroid = Filters.eq("_id", currentCentroidID);
            collection.updateOne(filterCentroid, update);
            //System.out.println(collection.find(filterCentroid).first());

        }
    }

    private static LinkedHashMap<String, BigDecimal> initializeMap(LinkedHashMap<String, BigDecimal> map, int noOfDimensions){
        for(int i = 0; i < noOfDimensions; i++){
            map.put("dim_"+i, BigDecimal.ZERO);
        }
        return map;
    }
    private static LinkedHashMap<String, BigDecimal> updateMap(LinkedHashMap<String, BigDecimal> map,Document pointDimensions, int noOfDimensions, Mean meanType) {
        if(meanType.equals(Mean.Arithmetic)){
            for(int i = 0; i < noOfDimensions; i++){
                BigDecimal newDim = pointDimensions.get("dim_"+i, Decimal128.class).bigDecimalValue();
                map.put("dim_"+i, map.get("dim_"+i).add(newDim, MathContext.DECIMAL128));
            }
        }
        else if(meanType.equals(Mean.Geometric)){
            for(int i = 0; i < noOfDimensions; i++){
                BigDecimal newDim = pointDimensions.get("dim_"+i, Decimal128.class).bigDecimalValue();
                BigDecimal update = BigDecimalMath.log(newDim, MathContext.DECIMAL128);
                map.put("dim_"+i, map.get("dim_"+i).add(update, MathContext.DECIMAL128));
            }
        }
        return map;
    }

    private static void createNewCentroids(MongoCollection<Document> collection, Mean meanType){

        FindIterable<Document> allCentroids = collection.find(Document.parse("{id:/^c\\.*/}"));
        LinkedHashMap<String, BigDecimal> dimensionStorage = new LinkedHashMap<>();
        // Iterating over all centroids.
        for(Document currentCentroid: allCentroids){
            String centroidID = currentCentroid.getString("_id");
            Document dimensions = currentCentroid.get("centroid", Document.class);
            int noOfDimensions = dimensions.size();
            // Checking if the centroid need reinitialization
            if(!currentCentroid.getBoolean("reinitialize")){
                Document newCentroidToAdd = new Document();	// Final centroid document to inserted into collection
                Document newCentroidDimensions = new Document();
                Bson filter = Filters.eq("label", centroidID);
                // Creating a map with keys: dim_[0...noOfDimensions] and values being updated sums of dimensions
                dimensionStorage = initializeMap(dimensionStorage, noOfDimensions);
                FindIterable<Document> allPoints = collection.find(filter).batchSize(250);
                if(meanType.equals(Mean.Arithmetic)){
                    BigDecimal counter = BigDecimal.ZERO;
                    for (Document currentPoint: allPoints){
                        Document pointDimensions = currentPoint.get("point", Document.class);
                        dimensionStorage = updateMap(dimensionStorage, pointDimensions, noOfDimensions, meanType);
                        counter = counter.add(BigDecimal.ONE, MathContext.DECIMAL128);
                    }
                    //Finding the mean of each dimension and adding it to the document.
                    for(int i = 0; i<noOfDimensions; i++){
                        BigDecimal dim_i = dimensionStorage.get("dim_"+i);
                        dim_i = dim_i.multiply(counter.pow(-1, MathContext.DECIMAL128), MathContext.DECIMAL128);
                        newCentroidDimensions.append("dim_"+i, dim_i);
                    }
                    String newCentroidID = "new_" + centroidID;
                    newCentroidToAdd.append("_id", newCentroidID);
                    newCentroidToAdd.append("centroid", newCentroidDimensions);
                    collection.insertOne(newCentroidToAdd);
                }

                else if(meanType.equals(Mean.Geometric)){
                    BigDecimal counter = BigDecimal.ZERO;
                    for (Document currentPoint: allPoints){
                        Document pointDimensions = currentPoint.get("point", Document.class);
                        dimensionStorage = updateMap(dimensionStorage, pointDimensions, noOfDimensions, meanType);
                        counter = counter.add(BigDecimal.ONE, MathContext.DECIMAL128);
                    }
                    //Finding the mean of each dimension and adding it to the document.
                    for(int i = 0; i<noOfDimensions; i++){
                        BigDecimal dim_i = dimensionStorage.get("dim_"+i);
                        dim_i = dim_i.multiply(counter.pow(-1, MathContext.DECIMAL128), MathContext.DECIMAL128);
                        dim_i = BigDecimalMath.exp(dim_i, MathContext.DECIMAL128);
                        newCentroidDimensions.append("dim_"+i, dim_i);
                    }
                    String newCentroidID = "new_" + centroidID;
                    newCentroidToAdd.append("_id", newCentroidID);
                    newCentroidToAdd.append("centroid", newCentroidDimensions);
                    collection.insertOne(newCentroidToAdd);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
//		System.out.println(BigDecimalMath.log(BigDecimal.valueOf(2),new MathContext(100)));
        final String mongoDBURL = args[0];
        final String mongoDBName = args[1];
        final String mongoCol = args[2];
        final Distance distance = Distance.valueOf(args[3]);
        final Mean mean = Mean.valueOf(args[4]);

        MongoClient client = getClient(mongoDBURL);
        MongoDatabase db = client.getDatabase(mongoDBName);

        MongoCollection<Document> collection = db.getCollection(mongoCol);
        collection.createIndex(new Document("_id",1));

        // TODO Your code here!

        /*
         *
         * Points have _id=p_XYZ and centroids have _id=c_ABC. A new centroid derived from an existing centroid c_i must have _id=new_c_i.
         *
         * To perform one epoch, each point should be assigned to the closest centroid using the input distance (Manhattan or Euclidean).
         * 	That is, if point p_i has the minimum distance to centroid c_j, p_i.label = c_j.
         *
         * Once the point assignment has been made, SSE is the sum of the square distance between point and centroid. Each centroid c_i
         * 	will contain a field sse that will store the SSE of all the points assigned to it. Furthermore, you need to include a field
         * 	with the total count of points assigned to the centroid (total_points), and whether must be reinitialized (reinitialize).
         *
         * A centroid must be reinitialized at the end of the epoch if it has no points assigned to it.
         *
         * Each new centroid is derived from a centroid that must not be reinitialized. A centroid that must be reinitialized has no
         * 	points assigned to it, so it is not possible to compute a new centroid. Assuming c_i is a centroid with points assigned, the
         * 	new centroid new_c_i is the (arithmetic or geometric) mean of all the points assigned to it. That is, for each dimension k,
         * 	the dimension k of new_c_i is the mean of all the dimensions k of the points assigned to c_i.
         *
         */
        LinkedHashMap<String, ArrayList<BigDecimal>> centroidInfo = new LinkedHashMap<>();
        centroidInfo = initCentroidInfo(collection, centroidInfo);
        centroidInfo = assignTotalPointsAndLabels(collection, centroidInfo, distance);
        updateCentroids(collection, centroidInfo);
        createNewCentroids(collection, mean);


        // TODO End of your code!

        client.close();
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