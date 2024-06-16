package org.vanilladb.core.storage.index.ivfflat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.SearchKey;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.util.CoreProperties;

public class ProductQuantizationMgr {
    // Hyperparameters
    public static final int NUM_SUBSPACES;
	public static final int NUM_CLUSTERS_PER_SUBSPACE;
	public static final int NUM_DIMENSION;
    public static boolean isCodeBooksGenerated;

    private int NUM_SUBSPACE_DIMENSION; // Dimensionality of each subspace
    private float[][][] codebooks; // Codebooks for each subspace
    private int[][] encodedVectors; // Encoded vectors

    static {
		NUM_SUBSPACES = CoreProperties.getLoader().getPropertyAsInteger(
				ProductQuantizationMgr.class.getName() + ".NUM_SUBSPACES", 4
		);
		NUM_CLUSTERS_PER_SUBSPACE = CoreProperties.getLoader().getPropertyAsInteger(
			ProductQuantizationMgr.class.getName() + ".NUM_CLUSTERS_PER_SUBSPACE", 200
		);
		NUM_DIMENSION = CoreProperties.getLoader().getPropertyAsInteger(
			ProductQuantizationMgr.class.getName() + ".NUM_DIMENSION", 128
		);
	}

    public ProductQuantizationMgr() {
        System.err.println("pqMgr initialized.");
        this.NUM_SUBSPACE_DIMENSION = NUM_DIMENSION / NUM_SUBSPACES;
        this.codebooks = new float[NUM_SUBSPACES][NUM_CLUSTERS_PER_SUBSPACE][NUM_SUBSPACE_DIMENSION];
        ProductQuantizationMgr.isCodeBooksGenerated = true;
    }



    // Split the vector into NUM_SUBSPACES subspaces
    private float[][] splitIntoSubspaces(VectorConstant vector) {
        float[] vector_vals = vector.asJavaVal();
        int NUM_SUBSPACE_DIMENSION = vector.dimension() / NUM_SUBSPACES;
        float[][] subspaces = new float[NUM_SUBSPACES][NUM_SUBSPACE_DIMENSION];
        for (int m = 0; m < NUM_SUBSPACES; m++) {
            System.arraycopy(vector_vals, m * NUM_SUBSPACE_DIMENSION, subspaces[m], 0, NUM_SUBSPACE_DIMENSION);
        }
        return subspaces;
    }

    // Compute the distance of two input VectorConstants
    private float computeDistance(VectorConstant v1, VectorConstant v2) {
		float distance = 0;
		VectorConstant subtractedVector = (VectorConstant) v1.sub(v2);
		for (int i = 0; i < subtractedVector.dimension(); ++i) {
			distance += subtractedVector.get(i) * subtractedVector.get(i);
		}
		return (float) Math.sqrt(distance);
	}

    // Train the codebooks using k-means clustering
    public void train(ArrayList<VectorConstant> vectors, Transaction tx) {
        isCodeBooksGenerated = false;
        Random rand = new Random();
        for (int m = 0; m < NUM_SUBSPACES; m++) {
            float[][] subspaceData = new float[vectors.size()][NUM_SUBSPACE_DIMENSION];
            for (int i = 0; i < vectors.size(); i++) {
                subspaceData[i] = splitIntoSubspaces(vectors.get(i))[m];
            }
            // NUM_CLUSTERS_PER_SUBSPACE-means clustering (simplified)
            for (int k = 0; k < NUM_CLUSTERS_PER_SUBSPACE; k++) {
                codebooks[m][k] = subspaceData[rand.nextInt(subspaceData.length)];
            }
            boolean changed;
            do {
                int[] labels = new int[subspaceData.length];
                for (int i = 0; i < subspaceData.length; i++) {
                    labels[i] = findNearestCluster(codebooks[m], subspaceData[i]);
                }
                float[][] newCenters = new float[NUM_CLUSTERS_PER_SUBSPACE][NUM_SUBSPACE_DIMENSION];
                int[] counts = new int[NUM_CLUSTERS_PER_SUBSPACE];
                for (int i = 0; i < subspaceData.length; i++) {
                    int label = labels[i];
                    for (int d = 0; d < NUM_SUBSPACE_DIMENSION; d++) {
                        newCenters[label][d] += subspaceData[i][d];
                    }
                    counts[label]++;
                }
                changed = false;
                for (int k = 0; k < NUM_CLUSTERS_PER_SUBSPACE; k++) {
                    if (counts[k] > 0) {
                        for (int d = 0; d < NUM_SUBSPACE_DIMENSION; d++) {
                            newCenters[k][d] /= counts[k];
                        }
                        if (!Arrays.equals(codebooks[m][k], newCenters[k])) {
                            changed = true;
                            codebooks[m][k] = newCenters[k];
                        }
                    }
                }
            } while (changed);
        }

        writeCodeBooksToMemory(tx);
    }

    private void writeCodeBooksToMemory(Transaction tx){
        for(int i=0; i<NUM_SUBSPACES; i++){
            int index = 1;
            String tblName = "CodeBooks" + i;
            TableInfo ti = VanillaDb.catalogMgr().getTableInfo(tblName, tx);
            if (ti == null) {
                VanillaDb.catalogMgr().createTable(tblName, codeBookSchema(), tx);
                ti = VanillaDb.catalogMgr().getTableInfo(tblName, tx);
            }
            RecordFile rf = ti.open(tx, false);
		    rf.beforeFirst();
            for(int k=0; k<NUM_CLUSTERS_PER_SUBSPACE; k++){
                rf.insert();
                rf.setVal("i_id", new IntegerConstant(index));
                rf.setVal("i_emb", new VectorConstant(codebooks[i][k]));
                index++;
            }
            rf.close();
        }
    }

    private void loadCodeBooksToMemory(Transaction tx){
        for(int i=0; i<NUM_SUBSPACES; i++){
            String tblName = "CodeBooks" + i;
            TableInfo ti = VanillaDb.catalogMgr().getTableInfo(tblName, tx);
            RecordFile rf = ti.open(tx, false);
		    rf.beforeFirst();
            int k = 0;
            while(rf.next()){
                this.codebooks[i][k++] = ((VectorConstant) rf.getVal("i_emb")).asJavaVal();
            }
            rf.close();
        }
        System.out.println("Load codeBook into memory succeed");
    }

    public Schema codeBookSchema(){
        Schema sch = new Schema();
		sch.addField("i_id", Type.INTEGER);
		sch.addField("i_emb", Type.VECTOR(NUM_SUBSPACE_DIMENSION));
		return sch;
    }

    // Find the nearest cluster center
    private int findNearestCluster(float[][] codebook, float[] vector) {
        int nearest = -1;
        double minDist = Float.MAX_VALUE;
        for (int k = 0; k < codebook.length; k++) {
            double dist = distance(codebook[k], vector);
            if (dist < minDist) {
                minDist = dist;
                nearest = k;
            }
        }
        return nearest;
    }

    // Euclidean distance
    private double distance(float[] a, float[] b) {
        float sum = 0;
        for (int i = 0; i < a.length; i++) {
            sum += Math.pow(a[i] - b[i], 2);
        }
        return Math.sqrt(sum);
    }

    // Encode vectors using the trained codebooks
    public void encodeVectors(ArrayList<VectorConstant> vectors) {
        encodedVectors = new int[vectors.size()][NUM_SUBSPACES];
        for (int i = 0; i < vectors.size(); i++) {
            float[][] subspaces = splitIntoSubspaces(vectors.get(i));
            for (int m = 0; m < NUM_SUBSPACES; m++) {
                encodedVectors[i][m] = findNearestCluster(codebooks[m], subspaces[m]);
            }
        }
    }

    // Encode vectors using the trained codebooks
    public VectorConstant encodeVector(VectorConstant vector, Transaction tx) {
        if(isCodeBooksGenerated) loadCodeBooksToMemory(tx);
        float[] labels = new float[NUM_SUBSPACES];
        float[][] subspaces = splitIntoSubspaces(vector);
        for (int m = 0; m < NUM_SUBSPACES; m++) {
            labels[m] = findNearestCluster(codebooks[m], subspaces[m]);
        }

        return new VectorConstant(labels);
    }

    public VectorConstant getDecodedVector(VectorConstant encodedVectors, Transaction tx){
        if(isCodeBooksGenerated) loadCodeBooksToMemory(tx);
        float[] pqKeys = encodedVectors.asJavaVal();
        float[][] vals = new float[NUM_SUBSPACES][NUM_SUBSPACE_DIMENSION];
        for(int m=0; m < NUM_SUBSPACES; m++){
             vals[m] = codebooks[m][(int) pqKeys[m]]; 
        }

        float[] vector = new float[NUM_DIMENSION];
        int index = 0;
        for(int i=0; i<NUM_SUBSPACES; i++)
            for(int j=0; j<NUM_SUBSPACE_DIMENSION; j++){
                vector[index++] = vals[i][j];
            }

        return new VectorConstant(vector);
    }


    // Search for the nearest neighbor
    public int search(VectorConstant query) {
        float minDist = Float.MAX_VALUE;
        int nearestIndex = -1;
        float[][] querySubspaces = splitIntoSubspaces(query);
        for (int i = 0; i < encodedVectors.length; i++) {
            float dist = 0;
            for (int m = 0; m < NUM_SUBSPACES; m++) {
                dist += distance(codebooks[m][encodedVectors[i][m]], querySubspaces[m]);
            }
            if (dist < minDist) {
                minDist = dist;
                nearestIndex = i;
            }
        }
        return nearestIndex;
    }

    public boolean isCodeBooksGenerated(){
        return isCodeBooksGenerated;
    }
}
