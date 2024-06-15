package org.vanilladb.core.storage.index.pq;

import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;

import java.lang.reflect.Constructor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Vector;

import javax.management.RuntimeErrorException;

import java.lang.Math;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.sql.distfn.EuclideanFn;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.SearchKey;
import org.vanilladb.core.storage.index.SearchKeyType;
import org.vanilladb.core.storage.index.SearchRange;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.record.RecordPage;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.TransactionMgr;
import org.vanilladb.core.storage.tx.concurrency.ConcurrencyMgr;
import org.vanilladb.core.storage.tx.concurrency.SerializableConcurrencyMgr;
import org.vanilladb.core.util.CoreProperties;

public class ProductQuantization {

    // M = number of subspaces
    // k= number of centroids in each subspces
    // CENTROID_DIMENSION = dimension of the vector

    //info

    /*
     * name of tbl of each codebook shld be ii.indexName() + "codebook" + codebookNum;
     * 

     * 
     */

    private static final String SCHEMA_KEY = "key", SCHEMA_RID_BLOCK = "block",
			SCHEMA_RID_ID = "id", SCHEMA_CODEBOOK = "codebook";

    public static final int NUM_SUBSPACES;
    public static final int NUM_CENTROIDS_IN_SUBSPACE;
    public static final int NUM_ITERATIONS;
    public static final int BEAM_SIZE;
    public static final int CENTROID_DIMENSION;

    private static int numCentroidsInSubspace;
    private static VectorConstant[][] centroidsAllSubspace;

    // get static var from properties (TODO)


    
    private static String keyFieldName(int index) {
		return SCHEMA_KEY + index;
	}
    
    private static Schema schema() {
		Schema sch = new Schema();
		sch.addField(keyFieldName(0), INTEGER);
		sch.addField(SCHEMA_RID_BLOCK, BIGINT);
		sch.addField(SCHEMA_RID_ID, INTEGER);
		return sch;
	}

    private static Schema coodebookSchema() {
		Schema sch = new Schema();
		sch.addField(SCHEMA_CODEBOOK, Type.VECTOR(CENTROID_DIMENSION));
		return sch;
	}

    private RecordFile rf;
    private String dataFileName;
    private SearchKeyType keyType;
	private Transaction tx;
    String tblName;
    TableInfo keyTi;



	private boolean isBeforeFirsted;
	private boolean isBuilding = false;

    
    private static VectorConstant[] currentCodebook;
    // create 2D array of array of curreentCodebook
    private static VectorConstant[][] codebooks;
    

    /**
	 * Opens a IVF-Flat index for the specified index.
	 * 
	 * @param ii
	 *            the information of this index
	 * @param keyType
	 *            the type of the search key
	 * @param tx
	 *            the calling transaction
	 */
	public ProductQuantization(String tblName, TableInfo keyTi, Transaction tx) {

        // find field name from keyTi (reference to IndexMgr)

        this.tblName = "sift.tbl";  // should be changed
		//this.tblName = tblName;
        this.dataFileName = tblName + ".tbl";
        this.keyTi = keyTi;
		this.tx = tx;
        //
			
	}
    public void checkCodebookLoaded(){
        if (codebooks == null ){
            for (int i=0;i < NUM_SUBSPACES; ++i){
                preloadCentroidsInSubspace(i);
            }
        }
    }


    /**
     * @param numSubspace
     *              the subspace to prelaod
     */
    private void preloadCentroidsInSubspace(int numSubspace){   //TBC
        List<VectorConstant> centroidsList = new ArrayList<VectorConstant>();
        String centroidTableName = "codebook" + numSubspace;
        TableInfo centroidTableInfo = VanillaDb.catalogMgr().getTableInfo(centroidTableName, tx);
        if (centroidTableInfo == null) 
			throw new RuntimeException();
        RecordFile centroidRecordFile = centroidTableInfo.open(tx, false);
        centroidRecordFile.beforeFirst();

        while(centroidRecordFile.next()) {
			centroidsList.add((VectorConstant) centroidRecordFile.getVal(SCHEMA_CODEBOOK));
		}
		centroidRecordFile.close();
		numCentroidsInSubspace = centroidsList.size();
		centroidsInSubspace = centroidsList.toArray(new VectorConstant[numCentroidsInSubspace]);
        
    }

    public void buildCodebook() {
        isBuilding = true;
        // Add the field names to the key catalog



		String fldName = "i_emb";
		TableInfo ti = VanillaDb.catalogMgr().getTableInfo(tblName, tx);
		rf = ti.open(tx, false);

        // 1. Read all records into memory
        List<VectorConstant> populationVectors = new ArrayList<VectorConstant>();
		List<RecordId> populationRecordIds = new ArrayList<RecordId>();

		rf.beforeFirst();
		while(rf.next()) {
			populationVectors.add((VectorConstant) rf.getVal(fldName));
			populationRecordIds.add(rf.currentRecordId());
		}
        rf.close();

        
        int dimension = populationVectors.get(0).dimension();
        //shuffle the populationVectors
        int randomSeed = 0;
		Collections.shuffle(populationVectors, new Random(randomSeed));
		Collections.shuffle(populationRecordIds, new Random(randomSeed));

        // perform k-meldoids
        // if length is not divisible by NUM_SUBSPACES, then the last subspace will have more vectors
        int subVectorSize = dimension / NUM_SUBSPACES;
        List<List<VectorConstant>> subVectors = new ArrayList<>();
        for (int i = 0; i < NUM_SUBSPACES; ++i) {
            List<VectorConstant> subSpace = new ArrayList<>();
            for (VectorConstant vector : populationVectors) {
                int start = i* subVectorSize;
                
                int end = (i == NUM_SUBSPACES-1)? Math.max((i+1)* subVectorSize, vector.dimension()) : (i+1)* subVectorSize;
                VectorConstant subVector = vector.slice(start, end);
                subSpace.add(subVector);
            }
            subVectors.add(subSpace);
        }

        // for each subVectors, perform k-meldoids of NUM_ITERATIONS times
        for (int i =0; i < NUM_SUBSPACES; ++i) {
            VectorConstant[] centroids = new VectorConstant[numCentroidsInSubspace];
            List<VectorConstant> subSpace = subVectors.get(i);

            // Initialize centroids here...
        
            for (int it = 0; it < NUM_ITERATIONS; ++it) {
                // Initialize clusters and distance functions
                List<List<Integer>> clusters = new ArrayList<>();
                for (int clusterIndex = 0; clusterIndex < numCentroidsInSubspace; ++clusterIndex) {
                    clusters.add(new ArrayList<Integer>());
                }
                // Assign each sample to the nearest cluster
                for (int sampleIndex = 0; sampleIndex < subSpace.size(); ++sampleIndex) {
                    int bestCentroid = findCentroid(subSpace.get(sampleIndex));
                    clusters.get(bestCentroid).add(sampleIndex);
                }

            }
            centroidsAllSubspace[i] = centroids;       //add centroids of this subspace to  centroidsAllSubspace 

            // Materialize each calculated centroid (todo)
        }




    }

    public void quantizeSearchRange() {// or move it to other class?
        // for now just assume we already have a class to quantiza the search range



    }

    








}