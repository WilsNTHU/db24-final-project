package org.vanilladb.core.storage.index.ivfflat;

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

public class PQIndex extends Index {

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
    private static VectorConstant[] centroidsInSubspace;

    // get static var from properties (TODO)


    public static long searchCost(SearchKeyType keyType, long totRecs, long matchRecs) {
		int rpb = Buffer.BUFFER_SIZE / RecordPage.slotSize(schema());
		return (totRecs / rpb) / NUM_SUBSPACES;  // not sure ?? TBC
	}
    
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
	private boolean isBeforeFirsted;
	private boolean isBuilding = false;

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
	public PQIndex(IndexInfo ii, SearchKeyType keyType, Transaction tx) {
		super(ii, keyType, tx);
		// Sanity check
		List<String> fldList = ii.fieldNames();
		if (fldList.size() > 1)
			throw new UnsupportedOperationException();
	}

    /**
	 * Preload the index blocks to memory.
	 */
    @Override
    public void preLoadToMemory() {  // this is useless????

        for(int i = 0;i< NUM_SUBSPACES; i++){
            for(int j =0;j< NUM_CENTROIDS_IN_SUBSPACE;j++){
                // dummy 
            }
        }
    }

    /**
     * @param numSubspace
     *              the subspace to prelaod
     */
    private void preloadCentroidsInSubspace(int numSubspace){
        List<VectorConstant> centroidsList = new ArrayList<VectorConstant>();
        String centroidTableName = ii.indexName() + "codebook" + numSubspace;
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








}