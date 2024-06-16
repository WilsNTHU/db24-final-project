package org.vanilladb.core.storage.index.ivfflat;

import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;

import java.lang.reflect.Constructor;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.stream.IntStream;

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
import org.vanilladb.core.util.CoreProperties;

/**
 * A IVF-Flat implementation of {@link Index}. Building the index will utilize K-Means
 * algorithm to cluster the vectors into several index represented by {@link RecordFile}.
 * 
 * Note: It physically stores the centroidId as the {@link SearchKey} but accepts both 
 * 		 centroidIds and {@link VectorConstant} as {@link SearchRange}.
 */
public class IVFFlatIndex extends Index {
	/**
	 * A field name of the schema of index records/
	 */
	private static final String SCHEMA_KEY = "key", SCHEMA_RID_BLOCK = "block",
			SCHEMA_RID_ID = "id", SCHEMA_CENTROID = "centroid";
	// Hyperparameters
	public static final int NUM_CENTROIDS;
	public static final int NUM_SAMPLES_PER_CENTROIDS;
	public static final int NUM_ITERATIONS;
	public static final int BEAM_SIZE;
	public static final int CENTROID_DIMENSION;
	// Distance function
	private static final Class<?> DIST_FN_CLS; 

	private static int numCentroids;
	private static VectorConstant[] centroids;

	static {
		NUM_CENTROIDS = CoreProperties.getLoader().getPropertyAsInteger(
				IVFFlatIndex.class.getName() + ".NUM_CENTROIDS", 950
		);
		NUM_SAMPLES_PER_CENTROIDS = CoreProperties.getLoader().getPropertyAsInteger(
			IVFFlatIndex.class.getName() + ".NUM_SAMPLES_PER_CENTROID", 50
		);
		NUM_ITERATIONS = CoreProperties.getLoader().getPropertyAsInteger(
			IVFFlatIndex.class.getName() + ".NUM_ITERATIONS", 10
		);
		BEAM_SIZE = CoreProperties.getLoader().getPropertyAsInteger(
			IVFFlatIndex.class.getName() + ".BEAM_SIZE", 1
		);
		CENTROID_DIMENSION = CoreProperties.getLoader().getPropertyAsInteger(
			IVFFlatIndex.class.getName() + ".CENTROID_DIMENSION",128
		);
		DIST_FN_CLS = CoreProperties.getLoader().getPropertyAsClass(
			IVFFlatIndex.class.getName() + ".DIST_FN_CLS", EuclideanFn.class,
			DistanceFn.class
		);
	}

	public static long searchCost(SearchKeyType keyType, long totRecs, long matchRecs) {
		int rpb = Buffer.BUFFER_SIZE / RecordPage.slotSize(schema());
		return (totRecs / rpb) / NUM_CENTROIDS;
	}

	private static String keyFieldName(int index) {
		return SCHEMA_KEY + index;
	}

	/**
	 * Returns the schema of the index records.
	 * 
	 * @param fldType
	 *	 			the type of the indexed field
	 * @return the schema of the index records
	 */
	private static Schema schema() {
		Schema sch = new Schema();
		sch.addField(keyFieldName(0), INTEGER);
		sch.addField(SCHEMA_RID_BLOCK, BIGINT);
		sch.addField(SCHEMA_RID_ID, INTEGER);
		return sch;
	}

	private static Schema centroidSchema() {
		Schema sch = new Schema();
		sch.addField(SCHEMA_CENTROID, Type.VECTOR(CENTROID_DIMENSION));
		return sch;
	}

	private class Pair {
		double key;
		int value;
		public Pair(double key, int value) {
			this.key = key;
			this.value = value;
		}
		public double getKey() {
			return key;
		}
		public int getValue() {
			return value;
		}
	}

	private int currentCentroid;
	private RecordFile rf;
	private boolean isBeforeFirsted;
	private boolean isBuilding = false;
	private Deque<Integer> centroidQueue = new ArrayDeque<>();

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
	public IVFFlatIndex(IndexInfo ii, SearchKeyType keyType, Transaction tx) {
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
	public void preLoadToMemory() {
		for (int i = 0; i < NUM_CENTROIDS; i++) {
			String tblname = ii.indexName() + i + ".tbl";
			long size = fileSize(tblname);
			BlockId blk;
			for (int j = 0; j < size; j++) {
				blk = new BlockId(tblname, j);
				tx.bufferMgr().pin(blk);
			}
		}
    }

	private void loadCentroidsToMemory() {
		List<VectorConstant> centroidsList = new ArrayList<VectorConstant>();
		// Materialize each calculated centroid
		String centroidTableName = ii.indexName() + "centroid";
		TableInfo centroidTableInfo = VanillaDb.catalogMgr().getTableInfo(centroidTableName, tx);
		if (centroidTableInfo == null) 
			throw new RuntimeException();
		RecordFile centroidRecordFile = centroidTableInfo.open(tx, false);
		centroidRecordFile.beforeFirst();
		while(centroidRecordFile.next()) {
			centroidsList.add((VectorConstant) centroidRecordFile.getVal(SCHEMA_CENTROID));
		}
		centroidRecordFile.close();
		numCentroids = centroidsList.size();
		centroids = centroidsList.toArray(new VectorConstant[numCentroids]);
	}	

	public void buildIndex(int limit) {
		isBuilding = true;
		System.out.println("Built index based on IndexInfo: " + ii);
		String tblName = ii.tableName();
		List<String> fldList = ii.fieldNames();
		TableInfo ti = VanillaDb.catalogMgr().getTableInfo(tblName, tx);
		rf = ti.open(tx, false);
		// Read all records into memory
		List<VectorConstant> populationVectors = new ArrayList<VectorConstant>();
		List<RecordId> populationRecordIds = new ArrayList<RecordId>();
		String fldName = fldList.get(0);
		int populationCount = 0;
		rf.beforeFirst();
		while(rf.next()) {
			++populationCount;
			populationVectors.add((VectorConstant) rf.getVal(fldName));
			populationRecordIds.add(rf.currentRecordId());
		}
        rf.close();
		// Warning: may fail!
		int dimension = populationVectors.get(0).dimension();
		// Optimization: Sampling records to perform K-Means (saves time!)
		long totalSamples = Math.min(NUM_SAMPLES_PER_CENTROIDS * NUM_CENTROIDS, populationVectors.size());
		List<Integer> shuffledIndex = new ArrayList<Integer>();
		for (int i = 0; i < totalSamples; ++i)
			shuffledIndex.add(i);
		Collections.shuffle(shuffledIndex, new Random(0));
		List<VectorConstant> samples = new ArrayList<>();
		for (int sampleIdx = 0; sampleIdx < totalSamples; ++sampleIdx)
			samples.add(populationVectors.get(shuffledIndex.get(sampleIdx)));
		// Initialize centroids
		numCentroids = Math.min(Math.toIntExact(totalSamples), NUM_CENTROIDS);
		// Centroids are initialized from existing vectors
		centroids = samples.subList(0, numCentroids).toArray(new VectorConstant[numCentroids]);
		System.out.println("There are " + totalSamples + " samples.");
		// Adjust centroids through iterations
		for (int it = 0; it < NUM_ITERATIONS; ++it) {
			// Initialize clusters and distance functions
			List<List<Integer>> clusters = new ArrayList<>();
			for (int clusterIndex = 0; clusterIndex < numCentroids; ++clusterIndex) {
				clusters.add(new ArrayList<Integer>());
			}
			// Assign each sample to the nearest cluster
			for (int sampleIndex = 0; sampleIndex < samples.size(); ++sampleIndex) {
				int bestCentroid = findCentroid(samples.get(sampleIndex));
				clusters.get(bestCentroid).add(sampleIndex);
			}
			// Adjust centroids
			for (int centroidIndex = 0; centroidIndex < numCentroids; ++centroidIndex) {
				VectorConstant meanCentroid = VectorConstant.zeros(dimension);
				int clusterSize = clusters.get(centroidIndex).size();
				for (Integer sampleIndex : clusters.get(centroidIndex))
					meanCentroid = (VectorConstant) meanCentroid.add(samples.get(sampleIndex));
				meanCentroid = (VectorConstant) meanCentroid.div(new IntegerConstant(clusterSize));
				centroids[centroidIndex] = meanCentroid;
			}
		}
		// Assign each record to the nearest centroid
		List<List<Integer>> clusters = new ArrayList<>();
		for (int clusterIndex = 0; clusterIndex < numCentroids; ++clusterIndex) {
			clusters.add(new ArrayList<Integer>());
		}
		for (int populationIndex = 0; populationIndex < populationVectors.size(); ++populationIndex) {
			int centroidId = findCentroid(populationVectors.get(populationIndex));
			clusters.get(centroidId).add(populationIndex);
		}
		Set<Integer> removedCentroids = new HashSet<Integer>();
		for (int centroidId = 0; centroidId < numCentroids; ++centroidId) {
			if (clusters.get(centroidId).size() < limit) {
				removedCentroids.add(centroidId);
				for (int movedPopulationIndex : clusters.get(centroidId)) {
					int targetCentroidId = findCentroid(populationVectors.get(movedPopulationIndex), removedCentroids);
					clusters.get(targetCentroidId).add(movedPopulationIndex);
				}
			}
		}
		int left = 0, right = 0;
		for (; right < numCentroids; ++right) {
			if (removedCentroids.contains(right))
				continue;
			clusters.set(left, clusters.get(right));
			centroids[left++] = centroids[right];
		}
		numCentroids = left;
		// Insert BlockId and RecordId into centroid inverted index files
		for (int centroidId = 0; centroidId < numCentroids; ++centroidId) {
			System.out.println("centroid " + centroidId + " contains " + clusters.get(centroidId).size() + " elements.");
			String clusterName = ii.indexName() + centroidId;
			TableInfo clusterTableInfo = new TableInfo(clusterName, schema());
			RecordFile clusterRecordFile = clusterTableInfo.open(tx, false);
			// Initialize the file header if needed
			if (clusterRecordFile.fileSize() == 0)
				RecordFile.formatFileHeader(clusterTableInfo.fileName(), tx);
			clusterRecordFile.beforeFirst();
			for (int populationIndex : clusters.get(centroidId)) {
				clusterRecordFile.insert();
				clusterRecordFile.setVal(
					SCHEMA_RID_BLOCK, 
					new BigIntConstant(
						populationRecordIds.get(populationIndex).block().number()
					)
				);
				clusterRecordFile.setVal(
					SCHEMA_RID_ID,
					new IntegerConstant(
						populationRecordIds.get(populationIndex).id()
					)
				);
			}
			clusterRecordFile.close();
		}
		// Materialize each calculated centroid
		String centroidTableName = ii.indexName() + "centroid";
		TableInfo centroidTableInfo = VanillaDb.catalogMgr().getTableInfo(centroidTableName, tx);
		if (centroidTableInfo == null) {
			VanillaDb.catalogMgr().createTable(centroidTableName, centroidSchema(), tx);
			centroidTableInfo = VanillaDb.catalogMgr().getTableInfo(centroidTableName, tx);
		}
		RecordFile centroidRecordFile = centroidTableInfo.open(tx, false);
		for (int centroidId = 0; centroidId < numCentroids; ++centroidId) {
			centroidRecordFile.insert();
			centroidRecordFile.setVal(SCHEMA_CENTROID, centroids[centroidId]);
		}
		centroidRecordFile.close();
		System.out.println("Number of centroids " + numCentroids);
		isBuilding = false;
	}

    /**
	 * Calculates the n-th nearest centroid and positions the index before the
	 * first index record corresponding to the n-th nearest centroids. The hyperparameter
	 * n is defined by the beam size.
	 * 
	 * @param searchRange
	 *            the range of search keys
	 */
    @Override
	public void beforeFirst(SearchRange searchRange) {
		close();
		if (!isBuilding && centroids == null)
			loadCentroidsToMemory();
		if (!searchRange.isSingleValue())
			throw new UnsupportedOperationException();
		if (centroidQueue.size() != 0) {
			centroidQueue.clear();
		}
		processMultipleSearchRange(searchRange);
		currentCentroid = centroidQueue.poll();
		// Open corresponding centroid record file
		String tblName = ii.indexName() + currentCentroid;
		TableInfo ti = new TableInfo(tblName, schema());
		this.rf = ti.open(tx, false);
		// Initialize the file header if needed
		if (rf.fileSize() == 0)
			RecordFile.formatFileHeader(ti.fileName(), tx);
		rf.beforeFirst();
		isBeforeFirsted = true;
    }

	/**
	 * Positions the index before the first index record having the specified
	 * search key. The method uses the search key to find the best matching
	 * centroid, and then opens a {@link RecordFile} on the file corresponding 
	 * to the bucket. The record file for the previous bucket (if any) is closed.
	 * 
	 * @param searchRange
	 *            the range of search keys
	 */
	private void beforeFirstSingular(SearchRange searchRange) {
		close();
		if (!isBuilding && centroids == null)
			loadCentroidsToMemory();
		if (!searchRange.isSingleValue())
			throw new UnsupportedOperationException();
		currentCentroid = processSearchRange(searchRange);
		// Open corresponding centroid record file
		String tblName = ii.indexName() + currentCentroid;
		TableInfo ti = new TableInfo(tblName, schema());
		this.rf = ti.open(tx, false);
		// Initialize the file header if needed
		if (rf.fileSize() == 0)
			RecordFile.formatFileHeader(ti.fileName(), tx);
		rf.beforeFirst();
		isBeforeFirsted = true;
    }

	/**
	 * Converts the search range into a Long object corresponding to a centroid ID.
	 * 
	 * @param searchRange
	 * 				The search range of the query
	 * 				The search range could be of two types:
	 * 				[1] CentroidId: representing the centroid of the query vector
	 * 				[2] VectorConstant: representing the query vector
	 * 
	 * @return the centroid index of the corresponding search range
	 */
	private int processSearchRange(SearchRange searchRange) {
		Constant searchObject = searchRange.asSearchKey().get(0);
		// The Constant object is of Long type
		if (searchObject.getType() == Type.INTEGER)
			return (int) searchObject.asJavaVal();
		// The Constant object is of VectorConstant type
		return findCentroid((VectorConstant) searchObject);
	}

	/**
	 * Enqueues the n-th nearest centroid to the query vector. The hyperparameter
	 * n is defined by the beam size.
	 * 
	 * Note: Assumes that the centroid queue is initially empty.
	 * 
	 * @param searchRange
	 * @return
	 */
	private void processMultipleSearchRange(SearchRange searchRange) {
		Constant searchObject = searchRange.asSearchKey().get(0);
		// The Constant object is of Long type
		if (searchObject.getType() == Type.INTEGER)
			centroidQueue.add((int) searchObject.asJavaVal());
		// The Constant object is of VectorConstant type
		PriorityQueue<Pair> pq = new PriorityQueue<>((a, b) -> (int)(b.getKey() - a.getKey()));
		DistanceFn distFn = getDistFn((VectorConstant) searchObject);
		for (int centroidIndex = 0; centroidIndex < numCentroids; ++centroidIndex) {
			double currentDistance = distFn.distance(centroids[centroidIndex]);
			if ((pq.size() >= BEAM_SIZE) && (currentDistance >= pq.peek().getKey()))
				continue;
			if (pq.size() >= BEAM_SIZE)
				pq.poll();
			pq.add(new Pair(currentDistance, centroidIndex));
		}
		// Get centorid queue
		for (Pair p : pq)
			centroidQueue.add(p.getValue());
	}

	/**
	 * Moves the index to the next record having the search key
	 * 
	 * @return false if no other index records for the search range.
	 */
    @Override
	public boolean next() {
        if (!isBeforeFirsted)
			throw new IllegalStateException("You must call beforeFirst() before iterating index '"
				+ ii.indexName() + "'");
		while (rf.next())
			return true;
		// Open next centroid file if 
		if (!centroidQueue.isEmpty()) {
			currentCentroid = centroidQueue.poll();
			// Open corresponding centroid record file
			String tblName = ii.indexName() + currentCentroid;
			TableInfo ti = new TableInfo(tblName, schema());
			this.rf = ti.open(tx, false);
			// Initialize the file header if needed
			if (rf.fileSize() == 0)
				RecordFile.formatFileHeader(ti.fileName(), tx);
			rf.beforeFirst();
			while(rf.next())
				return true;
		}
		return false;
    }

	/**
	 * Returns the data record ID from the current index record.
	 * 
	 * @return the data record ID stored in the current index record.
	 */
    @Override
	public RecordId getDataRecordId() {
		long blkNum = (Long) rf.getVal(SCHEMA_RID_BLOCK).asJavaVal();
		int id = (Integer) rf.getVal(SCHEMA_RID_ID).asJavaVal();
		return new RecordId(new BlockId(dataFileName, blkNum), id);
	}

	/**
	 * Inserts an index record having the specified key and data record ID.
	 * 
	 * @param key
	 *            the key in the new index record.
	 * @param dataRecordId
	 *            the data record ID in the new index record.
	 * @param doLogicalLogging
	 *            is logical logging enabled
	 */
    @Override
	public void insert(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
		// Search the position
		beforeFirstSingular(new SearchRange(key));
		// Insert the data
		rf.insert();
		// Optimization: store the search key as the centroidId (saves space!)
		key = new SearchKey(new IntegerConstant(currentCentroid));
		for (int i = 0; i < keyType.length(); ++i)
			rf.setVal(keyFieldName(i), key.get(i));
		rf.setVal(SCHEMA_RID_BLOCK, new BigIntConstant(dataRecordId.block().number()));
		rf.setVal(SCHEMA_RID_ID, new IntegerConstant(dataRecordId.id()));
		close();
	}

	/**
	 * Deletes the index record having the specified key and data record ID.
	 * 
	 * @param key
	 *            the key of the deleted index record
	 * @param dataRecordId
	 *            the data record ID of the deleted index record
	 * @param doLogicalLogging
	 *            is logical logging enabled
	 */
    @Override
	public void delete(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
		// Search the position
		beforeFirstSingular(new SearchRange(key));
		// Delete the specified entry
		while (next())
			if (getDataRecordId().equals(dataRecordId)) {
				rf.delete();
				close();
				return;
			}
		close();
    }

	/**
	 * Closes the index.
	 */
	public void close() {
		if (rf != null)
			rf.close();
    }

	private long fileSize(String fileName) {
		tx.concurrencyMgr().readFile(fileName);
		return VanillaDb.fileMgr().size(fileName);
	}

	/**
	 * Returns the centroid that best matches the query vector
	 * 
	 * @param queryVector
	 *            the query vector
	 * @return the 
	 */
	private int findCentroid(VectorConstant queryVector) {
		int bestCentroid = 0;
		double bestDistance = Double.POSITIVE_INFINITY;
		DistanceFn distFn = getDistFn(queryVector);
		for (int centroidIndex = 0; centroidIndex < numCentroids; ++centroidIndex) {
			double currentDistance = distFn.distance(centroids[centroidIndex]);
			if (currentDistance < bestDistance) {
				bestCentroid = centroidIndex;
				bestDistance = currentDistance;
			}
		}
		return bestCentroid;
	}

	/**
	 * Returns the centroid that best matches the query vector, except the 
	 * specified centroid
	 * 
	 * @param queryVector
	 *            the query vector
	 * @param excludeId
	 * 			  the index of excluded centroid
	 * @return the index of the best matching centroid
	 */
	private int findCentroid(VectorConstant queryVector, Set<Integer> excludeId) {
		int bestCentroid = 0;
		double bestDistance = Double.POSITIVE_INFINITY;
		DistanceFn distFn = getDistFn(queryVector);
		for (int centroidIndex = 0; centroidIndex < numCentroids; ++centroidIndex) {
			if (excludeId.contains(centroidIndex))
				continue;
			double currentDistance = distFn.distance(centroids[centroidIndex]);
			if (currentDistance < bestDistance) {
				bestCentroid = centroidIndex;
				bestDistance = currentDistance;
			}
		}
		return bestCentroid;
	}

	/**
	 * Returns the 
	 * @param queryVector
	 * @return the distance function with the query vector set
	 */
	private DistanceFn getDistFn(VectorConstant queryVector) {
		DistanceFn fn = null;
		try {
			// Parameter types
			Class<?> cls[] = new Class[] {String.class};
			Constructor<?> ct = DIST_FN_CLS.getConstructor(cls);
			fn = (DistanceFn) ct.newInstance(ii.fieldNames().get(0));
			fn.setQueryVector(queryVector);
		} catch(Exception e) {
			System.out.println(e);
		}
		return fn;
	}
}
