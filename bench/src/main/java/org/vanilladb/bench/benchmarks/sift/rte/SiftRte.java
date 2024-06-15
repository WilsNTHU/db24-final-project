package org.vanilladb.bench.benchmarks.sift.rte;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.benchmarks.sift.SiftBenchConstants;
import org.vanilladb.bench.benchmarks.sift.SiftTransactionType;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.remote.sp.VanillaDbSpResultSet;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;
import org.vanilladb.bench.rte.TransactionExecutor;
import org.vanilladb.bench.rte.TxParamGenerator;
import org.vanilladb.bench.util.RandomValueGenerator;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.Record;

public class SiftRte extends RemoteTerminalEmulator<SiftTransactionType> {

    private SiftTxExecutor executor;
    private static final int precision = 100;

    static Map<VectorConstant, Set<Integer>> resultMap = new ConcurrentHashMap<>();
    static Map<VectorConstant, Integer> insertMap = new ConcurrentHashMap<>();
    static CopyOnWriteArrayList<Object[]> insertHistory = new CopyOnWriteArrayList<>();

    public SiftRte(SutConnection conn, StatisticMgr statMgr, long sleepTime) {
        super(conn, statMgr, sleepTime);
    }

    @Override
    protected SiftTransactionType getNextTxType() {
        RandomValueGenerator rvg = new RandomValueGenerator();

        int flag = (int) (SiftBenchConstants.READ_INSERT_TX_RATE * precision);

        if (rvg.number(0, precision - 1) < flag) {
            return SiftTransactionType.ANN;
        }

        return SiftTransactionType.INSERT;
    }

    @Override
    protected TransactionExecutor<SiftTransactionType> getTxExeutor(SiftTransactionType type) {
        TxParamGenerator<SiftTransactionType> paramGen;
        switch(type) {
            case ANN:
                paramGen = new SiftParamGen();
                break;
            case INSERT:
                paramGen = new SiftInsertParamGen();
                break;
            default:
                paramGen = new SiftParamGen();
                break;
        }
        executor = new SiftTxExecutor(paramGen, resultMap, insertMap, insertHistory);
        return executor;
    }

    public void executeCalculateRecall(SutConnection conn) throws SQLException {
        List<Double> recallList = new ArrayList<>();
        List<Map.Entry<VectorConstant, Integer>> insertMapList = new ArrayList<>(insertMap.entrySet());
        insertMapList.sort(Map.Entry.comparingByValue());

        // System.out.println("insertHistory: ");
        // for (Object[] array : insertHistory) {
        //     for (Object obj : array) {
        //         System.out.print(obj + " ");
        //     }
        //     System.out.println();
        // }
        // System.out.println("==========");

        for (Map.Entry<VectorConstant, Integer> entry : insertMapList) {
            VectorConstant query = entry.getKey();
            int insertCount = entry.getValue();
            Set<Integer> approximateNeighbors = resultMap.get(query);

            ArrayList<Object> paramList = new ArrayList<>();
            paramList.add(SiftBenchConstants.NUM_DIMENSION);
            for (int i = 0; i < SiftBenchConstants.NUM_DIMENSION; i++) {
                paramList.add(query.get(i));
            }
            paramList.add(insertCount);
            for (int i = 0; i < insertCount; i++) {
                Object[] insertParam = insertHistory.get(i);
                for (Object obj : insertParam) {
                    paramList.add(obj);
                }
            }

            // System.out.println("paramList: " + paramList);

            VanillaDbSpResultSet recallResultSet = (VanillaDbSpResultSet) conn.callStoredProc(SiftTransactionType.CALCULATE_RECALL.getProcedureId(), paramList.toArray());
            Schema sch = recallResultSet.getSchema();
            Record rec = recallResultSet.getRecords()[0];

            Set<Integer> trueNeighbors = new HashSet<>();

            for (String fld : sch.fields()) {
                if (fld.equals("rc")) {
                    // For record count
                    continue;
                }
                trueNeighbors.add((Integer) rec.getVal(fld).asJavaVal());
            }

            approximateNeighbors.retainAll(trueNeighbors);
            double recallRate = (double) approximateNeighbors.size() / trueNeighbors.size();

            recallList.add(recallRate);
        }

        double sum = 0;
        for (double recallRate : recallList) {
            sum += recallRate;
        }
        double averageRecallRate = sum / recallList.size();

        statMgr.setRecall(averageRecallRate);

        System.out.println("Average Recall Rate: " + averageRecallRate);
    }
    
}
