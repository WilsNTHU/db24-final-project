package org.vanilladb.core.query.algebra.vector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.distfn.DistanceFn;

public class NearestNeighborScan implements Scan {

    private class Pair {
        double key;
        Map<String, Constant> value;

        public Pair(double key, Map<String, Constant> value) {
            this.key = key;
            this.value = value;
        }

        public double getKey() {
            return key;
        }

        public Map<String, Constant> getValue() {
            return value;
        }
    }

    Scan s;

    private DistanceFn distFn;
    private Set<String> projectFields;
    private int limit;

    private int currentResult;
    private List<Map<String, Constant>> resultSet = new ArrayList<>(); 
    
    private boolean isBeforeFirsted;

    public NearestNeighborScan(Scan s, DistanceFn distFn, int limit, Set<String> projectFields) {
        this.s = s;
        this.distFn = distFn;
        this.projectFields = projectFields;
        this.limit = limit;
    }

    @Override
    public void beforeFirst() {
        // Optimization: Sort, project, and limit vectors here
        PriorityQueue<Pair> pq = new PriorityQueue<>((a, b) -> (int)(b.getKey() - a.getKey()));
        s.beforeFirst();
        while (s.next()) {
            // Get distance
            if (!s.hasField(distFn.fieldName()))
                throw new UnsupportedOperationException();
            VectorConstant currentVector = (VectorConstant) s.getVal(distFn.fieldName());
            double distance = distFn.distance(currentVector);
            // Optimization: early-stopping
            if ((pq.size() >= limit) && (distance >= pq.peek().getKey()))
                continue;
            // Remove max element and insert current result record
            Map<String, Constant> resultRecord = new HashMap<>();
            for (String projectField : projectFields) {
                if (s.hasField(projectField))
                    resultRecord.put(projectField, s.getVal(projectField));
            }
            if (pq.size() >= limit)
                pq.poll();
            pq.add(new Pair(distance, resultRecord));
        }
        s.close();
        // Add all remaining Constant into
        while(pq.size() != 0)
            resultSet.add(pq.poll().getValue());
        currentResult = -1;
        isBeforeFirsted = true;
    }

    @Override
    public boolean next() {
        if (!isBeforeFirsted) 
            throw new IllegalStateException("You must call beforeFirst() before iterating NearestNeighborScan.");
        return (++currentResult < resultSet.size());
    }

    @Override
    public void close() {
        if (resultSet.size() < limit) {
            System.out.println("Returned " + resultSet.size() + "elements.");
        }
    }

    @Override
    public boolean hasField(String fldName) {
        return resultSet.get(currentResult).containsKey(fldName);
    }

    @Override
    public Constant getVal(String fldName) {
        return resultSet.get(currentResult).get(fldName);
    }
}
