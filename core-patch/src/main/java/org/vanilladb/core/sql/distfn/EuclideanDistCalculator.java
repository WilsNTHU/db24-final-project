package org.vanilladb.core.sql.distfn;

import org.vanilladb.core.sql.VectorConstant;


// Optimization: SIMD operation
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

public class EuclideanDistCalculator {
    static final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;
    static final int SPECIES_LENGTH = SPECIES.length();

    public EuclideanDistCalculator(){};

    protected double calDistanceOfTwoVec(VectorConstant vec1, VectorConstant vec2){
        float[] array1 = vec1.asJavaVal();
        float[] array2 = vec2.asJavaVal();
        double sum = 0;
        int i = 0;
        int bound = SPECIES.loopBound(vec1.dimension());
        FloatVector vector1, vector2, diffVector;
        // Perform SIMD on operands as many as possible
        for (; i < bound; i += SPECIES_LENGTH) {
            vector1 = FloatVector.fromArray(SPECIES, array1, i);
            vector2 = FloatVector.fromArray(SPECIES, array2, i);
            diffVector = vector1.sub(vector2);
            sum += diffVector.mul(diffVector).reduceLanes(VectorOperators.ADD);
        }
        // Perform tail operands sequentially
        for(; i < vec1.dimension(); ++i) {
            double diff = array1[i] - array2[i];
            sum = Math.fma(diff, diff, sum);
        }
        return Math.sqrt(sum);
    }
    
}
