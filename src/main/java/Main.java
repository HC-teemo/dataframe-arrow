import org.apache.arrow.memory.*;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;
import scala.Int;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
        BitVector bitVector = new BitVector("boolean", allocator);
        VarCharVector varCharVector = new VarCharVector("varchar", allocator);
        bitVector.allocateNew();
        varCharVector.allocateNew();
        for (int i = 0; i < 10; i++) {
            bitVector.setSafe(i, i % 2 == 0 ? 0 : 1);
            varCharVector.setSafe(i, ("test" + i).getBytes(StandardCharsets.UTF_8));
        }
        bitVector.setValueCount(10);
        varCharVector.setValueCount(10);

        List<Field> fields = Arrays.asList(bitVector.getField(), varCharVector.getField());
        List<FieldVector> vectors = Arrays.asList(bitVector, varCharVector);
        VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(fields, vectors);
        System.out.println(vectorSchemaRoot);
        VectorUnloader unloader = new VectorUnloader(vectorSchemaRoot);
        ArrowRecordBatch recordBatch = unloader.getRecordBatch();
        System.out.println(recordBatch);
    }
}
