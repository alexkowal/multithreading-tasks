package dataprocessor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class DataReader {
    private final int chunksCount;
    private final FileChannel fileChannel;


    public DataReader(int chunksCount, FileChannel fileChannel) {
        this.chunksCount = chunksCount;
        this.fileChannel = fileChannel;
    }

    public String read() throws IOException, ExecutionException, InterruptedException {
        System.out.println(fileChannel.size());
        int size = Math.toIntExact(fileChannel.size());
        int pos = 0;
        int chunkSize = size / chunksCount;

        List<Future<String>> results = new ArrayList<>();
        ExecutorService e = Executors.newWorkStealingPool(7);
        for (int i = 0; i < chunksCount; i++) {
            Future<String> f = e.submit(new ChunkReader(pos, chunkSize, fileChannel));
            results.add(f);
            pos += chunkSize;
        }
        Future<String> f = e.submit(new ChunkReader(pos, size - pos, fileChannel));
        StringBuilder sb = new StringBuilder();
        results.add(f);

        for (Future<String> result : results) {
            sb.append(result.get());
        }
        return sb.toString();
    }

    private static class ChunkReader implements Callable<String> {
        private final int pos;
        private final int chunkSize;
        private final FileChannel fileChannel;

        public ChunkReader(int pos, int chunkSize, FileChannel fileChannel) {
            this.pos = pos;
            this.chunkSize = chunkSize;
            this.fileChannel = fileChannel;
        }

        @Override
        public String call() throws Exception {
            ByteBuffer buffer = ByteBuffer.allocate(chunkSize);
            fileChannel.read(buffer, pos);
            return new String(buffer.array(), StandardCharsets.UTF_8);
        }
    }

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        Path path = FileSystems.getDefault().getPath("/Users/aleksandr/Desktop/test.csv");
        DataReader r = new DataReader(5, FileChannel.open(path));
        System.out.println(r.read());
    }
}
