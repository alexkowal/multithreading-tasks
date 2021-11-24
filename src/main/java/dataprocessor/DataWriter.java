package dataprocessor;

import dataprocessor.data.Person;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class DataWriter {
    private static final String INSERT_PERSON = "INSERT INTO people(id, firstName, lastName, primaryEmail, secondaryEmail, profession)" +
            "VALUES(?,?,?,?,?,?)";
    private static Connection c;

    public DataWriter() throws SQLException {
        c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/people", "postgres", "12345678");
        c.setAutoCommit(false);
    }

    public void writeBatches(List<Person> people, int batchSize) {
        int batchesCount = people.size() / batchSize;
        int pos = 0;
        Executor executor = Executors.newWorkStealingPool(8);
        for (int i = 0; i < batchesCount; i++) {
            executor.execute(
                    new BatchWriter(c,
                            people.subList(i * batchSize, i * batchSize + batchSize)));
            pos = i * batchSize + batchSize;
        }
        executor.execute(new BatchWriter(c,
                people.subList(pos, people.size())));
    }

    private static class BatchWriter implements Runnable {
        private final Connection connection;
        private final List<Person> people;

        public BatchWriter(Connection connection, List<Person> people) {
            this.connection = connection;
            this.people = people;
        }

        public void write(List<Person> people) throws SQLException, InterruptedException {
            PreparedStatement s = connection.prepareStatement(INSERT_PERSON);
            for (int i = 0; i < people.size(); i++) {
                s.setInt(1, people.get(i).getId());
                s.setString(2, people.get(i).getFirstName());
                s.setString(3, people.get(i).getLastName());
                s.setString(4, people.get(i).getPrimaryEmail());
                s.setString(5, people.get(i).getSecondaryEmail());
                s.setString(6, people.get(i).getProfession());
                s.addBatch();
            }
            s.executeBatch();
            connection.commit();
        }

        @Override
        public void run() {
            try {
                write(people);
            } catch (SQLException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws SQLException, IOException, ExecutionException, InterruptedException {
        Path path = FileSystems.getDefault().getPath("/Users/aleksandr/Desktop/test2.csv");
        DataReader r = new DataReader(100, FileChannel.open(path));
        DataWriter w = new DataWriter();
        DataAnonymizer dataAnonymizer = new DataAnonymizer(r, w);
        System.currentTimeMillis();
        String s = r.read();
        List<Person> people = dataAnonymizer.anonymizeData(s);
        w.writeBatches(people, 10000);
        Thread.sleep(1000000);
    }
}
