package dataprocessor;

import dataprocessor.data.Person;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class DataAnonymizer {
    private DataReader dataReader;
    private DataWriter dataWriter;

    public DataAnonymizer(DataReader dataReader, DataWriter dataWriter) {
        this.dataReader = dataReader;
        this.dataWriter = dataWriter;
    }

    public List<Person> anonymizeData(String data) {
        List<String> raws = new ArrayList<>(List.of(data.split("\n")));
        raws.remove(0);
        List<Person> people = raws.stream()
                .map(this::processRaw)
                .collect(Collectors.toList());
        people.forEach(person -> person.setFirstName("ANONYMOUS"));
        return people;
    }

    private Person processRaw(String raw) {
        List<String> split = List.of(raw.split(","));
        Person p = new Person();
        p.setId(Integer.parseInt(split.get(0)));
        p.setFirstName(split.get(1));
        p.setLastName(split.get(2));
        p.setPrimaryEmail(split.get(3));
        p.setSecondaryEmail(split.get(4));
        p.setProfession(split.get(5));
        return p;
    }

    private String readData() throws IOException, ExecutionException, InterruptedException {
        return dataReader.read();
    }

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException, SQLException {
        Path path = FileSystems.getDefault().getPath("/Users/aleksandr/Desktop/test.csv");
        DataReader r = new DataReader(5, FileChannel.open(path));
        DataWriter writer = new DataWriter();
        String peopleInRaw = r.read();
        List<Person> people = new DataAnonymizer(r, null).anonymizeData(peopleInRaw);
    }
}
