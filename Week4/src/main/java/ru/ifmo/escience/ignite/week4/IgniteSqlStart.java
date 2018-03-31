package ru.ifmo.escience.ignite.week4;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;

import javax.cache.Cache;
import java.io.IOException;
import java.util.List;

import static ru.ifmo.escience.ignite.Utils.readln;

public class IgniteSqlStart {
    public static void main(String[] args) throws IOException {
        //System.setProperty("IGNITE_H2_DEBUG_CONSOLE", "true");

        try (Ignite node = Ignition.start("Week4/config/default.xml")) {

            IgniteCache dflt = node.getOrCreateCache("default");
            dflt.query(new SqlFieldsQuery("CREATE TABLE \"PUBLIC\".Book (id int, store_id int, author VARCHAR, title VARCHAR, year INTEGER, genre VARCHAR " +
                    ",PRIMARY KEY (id, store_id)) WITH \"cache_name=Book, key_type=BookKey, value_type=Book, backups=1, affinityKey=store_id\"")).getAll();

            execute(dflt, "INSERT INTO Book(id, store_id, author, title, year, genre) VALUES (1, 2, 'Rowling', 'Potter', 2000, 'fant')");
            System.out.println(execute(dflt, "SELECT * FROM Book WHERE year = ? AND genre = ?", 2000, "fant"));

            IgniteCache cache = node.getOrCreateCache("Book");

            BinaryObjectBuilder bldr = node.binary().builder("BookKey");
            bldr.setField("ID", 2);
            bldr.setField("STORE_ID", 3);
            BinaryObject key = bldr.build();

            System.out.println(key.toString());
         //   System.out.println(node.cache("Book").withKeepBinary().get(key).toString());

            BinaryObjectBuilder bldr2 = node.binary().builder("Book");
            bldr2.setField("AUTHOR", "T");
            bldr2.setField("TITLE", "A");
            bldr2.setField("YEAR", 1950);
            bldr2.setField("GENRE", "fant");
            BinaryObject value = bldr2.build();
            node.cache("Book").withKeepBinary().put(key, value);

            System.out.println(execute(dflt, "SELECT * FROM Book "));




            /*
            node.cache("mycache").put(1, new Person("John", "Doe", 5000));
            node.cache("mycache").put(2, new Person("Mike", "Smith", 10000));
            node.cache("mycache").put(3, new Person("Jane", "Li", 700));
            IgniteCache<Integer, Person> cache = node.cache("mycache");

            SqlFieldsQuery sqlInsert = new SqlFieldsQuery
                    ("insert into person  (_key, name, surname, salary) values (?, 'Mary', 'Rose', 7000) ");
            try (FieldsQueryCursor<List<?>>  cursor = cache.query(sqlInsert.setArgs(4))){
                for (List<?> e : cursor) {
                    System.out.println(e);
                }
            }
            SqlFieldsQuery sql = new SqlFieldsQuery
                    ("select concat(name, ' ', surname) from person where salary > ?");
            try (FieldsQueryCursor<List<?>>  cursor = cache.query(sql.setArgs(1000))){
                for (List<?> e : cursor) {
                    System.out.println(e);
                }
            }
            Person p = cache.get(4);
            System.out.println(p.toString());
*/

            readln();

        }
    }

    private static List<List<?>> execute(IgniteCache cache, String myQuery, Object ... arguments) {
        return cache.query(new SqlFieldsQuery(myQuery).setSchema("PUBLIC").setArgs(arguments)).getAll();

    }
}
