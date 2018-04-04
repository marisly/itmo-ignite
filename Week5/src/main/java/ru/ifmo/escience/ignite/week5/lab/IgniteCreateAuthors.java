package ru.ifmo.escience.ignite.week5.lab;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import ru.ifmo.escience.ignite.Utils;
import ru.ifmo.escience.ignite.week4.Person;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;

import javax.cache.Cache;
import java.io.IOException;
import java.util.List;

import static ru.ifmo.escience.ignite.Utils.print;
import static ru.ifmo.escience.ignite.Utils.readln;

public class IgniteCreateAuthors {
    public static void main(String[] args) throws IOException {

        CacheConfiguration<Integer, Author> ccfg = new CacheConfiguration<>("Author");
//        CacheConfiguration<Integer, Author> ccfg1 = new CacheConfiguration<>("Book");

        ccfg.setIndexedTypes(Integer.class, Author.class);

        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setClientMode(true);
        cfg.setCacheConfiguration(ccfg);

        Ignite node1 = Ignition.start("Week5/config/default-client.xml");

        node1.getOrCreateCache(ccfg);

        try {

            node1.cache("Author").put(1, new Author("Joah K", "Rowling", 1950));
            node1.cache("Author").put(2, new Author("Jack", "London", 1880));
            node1.cache("Author").put(3, new Author("Mark", "Twain", 1830));
            node1.cache("Author").put(4, new Author("Charles", "Dickens", 1880));
            node1.cache("Author").put(5, new Author("Oscar", "Wilde", 1870));

            System.out.println(node1.cache("Author").query(new SqlFieldsQuery("SELECT * FROM Author")).getAll());


           node1.cache("Author").query(new SqlFieldsQuery("DROP TABLE IF EXISTS \"PUBLIC\".Book"));

            node1.cache("Author").query(new SqlFieldsQuery("CREATE TABLE if not exists \"PUBLIC\".Book(id int, author_id int, title varchar, year int, genre_id int, primary key(id, author_id)) WITH \"affinitykey=AUTHOR_ID,cache_name=Book,key_type=BookKey,value_type=Book\""));

            node1.cache("Author").query(new SqlFieldsQuery("CREATE TABLE if not exists \"PUBLIC\".Genre(id int, name int, primary key(id)) WITH \"cache_name=Genre,key_type=GenreKey,value_type=Genre, backups=1\""));


           node1.cache("Book").query(new SqlFieldsQuery("INSERT INTO Book(id, author_id, title, year, genre_id) VALUES (1, 1, 'Potter', 2000, '1')"));
            node1.cache("Book").query(new SqlFieldsQuery("INSERT INTO Book(id, author_id, title, year, genre_id) VALUES (2, 2, 'Martin', 1910, '1')"));

            //BinaryObjectBuilder bldr1 = node1.binary().builder("Author");


            System.out.println(node1.cache("Book").query(new SqlFieldsQuery("SELECT * FROM \"PUBLIC\".Book")).getAll());
            System.out.println(node1.cache("Genre").query(new SqlFieldsQuery("SELECT * FROM \"PUBLIC\".Genre")).getAll());

            BinaryObject ck = node1.binary().builder("BookKey").setField("ID", 1).setField("AUTHOR_ID", 100000).build();


            print(Utils.sameAffinity(node1, "Book", 1, "Author", 1));

            readln();

        } finally {
            node1.cache("Author").query(new SqlFieldsQuery("DROP TABLE IF EXISTS \"PUBLIC\".Book"));
            node1.cache("Author").destroy();

            node1.close();
        }
    }

}

