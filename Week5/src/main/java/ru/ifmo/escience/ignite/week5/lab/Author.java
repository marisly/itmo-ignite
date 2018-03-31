package ru.ifmo.escience.ignite.week5.lab;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.internal.util.typedef.internal.S;


public class Author {
    @QuerySqlField(index = true, groups = {"firstName"})
    private final String firstName;

    @QuerySqlField(index = true, groups = {"lastName"})
    @AffinityKeyMapped
    private final String lastName;

    @QuerySqlField(index = true)
    private final Integer birthYear;

    public Author(String firstName, String lastName, Integer birthYear){
        this.firstName = firstName;
        this.lastName = lastName;
        this.birthYear = birthYear;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public Integer getBirthYear() {
        return birthYear;
    }

    @Override
    public String toString() {
        return S.toString(Author.class, this);
    }
}
