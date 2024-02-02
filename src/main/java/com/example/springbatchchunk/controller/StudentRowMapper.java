package com.example.springbatchchunk.controller;

import com.example.springbatchchunk.model.Student;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;


public class StudentRowMapper implements RowMapper<Student> {

    @Override
    public Student mapRow(ResultSet rs, int rowNum) throws SQLException {
        System.out.println("*****");
        return Student.builder()
                .id(rs.getInt("id"))
                .name(rs.getString("name"))
                .department(rs.getString("department"))
                .age(rs.getInt("age"))

                .build();
    }
}