package com.example.batch.demo.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

import java.util.Date;

@Entity
public class Employee {

    // notice there is no @GeneratedValue, since the value will be pulled from the csv data
    @Id
    private Integer id;
    private String name;
    private String designation;
    private Date nameUpdatedAt;
    private Date designationUpdatedAt;

    public Employee() {
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDesignation() {
        return designation;
    }

    public void setDesignation(String designation) {
        this.designation = designation;
    }

    public void setNameUpdatedAt(Date nameUpdatedAt) {
        this.nameUpdatedAt = nameUpdatedAt;
    }

    public void setDesignationUpdatedAt(Date designationUpdatedAt) {
        this.designationUpdatedAt = designationUpdatedAt;
    }

    @Override
    public String toString() {
        return String.format("Employee{id=%d, name='%s', designation='%s', nameUpdated='%s', designationUpdated='%s'}",
                id, name, designation, nameUpdatedAt, designationUpdatedAt);
    }
}