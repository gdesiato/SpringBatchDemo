package com.example.batch.demo.model;

import jakarta.persistence.EntityNotFoundException;

import java.util.Arrays;

// Enum class for the Designation of the Employees
public enum Designation {
    DEVELOPER("dev", "Developer"),
    DESIGNER("des", "Designer"),
    TESTER("test", "Tester");

    private final String code;
    private final String title;

    // Constructor
    Designation(String code, String title) {
        this.code = code;
        this.title = title;
    }

    // Getters
    public String getCode() {
        return code;
    }

    public String getTitle() {
        return title;
    }

    public static Designation getByCode(final String code) {
        return Arrays.stream(Designation.values())
                .filter(designation -> designation.getCode().equals(code))
                .findFirst()
                .orElseThrow(EntityNotFoundException::new);
    }
}
