package com.example.worldartexport.model;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder(value = {
        "name",
        "date",
        "imdbUrl",
        "score",
})
public record ImdbCsvDto(String name, String date, String imdbUrl, int score) { }
