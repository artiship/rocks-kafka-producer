package com.me.rocks.kafka.model;

import org.apache.avro.reflect.AvroDefault;
import org.apache.avro.reflect.AvroIgnore;
import org.apache.avro.reflect.AvroMeta;
import org.apache.avro.reflect.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DifferentUser {
    @Nullable
    @AvroMeta(key = "key", value = "value")
    private java.lang.String name;
    @AvroDefault(value = "0")
    private int favoriteNumber;
    private java.lang.String favoriteColor;
    @AvroIgnore
    private java.lang.String ignore;
    private String userType;
    private List<java.lang.String> colors = new ArrayList<>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getFavoriteNumber() {
        return favoriteNumber;
    }

    public void setFavoriteNumber(int favoriteNumber) {
        this.favoriteNumber = favoriteNumber;
    }

    public String getFavoriteColor() {
        return favoriteColor;
    }

    public void setFavoriteColor(String favoriteColor) {
        this.favoriteColor = favoriteColor;
    }

    public String getIgnore() {
        return ignore;
    }

    public void setIgnore(String ignore) {
        this.ignore = ignore;
    }

    public String getUserType() {
        return userType;
    }

    public void setUserType(String userType) {
        this.userType = userType;
    }

    public List<String> getColors() {
        return colors;
    }

    public void setColors(List<String> colors) {
        this.colors = colors;
    }

    @Override
    public String toString() {
        return "DifferentUser{" +
                "name='" + name + '\'' +
                ", favoriteNumber=" + favoriteNumber +
                ", favoriteColor='" + favoriteColor + '\'' +
                ", ignore='" + ignore + '\'' +
                ", userType='" + userType + '\'' +
                ", colors=" + colors +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DifferentUser that = (DifferentUser) o;
        return favoriteNumber == that.favoriteNumber &&
                Objects.equals(name, that.name) &&
                Objects.equals(favoriteColor, that.favoriteColor) &&
                Objects.equals(ignore, that.ignore) &&
                Objects.equals(userType, that.userType) &&
                Objects.equals(colors, that.colors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, favoriteNumber, favoriteColor, ignore, userType, colors);
    }
}
