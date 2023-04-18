package com.example.cdh.dto;

import java.io.Serializable;

/**
 * @author chunyang.leng
 * @date 2023-04-18 10:55
 */
public class UserDTO implements Serializable {
    private String name;

    private Integer age;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }
}
