package com.ftzex.hadoop;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Created by dondavid on 16/6/12.
 */
public class TestToStringBuilder {
    private int id;
    private String name;
    private int age;

    public TestToStringBuilder(int id, String name, int age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }


    @Override
    public String toString() {
        //return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
        //return ToStringBuilder.reflectionToString(this, ToStringStyle.DEFAULT_STYLE);
        return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
    }

    public static void main(String[] args){
        TestToStringBuilder tt = new TestToStringBuilder(1,"Messi",29);
        System.out.println(tt.toString());
    }
}
