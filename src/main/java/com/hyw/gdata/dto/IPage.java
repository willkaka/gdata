package com.hyw.gdata.dto;

import lombok.Data;

import java.util.List;

@Data
public class IPage<T> {

    private int totalCnt=0;  //总记录
    private int pageSize=0;  //每页记录数
    private int totalPage=0; //总页数
    private int curRecord=0; //当前记录号
    private int curPage=0;   //当前页数
    private List<T> records;

}
