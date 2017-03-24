package com.sdu.jstorm.test;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * @author hanhan.zhang
 * */
@Setter
@Getter
@AllArgsConstructor
public class JInputData<T> implements Serializable {

    private String key;

    private T inputData;

}
