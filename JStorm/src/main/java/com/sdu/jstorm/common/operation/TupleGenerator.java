package com.sdu.jstorm.common.operation;

import java.io.Serializable;
import java.util.List;

/**
 * Generate Tuple For Spout
 *
 * @author hanhan.zhang
 * */
public interface TupleGenerator extends Serializable{

    public List<Object> generator();

}
