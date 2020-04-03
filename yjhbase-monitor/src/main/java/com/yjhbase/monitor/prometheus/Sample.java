/**
 * 
 */
package com.yjhbase.monitor.prometheus;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhengzhubin
 * da-jmx-portal
 * 2016年11月25日 下午6:39:34
 */

/**
 * A single Sample, with a unique name and set of labels.
 */
public class Sample {
    public String name = null;
    public List<String> labelNames = null;
    public List<String> labelValues = null;  // Must have same length as labelNames.
    public double value;
    
    public Sample(String name, List<String> labelNames, List<String> labelValues, double value) {
      this.name = name;
      this.labelNames = labelNames;
      this.labelValues = labelValues;
      this.value = value;
    }
    
    public Sample(String name, String labelName, String labelValue, double value) {
        this.name = name;
        this.labelNames = new ArrayList<String>();
        this.labelValues = new ArrayList<String>();
        this.labelNames.add(labelName);
        this.labelValues.add(labelValue);
        this.value = value;
      }
    
    public Sample(String name) {
        this.name = name;
        this.labelNames = new ArrayList<String>();
        this.labelValues = new ArrayList<String>();
    }
    
    public void addLable(String name,String value){
    	this.labelNames.add(name);
    	this.labelValues.add(value);
    }

    public void setSampleValue(double value){
    	this.value = value;
    }

    public static Sample build(String name, String clusterId) {
        Sample sample = new Sample(name);
        sample.addLable("clusterId" , clusterId);
        return sample;
    }

    /**
     * Convert a double to it's string representation in Go.
     */
    public static String doubleToGoString(double d) {
      if (d == Double.POSITIVE_INFINITY) {
        return "+Inf";
      } 
      if (d == Double.NEGATIVE_INFINITY) {
        return "-Inf";
      }
      if (Double.isNaN(d)) {
        return "NaN";
      }
      return Double.toString(d);
    }
  }