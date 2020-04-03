/**
 * 
 */
package com.yjhbase.monitor.prometheus;

import java.util.ArrayList;
import java.util.List;


/**
 * @author zhengzhubin
 * da-jmx-utils
 * 2016年11月25日 下午3:56:30
 */
public class MetricFamilySamples {
    public String name = null;
    public Type type = null;
    public String help = null;
    public List<Sample> samples = null;
    
    public MetricFamilySamples(String name, Type type, String help, List<Sample> samples) {
      this.name = name;
      this.type = type;
      this.help = help;
      this.samples = samples;
    }
    
    public MetricFamilySamples(String name, Type type, String help) {
      this(name, type, help, new ArrayList<>());
    }

    public void setSamples(List<Sample> samples){
    	this.samples = samples;
    }

    public MetricFamilySamples addSamples(List<Sample> samples){
        this.samples.addAll(samples);
        return this;
    }

    public MetricFamilySamples addSample(Sample sample){
    	this.samples.add(sample);
    	return this;
    }

    public static MetricFamilySamples build(String metric, String help) {
        return build(metric, help, Type.GAUGE);
    }

    public static MetricFamilySamples build(String metric, String help, Type type) {
        return new MetricFamilySamples(metric, type, help);
    }

    @Override
    public String toString() {
      return "Name: " + name + " Type: " + type + " Help: " + help + 
        " Samples: " + samples;
    }

    public enum Type {
        COUNTER((String) "counter"),
        GAUGE((String) "gauge"),
        SUMMARY((String) "summary"),
        HISTOGRAM((String) "histogram");
        private String value;
    	Type(String value){
    		this.value = value;
    	}
    	
    	 public String value() {
    	        return value;
    	    }
    }
}
