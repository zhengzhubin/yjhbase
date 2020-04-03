package com.yjhbase.monitor.prometheus;

import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.List;

public class TextFormat {
	  /**
	   * Write out the text version 0.0.4 of the given MetricFamilySamples.
	   */
	  public static StringBuffer write003(List<MetricFamilySamples> mfs) 
			  throws IOException {
	    /* See http://prometheus.io/docs/instrumenting/exposition_formats/
	     * for the output format specification. */
	    
		  StringBuffer sbf = new StringBuffer("");
		  
		  for (MetricFamilySamples metricFamilySamples: mfs) {
			  sbf.append("# HELP " + metricFamilySamples.name + " " + escapeHelp(metricFamilySamples.help) + "\n");
			  sbf.append("# TYPE " + metricFamilySamples.name + " " + metricFamilySamples.type.value() + "\n");
	      for (Sample sample: metricFamilySamples.samples) {
	    	  sbf.append(sample.name);
	        if (sample.labelNames.size() > 0) {
	        	sbf.append("{");
	          for (int i = 0; i < sample.labelNames.size(); ++i) {
	        	  sbf.append(String.format("%s=\"%s\",",
	                sample.labelNames.get(i),  escapeLabelValue(sample.labelValues.get(i))));
	          }
	          sbf.append("}");
	        }
	        sbf.append(" " + Sample.doubleToGoString(sample.value) + "\n");
	      }
	    }
	    return sbf;
	  }

    public static Writer write004(Writer writer, MetricFamilySamples mfs) throws IOException {
        return write004(writer, Arrays.asList(mfs));
    }
    /**
   * Write out the text version 0.0.4 of the given MetricFamilySamples.
   */
  public static Writer write004(Writer writer, List<MetricFamilySamples> mfs) 
		  throws IOException {
    /* See http://prometheus.io/docs/instrumenting/exposition_formats/
     * for the output format specification. */
    for (MetricFamilySamples metricFamilySamples: mfs) {
      writer.write("# HELP " + metricFamilySamples.name + " " + escapeHelp(metricFamilySamples.help) + "\n");
      writer.write("# TYPE " + metricFamilySamples.name + " " + metricFamilySamples.type.value() + "\n");
      for (Sample sample: metricFamilySamples.samples) {
        writer.write(sample.name);
        if (sample.labelNames.size() > 0) {
          writer.write("{");
          for (int i = 0; i < sample.labelNames.size(); ++i) {
            writer.write(String.format("%s=\"%s\",",
                sample.labelNames.get(i),  escapeLabelValue(sample.labelValues.get(i))));
          }
          writer.write("}");
        }
        writer.write(" " + Sample.doubleToGoString(sample.value) + "\n");
      }
    }
    return writer;
  }

  /**
   * Content-type for text version 0.0.4.
   */
  public final static String CONTENT_TYPE_004 = "text/plain; version=0.0.4; charset=utf-8";

  static String escapeHelp(String s) {
    return s.replace("\\", "\\\\").replace("\n", "\\n");
  }
  static String escapeLabelValue(String s) {
    return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
  }
  
}
