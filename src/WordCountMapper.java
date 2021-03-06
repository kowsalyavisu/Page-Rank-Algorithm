import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.filter.ElementFilter;
import org.jdom.input.SAXBuilder;

public class WordCountMapper extends
    Mapper<LongWritable, Text,NullWritable, Text> {
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  String xmlString = value.toString();
	  SAXBuilder builder = new SAXBuilder();
      Reader in = new StringReader(xmlString);
      String result="";
      String tag1="";
      String tag2="";
 try {
    
    Document doc = builder.build(in);
    Element root = doc.getRootElement();
    //System.out.println("The root is : "+root.getName());
    
    List children = root.getChildren();
    Iterator iterator = children.iterator();
    while (iterator.hasNext()) {
      Element child = (Element) iterator.next();
      if(child.getName().equalsIgnoreCase("title"))
      {
    	  tag1 = child.getText();
      }
      else if(child.getName().equalsIgnoreCase("revision"))
      {
    	  List subChildren = child.getChildren();
    	  Iterator subIterator = subChildren.iterator();
    	  while (subIterator.hasNext()) {
    	      Element subChild = (Element) subIterator.next();
    	      if(subChild.getName().equalsIgnoreCase("text"))
	    	  {
	    		  tag2 = subChild.getText();
	    		  //System.out.println("Tag2 value is :"+tag2);
	    	  }
    	  }
      }
    }
     
     StringBuilder stringBuilder = new StringBuilder();
     int formater = 0; // flag to append the first string found without # preseeding
     Pattern p =Pattern.compile("\\[\\[([^\\[\\]|]*)[^\\[\\]]*\\]\\]");
     Matcher m = p.matcher(tag2);
     while(m.find())
	    {
    	 String link = tag2.substring(m.start(), m.end());
    	 int st = 0;
    	 int en = 0;
    	 //System.out.println("The String is : "+link);
    	 if(link.contains(":") || link.contains("(") || link.contains(")") || link.contains(",") || link.contains("/") || link.contains("\\"))
    	 {
    		 //System.out.println("the link contains : so the link_text will be empty");
    		 String link_text="";
    		 result= tag1+ ","+link_text;
    	     context.write(NullWritable.get(), new Text(result));
    		/* if(formater == 0) 
				{
		    	 stringBuilder.append(link_text);
		    	 formater++;
		    	}
	  		else
	  			stringBuilder.append("#"+link_text);*/
    	 }
    	 else if(link.contains("|"))
    	 {
    		int inter =0;
    		int sub = 0;
    		System.out.println("The String is : "+link);
    		//System.out.println("The Link contains |");
    		inter = link.indexOf("|");
    		//System.out.println("Contains | at : "+inter);
    		st = m.start();
    		//System.out.println("Start Index is : "+st);
    		sub=st+(inter-1);
    		//System.out.println("end index is: "+sub);
    		String link_text = null;
    		if(st+2 < sub){
    			link_text =tag2.substring(st+2,sub);
	    		if(link_text.contains(":") || link_text.contains("(") || link_text.contains(")") || link_text.contains(",") || link_text.contains("/") || link_text.contains("\\"))
	        	{
	    			 link_text="";
		       		 result= tag1+ ","+link_text;
		       	     context.write(NullWritable.get(), new Text(result));
	        	}
	    		else
	    		{
	    			result= tag1+ ","+link_text;
	       	     	context.write(NullWritable.get(), new Text(result));
	    		
	    		}
    		}
    		//System.out.println("The need is : "+link_text);
    		/*if(formater == 0) 
    			{
    	    	 stringBuilder.append(link_text);
    	    	 formater++;
    	    	}
    		else
    		    stringBuilder.append("#"+link_text);*/
    		
    	 }
    	 else
    	 {
    		//System.out.println("The Link is clean");
    		st = m.start();
     		en = m.end();
     		String link_text=tag2.substring(st+2,en-2);
     		/*if(formater == 0) 
				{
		    	 stringBuilder.append(link_text);
		    	 formater++;
		    	}
     		else
     			stringBuilder.append("#"+link_text);*/
     		result= tag1+ ","+link_text;
     		context.write(NullWritable.get(), new Text(result));
    	 }
	    }
     //String finalString = stringBuilder.toString();
     
     /*result= tag1+ ","+finalString;
     context.write(NullWritable.get(), new Text(result));*/
 } catch (JDOMException ex) {
     //Logger.getLogger(MyParserMapper.class.getName()).log(Level.SEVERE, null, ex);
 } catch (IOException ex) {
     //Logger.getLogger(MyParserMapper.class.getName()).log(Level.SEVERE, null, ex);
 }
  
    }
  }

