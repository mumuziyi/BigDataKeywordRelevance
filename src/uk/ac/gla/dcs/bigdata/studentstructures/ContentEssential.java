package uk.ac.gla.dcs.bigdata.studentstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ContentEssential implements Serializable {
    String content;
    String subtype;
    String type;

    public String getContent(){
        return content;
    }
    public String getSubtype(){
        return subtype;
    }
    public String getType(){
        return type;
    }

    public ContentEssential(String content, String subtype, String type){
        this.content = content;
        this.subtype = subtype;
        this.type = type;
    }

    public static List<ContentEssential> convert(List<ContentItem> items){
        List<ContentEssential> essentials = new ArrayList<>();

        for (ContentItem item : items){
            String content = "";
            String subtype = "";
            String type = "";
            content += item.getContent();
            subtype += item.getSubtype();
            type += item.getType();
            essentials.add(new ContentEssential(content, subtype, type));
        }
        return essentials;

    }

    public ContentEssential convert(){
        TextPreProcessor processor = new TextPreProcessor();
        List<String> processed_strings = processor.process(this.content);
        String content = processed_strings.toString();
        return new ContentEssential(content, this.subtype, this.type);
    }

}
