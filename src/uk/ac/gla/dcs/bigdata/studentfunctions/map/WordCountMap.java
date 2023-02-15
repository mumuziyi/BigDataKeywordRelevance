package uk.ac.gla.dcs.bigdata.studentfunctions.map;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.util.LongAccumulator;
import org.terrier.matching.models.aftereffect.L;
import scala.Tuple2;
import scala.Tuple3;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

import java.util.List;

public class WordCountMap implements MapFunction<NewsArticle, Tuple3<NewsArticle,Integer, Short>> {
    LongAccumulator fileCount = new LongAccumulator();
    LongAccumulator termCount = new LongAccumulator();
    String term;

    public WordCountMap(LongAccumulator longAccumulator, LongAccumulator termAccount, String term){
        this.fileCount = longAccumulator;
        this.termCount = termAccount;
        this.term = term;
    }

    @Override
    public Tuple3<NewsArticle, Integer,Short> call(NewsArticle value) throws Exception {

        int thisFileCount = 0;
        short thisTermCount = 0;
        int paragraph = 0;

        if (value.getTitle() != null && value.getTitle().length() > 0){
            String title = value.getTitle();
            fileCount.add(title.split(" ").length);
            thisFileCount += title.split(" ").length;
            termCount.add(getNumber(title, term));
            thisTermCount += getNumber(title,term);
        }

        List<ContentItem> contentItems = value.getContents();

        for (ContentItem contentItem: contentItems){
            if (contentItem.getSubtype() != null && contentItem.getSubtype().equals("paragraph")){
                String content = contentItem.getContent();
                long contentLength = content.split(" ").length;
                fileCount.add(contentLength);
                thisFileCount += contentLength;

                long Count = getNumber(content,term);
                termCount.add(Count);
                thisTermCount += Count;

                paragraph += 1;
                if (paragraph >= 5){
                    break;
                }

            }
        }
        System.out.println("finish one vale");
        return new Tuple3<>(value,thisFileCount,thisTermCount);
    }

    public int getNumber(String str, String term){
        int ans = 0;
        String[] tokens = str.split(" ");
        for (int i = 0; i < tokens.length; i++){
            if (term.equals(tokens[i])){
                ans ++;
            }
        }
        return ans;
    }
}
