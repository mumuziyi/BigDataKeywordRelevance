package uk.ac.gla.dcs.bigdata.studentfunctions.map;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple3;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.studentstructures.SimplifiedNewsArticle;

import java.util.List;

/**
 * This Map Function takes a {@link SimplifiedNewsArticle} and count the number of a specific term in the title and content.
 * <br>
 * It returns a Tuple in the form of (term, fileCount, termCount)
 */
public class WordCountMap implements MapFunction<SimplifiedNewsArticle, Tuple3<String,Integer, Short>> {
    LongAccumulator fileCount;
    LongAccumulator termCount;
    String term;

    public WordCountMap(LongAccumulator longAccumulator, LongAccumulator termAccount, String term){
        this.fileCount = longAccumulator;
        this.termCount = termAccount;
        this.term = term;
    }

    @Override
    public Tuple3<String, Integer,Short> call(SimplifiedNewsArticle value) throws Exception {

        int thisFileCount = 0;
        short thisTermCount = 0;
        int paragraph = 0;

        if (value.getFilteredTitle() != null && value.getFilteredTitle().length() > 0){
            String title = value.getFilteredTitle();
            fileCount.add(title.split(" ").length);
            thisFileCount += title.split(" ").length;
            termCount.add(getCountByTerm(title, term));
            thisTermCount += getCountByTerm(title,term);
        }

        List<ContentItem> contentItems = value.getContents();
        if (contentItems == null){
            return new Tuple3<>(value.getOriginalTitle(),thisFileCount,thisTermCount);
        }

        for (ContentItem contentItem: contentItems){
            if (contentItem.getSubtype() != null && contentItem.getSubtype().equals("paragraph")){
                String content = contentItem.getContent();
                long contentLength = content.split(" ").length;
                fileCount.add(contentLength);
                thisFileCount += contentLength;

                long Count = getCountByTerm(content,term);
                termCount.add(Count);
                thisTermCount += Count;

                paragraph += 1;
                if (paragraph >= 5){
                    break;
                }

            }
        }
        return new Tuple3<>(value.getOriginalTitle(),thisFileCount,thisTermCount);
    }

    public int getCountByTerm(String str, String term){
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
