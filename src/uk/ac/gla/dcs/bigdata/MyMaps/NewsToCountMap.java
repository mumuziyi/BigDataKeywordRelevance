package uk.ac.gla.dcs.bigdata.MyMaps;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.MyStructure.NewsCount;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This converts a NewsArticle into a {@link NewsCount} Object.
 */
public class NewsToCountMap implements MapFunction<NewsArticle, NewsCount> {

    private static final long serialVersionUID = -1912835801896003830L;
    Broadcast<Set<String>> broadcastQueryList;

    Map<String, LongAccumulator> accumulatorMap;

    LongAccumulator globalArticleLength;
    LongAccumulator globalArticleNumber;

    /**
     * @param queryTermSet        Set of query terms that are broadcast to all nodes.
     * @param accumulatorMap      A map that keeps track of each query term's appearances in ALL articles.
     * @param globalArticleLength A global accumulator that keeps track of the total length of all the articles. i.e. Sum of number of words in each article.
     * @param globalArticleNumber A global accumulator that keeps track of the total number of articles.
     */
    public NewsToCountMap(Broadcast<Set<String>> queryTermSet, Map<String, LongAccumulator> accumulatorMap, LongAccumulator globalArticleLength, LongAccumulator globalArticleNumber) {
        this.broadcastQueryList = queryTermSet;
        this.accumulatorMap = accumulatorMap;
        this.globalArticleLength = globalArticleLength;
        this.globalArticleNumber = globalArticleNumber;
    }

    @Override
    public NewsCount call(NewsArticle value) throws Exception {
//        The total length of an article, containing the length of TITLE and length of CONTENT.
        int articleLength = 0;

//        Considering an article might have an empty title,
//        in that case, we just return a NewsCount object with a null title and an empty map,
//        and the total length is 0.
        if (value.getTitle() == null) {
            return new NewsCount(value, new HashMap<>(), 0);
        }

        globalArticleNumber.add(1);
//        Retrieving the query term set from the broadcast variable.
        Set<String> QueriesTerms = broadcastQueryList.value();

//        A map keeps track of each query term's appearances in the article.
//        The number of appearances is highly likely to be 0.
        Map<String, Integer> termCountMap = new HashMap<>();

        // First handle the title's terms
        TextPreProcessor processor = new TextPreProcessor();
        String title = value.getTitle();
        List<String> titleTerms = processor.process(title);


        for (String titleTerm : titleTerms) {
//            Each time an article's processed, the global article length accumulator is incremented by 1.
//            By the time all articles are processed, the global article length accumulator will have the total length of all articles.
            globalArticleLength.add(1);
            articleLength++;
//            If the current term is a query term, then we increment the term's appearance in the article by 1.
            if (QueriesTerms.contains(titleTerm)) {
                termCountMap.put(titleTerm, termCountMap.getOrDefault(titleTerm, 0) + 1);
                accumulatorMap.get(titleTerm).add(1);
            }
        }

//        Get all the contentItems from the article.
        List<ContentItem> contentItems = value.getContents();
//        A counter to keep track of the number of paragraphs we have processed.
//        We only want to process the first 5 paragraphs.
        int curPara = 0;
        for (ContentItem contentItem : contentItems) {
//            Need to consider the case that the contentItem is null or the contentItem doesn't have a subtype.
            if (contentItem == null) continue;
            if (contentItem.getSubtype() == null || !contentItem.getSubtype().equals("paragraph")) continue;
            curPara += 1;
//            If the current paragraph is the 6th paragraph, then we break the loop, because we have needed 5 paragraphs.
            if (curPara > 5) break;
//            Get the content of the current paragraph and process it to remove stop words and stem the words.
            List<String> contentTokens = processor.process(contentItem.getContent());

            for (String contentToken : contentTokens) {
//                Every time a word is processed, the global article length accumulator is incremented by 1.
                globalArticleLength.add(1);
                if (QueriesTerms.contains(contentToken)) {
//                    When a query term is found, we increment the term's appearance in the article by 1
                    termCountMap.put(contentToken, termCountMap.getOrDefault(contentToken, 0) + 1);
                    accumulatorMap.get(contentToken).add(1);
                }
                articleLength++;
            }

        }
//        Once everything in the article is processed, return a NewsCount Object.
        return new NewsCount(value, termCountMap, articleLength);
    }
}
