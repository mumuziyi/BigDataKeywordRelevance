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
        List<String> titleList = processor.process(title);




        // 处理title的所含的term
        // 遍历title，如果发现title中当前的单词属于query 的term， 就放进去
        for (String titleTerm : titleList) {
//            Each time an article's processed, the global article length accumulator is incremented by 1.
//            By the time all articles are processed, the global article length accumulator will have the total length of all articles.
            globalArticleLength.add(1);
            articleLength++;
            if (QueriesTerms.contains(titleTerm)) {
                termCountMap.put(titleTerm, termCountMap.getOrDefault(titleTerm, 0) + 1);
                accumulatorMap.get(titleTerm).add(1);
            }
        }

        // 处理正文
        List<ContentItem> contentItems = value.getContents();
        // 记录当前段落
        int curPara = 0;
        // 遍历newsArticle的所有contentItem
        for (ContentItem contentItem : contentItems) {
            // 如果当前的subtype不是段落，则取下一个contentItem
            if (contentItem == null) continue;
//                throw new NullPointerException("contentItem is null");
            if (contentItem.getSubtype() == null || !contentItem.getSubtype().equals("paragraph")) {
                continue;
            }
            curPara += 1;
            // 只要前五段，段数大于五段break返回
            if (curPara > 5) {
                break;
            }

            String content = contentItem.getContent();
            List<String> contentTokens = processor.process(content);

            // 处理当前的content
            for (String contentToken : contentTokens) {
                globalArticleLength.add(1);
                if (QueriesTerms.contains(contentToken)) {
                    termCountMap.put(contentToken, termCountMap.getOrDefault(contentToken, 0) + 1);
                    accumulatorMap.get(contentToken).add(1);
                }
                articleLength++;
            }

        }
        // 处理完这篇文章
        return new NewsCount(value, termCountMap, articleLength);
    }
}
