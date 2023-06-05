package bin.bigdata.newfunctions;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import bin.bigdata.newstructures.NewsCount;
import bin.bigdata.providedstructures.ContentItem;
import bin.bigdata.providedstructures.NewsArticle;
import bin.bigdata.providedutilities.TextPreProcessor;

import java.util.*;

/**
 * This converts a NewsArticle into a {@link NewsCount} Object.
 * Finish counting all query terms in one loop
 * NewsCount contains (NewsArticle, allKindsOfCount) pair,
 * which can be used to calculate DPHScore
 */
public class NewsToCountMap implements MapFunction<NewsArticle, NewsCount> {

    private static final long serialVersionUID = -1912835801896003830L;
    Broadcast<Set<String>> broadcastQueryList;

    Map<String, LongAccumulator> accumulatorMap;

    LongAccumulator totalLengthInAll;
    LongAccumulator newsNumberInAll;

    public NewsToCountMap(Broadcast<Set<String>> broadcastQueryList,Map<String, LongAccumulator> accumulatorMap,LongAccumulator totalLengthInAll,LongAccumulator newsNumberInAll){
        this.broadcastQueryList = broadcastQueryList;
        this.accumulatorMap = accumulatorMap;
        this.totalLengthInAll = totalLengthInAll;
        this.newsNumberInAll = newsNumberInAll;
    }

    @Override
    public NewsCount call(NewsArticle value) throws Exception {

        int articleLength = 0;

        // 如果title为空，不计入
        if (value.getTitle() == null){
            value.setContents(null);
            return new NewsCount(value,new HashMap<>(),0);
        }

        newsNumberInAll.add(1);
        // 所有terms的集合
        Set<String> QueriesTerms = broadcastQueryList.value();

//        A map keeps track of each query term's appearances in the article.
//        The number of appearances is highly likely to be 0.
        Map<String, Integer> termCountMap = new HashMap<>();

        // First handle the title's terms
        TextPreProcessor processor = new TextPreProcessor();

        // 处理文章标题terms
        String title = value.getTitle();
        List<String> titleList = processor.process(title);

        // 处理title的所含的term
        // 遍历title，如果发现title中当前的单词属于query 的term， 就放进去或者更新（+1）
        for (String titleTerm: titleList){
            totalLengthInAll.add(1);
            articleLength ++;
            if (QueriesTerms.contains(titleTerm)){
                termCountMap.put(titleTerm, termCountMap.getOrDefault(titleTerm,0) + 1);
                // 在accumulator中加，用来保存每个term在所有文章中出现的次数
                accumulatorMap.get(titleTerm).add(1);
            }
        }

        // 处理正文
        List<ContentItem> contentItems = value.getContents();
        // 记录当前段落
        List<ContentItem> newContentItems = new ArrayList<>();
        int curPara = 0;
        // 遍历newsArticle的所有contentItem
        for (ContentItem contentItem: contentItems){
            if (contentItem == null){
                continue;
            }
            // 如果当前的subtype不是段落，则取下一个contentItem
            if (contentItem.getSubtype() == null || !contentItem.getSubtype().equals("paragraph")){
                continue;
            }
            curPara +=1;
            // 只要前五段，段数大于五段break返回
            if (curPara > 5){
                break;
            }

            String content = contentItem.getContent();
            List<String> contentTokens = processor.process(content);
            StringBuilder contentSb = new StringBuilder();

            // 处理当前的content
            for (String contentToken: contentTokens){
                contentSb.append(contentToken + " ");
                totalLengthInAll.add(1);
                if (QueriesTerms.contains(contentToken)){
                    termCountMap.put(contentToken, termCountMap.getOrDefault(contentToken,0) + 1);
                    accumulatorMap.get(contentToken).add(1);
                }
                articleLength++;
            }
            newContentItems.add(contentItem);
        }
        value.setContents(newContentItems);
        // 处理完这篇文章
        return new NewsCount(value,termCountMap,articleLength);
    }
}
