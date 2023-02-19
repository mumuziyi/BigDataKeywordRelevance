package uk.ac.gla.dcs.bigdata.MyMaps;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.MyStructure.NewsCount;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;

import java.util.*;

public class NewsToCountMap implements MapFunction<NewsArticle, NewsCount> {

    private static final long serialVersionUID = -1912835801896003830L;
    Broadcast<Set<String>> broadcastQueryList;

    Map<String, LongAccumulator> accumulatorMap;

    public NewsToCountMap(Broadcast<Set<String>> broadcastQueryList,Map<String, LongAccumulator> accumulatorMap){
        this.broadcastQueryList = broadcastQueryList;

        this.accumulatorMap = accumulatorMap;
    }
    @Override
    public NewsCount call(NewsArticle value) throws Exception {

        // 如果title为空，不计入
        if (value.getTitle() == null){
            return new NewsCount(value,new HashMap<>());
        }

        // 所有terms的集合
        Set<String> QueriesTerms = broadcastQueryList.value();

        TextPreProcessor processor = new TextPreProcessor();

        // 处理文章标题terms
        String title = value.getTitle();
        List<String> titleList = processor.process(title);

        // 要返回的map，其中包括此文章所含的query term以及相应的个数
        Map<String,Integer> termCountMap = new HashMap<>();


        // 处理title的所含的term
        // 遍历title，如果发现title中当前的单词属于query 的term， 就放进去
        for (String titleTerm: titleList){
            if (QueriesTerms.contains(titleTerm)){
                termCountMap.put(titleTerm, termCountMap.getOrDefault(titleTerm,0) + 1);
                // 在accumulator中加，用来保存每个term在所有文章中出现的次数
                accumulatorMap.get(titleTerm).add(1);
            }
        }

        // 处理正文
        List<ContentItem> contentItems = value.getContents();
        // 记录当前段落
        int curPara = 0;
        // 遍历newsArticle的所有contentItem
        for (ContentItem contentItem: contentItems){
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

            // 处理当前的content
            for (String contentToken: contentTokens){
                if (QueriesTerms.contains(contentToken)){
                    if (contentToken.equals("financ")){
                        System.out.println("=======================");
                    }
                    termCountMap.put(contentToken, termCountMap.getOrDefault(contentToken,0) + 1);
                    accumulatorMap.get(contentToken).add(1);
                }
            }

        }
        // 处理完这篇文章

        return new NewsCount(value,termCountMap);
    }
}
