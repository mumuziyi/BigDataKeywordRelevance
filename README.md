这个程序是使用Spark完成的一个搜索项目

这个程序可以按照data/queries.list中的关键词，对TREC_Washington_Post_collection.v3.example.json这个文件中各个文件的相关度进行排序。
并将排序结果输出到result中。

运行方式：
使用Maven导入包之后，直接运行MapReduceMain，然后在Result中查看每个关键词的所对应的文章相关度。

我目前正在整理从Hadoop到Spark的各种原理，可以先阅读[大数据 数据处理原理](https://juejin.cn/column/7238519458624716861)这个专栏，后续
会对这个程序进行详细讲解。





