from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, regexp_replace, trim, concat_ws, lit, split, concat, expr, size, explode, collect_list, explode_outer,array_except, array
)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, length, col

spark = SparkSession.builder.appName("JoinCrawlerWithWiki").getOrCreate()

fields = [
    "creator","based_on","writer","director","starring","music","opening_theme",
    "ending_theme","executive_producer","producer",
    "camera_setup","runtime","budget","location","company"
    ,"first_aired","last_aired","num_seasons","num_episodes","genre"
]

all_empty_condition = " AND ".join(["{} = ''".format(f) for f in fields])


crawler_path = "file:///app/entities.jsonl"
crawler_df = spark.read.option("multiLine", "false").json(crawler_path).select(
    col("title"),
    col("url"),
    col("description"),
    col("domain"),
    col("author"),
    col("date"),
    col("tags")
)

crawler_df = (
    crawler_df
    .withColumn("title_clean",lower(col("title")))
    .withColumn("title_clean", trim(regexp_replace(col("title_clean"), r"[^a-z0-9\s]", "")))
    .withColumn("title_clean", regexp_replace(col("title_clean"), r"\s+", " "))  
    .withColumn("title_clean", trim(col("title_clean")))
    .filter(col("title_clean") != "")
)

# tags
tags_exploded = crawler_df.select("*", explode_outer(col("tags")).alias("tag_item")
)

tags_exploded = tags_exploded.withColumn(
    "tag_clean", trim( regexp_replace(regexp_replace(lower(col("tag_item")), r"[^a-z0-9\s]", ""),
            r"\s+",
            " "
        )
    )
)


crawler_df = tags_exploded.groupBy(
    "title", "title_clean",
    "url", "description", "domain", "author", "date", "tags"
).agg(
    collect_list("tag_clean").alias("tags_clean")
)

crawler_df = crawler_df.withColumn(
    "tags_clean",
    array_except(col("tags_clean"), array(lit("romance"), lit("family")))
)

wiki_path = "file:///app/output/wiki_kdrama_final1_parquet"
wiki_df = spark.read.parquet(wiki_path).select(
    col("title").alias("wiki_title"),
    col("alt_name"),
    col("wiki_id"),
    col("timestamp"),
    col("username"),
    col("synopsis"),
    col("creator"),
    col("based_on"),
    col("writer"),
    col("director"),
    col("starring"),
    col("music"),
    col("opening_theme"),
    col("ending_theme"),
    col("executive_producer"),
    col("producer"),
    col("camera_setup"),
    col("runtime"),
    col("budget"),
    col("location"),
    col("company"),
    col("network"),
    col("first_aired"),
    col("last_aired"),
    col("num_seasons"),
    col("num_episodes"),
    col("genre"),
    col("language"),
    col("country"),
)

# wiki title
wiki_df = (
    wiki_df
    .withColumn("wiki_title_clean",lower(col("wiki_title")))
    .withColumn("wiki_title_clean", regexp_replace(col("wiki_title_clean"), r"\(.*?\)", ""))
    .withColumn("wiki_title_clean", regexp_replace(col("wiki_title_clean"), r"[^a-z0-9\s]", ""))
    .withColumn("wiki_title_clean", regexp_replace(col("wiki_title_clean"), r"\s+", " "))
    .withColumn("wiki_title_clean", trim(col("wiki_title_clean")))
)
   

wiki_df = wiki_df.filter("NOT ({})".format(all_empty_condition))

# wiki alt names
wiki_df = (
    wiki_df
    .withColumn("alt_name_clean", regexp_replace(lower(col("alt_name")), r"[^a-z0-9\s,]", ""))
    .withColumn("alt_name_clean", regexp_replace(col("alt_name_clean"), r"\s+", " "))
    .withColumn("alt_name_list", split(col("alt_name_clean"), ","))
    .withColumn("alt_name_list", expr("transform(alt_name_list, x -> trim(x))"))
    .withColumn("alt_name_list", expr("filter(alt_name_list, x -> x != '')"))
)

wiki_alt = wiki_df.select(
    "*",
    explode_outer(col("alt_name_list")).alias("alt_name_item")
)



join_tags = crawler_df.crossJoin(wiki_df).filter(
    expr("array_contains(tags_clean, wiki_title_clean)")
).withColumn("match_type", lit("tags"))


join_title_raw = (
    crawler_df.crossJoin(wiki_df)
    .filter(
        expr("title_clean RLIKE concat('(^| )', wiki_title_clean, '( |$)')")
    )
    .withColumn("match_type", lit("title"))
)

join_alt_raw = (
    crawler_df.crossJoin(wiki_alt)
    .filter(
        expr("title_clean RLIKE concat('(^| )', alt_name_item, '( |$)')")
    )
    .withColumn("match_type", lit("alt_name"))
)

join_title_raw = join_title_raw.withColumn("alt_name_item", lit(None))
join_title_raw = join_title_raw.withColumn("match_value", col("wiki_title_clean"))
join_title_raw = join_title_raw.withColumn("match_type", lit("title"))
join_alt_raw = join_alt_raw.withColumn("match_value", col("alt_name_item"))
join_alt_raw = join_alt_raw.withColumn("match_type", lit("alt_name"))


join_tags = join_tags.withColumn("alt_name_item", lit(None))

title_alt = (
    join_title_raw
    .unionByName(join_alt_raw)
)
title_alt = title_alt.withColumn("match_len", length(col("match_value")))

w_best = Window.partitionBy("title_clean").orderBy(col("match_len").desc())

title_alt_best = (
    title_alt
    .withColumn("rn", row_number().over(w_best))
    .filter(col("rn") == 1)
    .drop("rn", "match_len")
    .drop("match_value") 
)


joined_df = (
    title_alt_best
    .unionByName(join_tags)
)


joined_df = joined_df.dropDuplicates(["title_clean", "wiki_title_clean"])
joined_df = joined_df.select(
    "match_type","alt_name_item","alt_name","title","title_clean", 
    "wiki_title_clean", "wiki_title",  "url", "description", "domain", "author", "date", "tags",
    "wiki_id", "timestamp", "username", "synopsis", "creator","based_on",
    "writer","director","starring","music",
    "opening_theme","ending_theme","executive_producer","producer",
    "camera_setup","runtime","budget","location","company","network","first_aired",
    "last_aired","num_seasons","num_episodes","genre","language","country"
)
for c in joined_df.columns:
    joined_df = joined_df.withColumn(
        c,
        regexp_replace(col(c).cast("string"), r"[\r\n\t]+", " ")
    )
unique_wiki = wiki_df.select("wiki_title").distinct().count()
joined_count = joined_df.count()


joined_df = joined_df.withColumn("tags", concat_ws("|", col("tags")))

output_path = "file:///app/output/joined_parquet"
joined_df.coalesce(1).write.mode("overwrite").parquet(output_path)

print("Joined dataset saved successfully.")
print("Number of unique wiki pages:", unique_wiki)
print("Number of joined records:", joined_count)

spark.stop()
