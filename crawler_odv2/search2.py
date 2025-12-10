import lucene
import re
from java.nio.file import Paths
from org.apache.lucene.store import FSDirectory
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.queryparser.classic import MultiFieldQueryParser
from org.apache.lucene.search import BooleanClause
from org.apache.lucene.search import BooleanQuery, BooleanClause, TermQuery, BoostQuery
from org.apache.lucene.index import Term


lucene.initVM()

INDEX_DIR = "lucene_index"

directory = FSDirectory.open(Paths.get(INDEX_DIR))
reader = DirectoryReader.open(directory)
searcher = IndexSearcher(reader)

analyzer = StandardAnalyzer()
CUSTOM_STOPWORDS = {
    "episode", "episodes", "recap", "review", "reviews","kdramas", "dramas", "drama","kdrama",
    "series", "season", "show", "actor", "actress", "cast", "scene", 
    "story", "plot", "recaps", "tv", "korean"
}

SEARCH_FIELDS = [
    "title",
    "description",
    "tags",
    "alt_name",
    "synopsis",  
    "creator",
    "based_on",
    "writer",
    "director",
    "starring",
    "music",
    "opening_theme",
    "ending_theme",
    "producer",
    "location",
    "company",
    "genre",
    "wiki_title",
    "network",
    "language",
    "country",
    "executive_producer",
    "first_aired",   
    "last_aired",
    "num_seasons",
    "num_episodes"     
]



def clean_query(text):
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    tokens = text.split()
    tokens = [t for t in tokens if t not in CUSTOM_STOPWORDS]
    return " ".join(tokens)


def search(query_str, limit=10):
    print(f"\n Searching '{query_str}'...")

  

    cleaned = clean_query(query_str)
    flags = [BooleanClause.Occur.SHOULD] * len(SEARCH_FIELDS)

    query = MultiFieldQueryParser.parse(
        cleaned,
        SEARCH_FIELDS,
        flags,
        analyzer
    )

    hits = searcher.search(query, limit).scoreDocs

    print(f"Found {len(hits)} results:\n")

    for hit in hits:
        doc = searcher.storedFields().document(hit.doc)
        print("------------------------------------------------")
        print(f"Score: {hit.score:.3f}")
        skip_fields = {"description", "synopsis"}

        for f in doc.getFields():
            name = f.name()
            if name in skip_fields:
                continue  

            value = doc.get(name)

            if value is not None and value.strip() != "":
                print(f"{name}: {value}")

        print("------------------------------------------------")


if __name__ == "__main__":
    while True:
        try:
            q = input("\nEnter query (or 'exit'): ")
            if q.lower() == "exit":
                break


            search(q)

        except Exception as e:
            print("Error:", e)
