import lucene
from java.nio.file import Paths
from org.apache.lucene.store import FSDirectory
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.queryparser.classic import MultiFieldQueryParser
from org.apache.lucene.search import BooleanClause

lucene.initVM()

INDEX_DIR = "lucene_index"

directory = FSDirectory.open(Paths.get(INDEX_DIR))
reader = DirectoryReader.open(directory)
searcher = IndexSearcher(reader)

analyzer = StandardAnalyzer()
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




def search(query_str, limit=10):
    print(f"\n Searching '{query_str}'...")

    # qp = QueryParser(field, analyzer)
    # Multi-field query parser
    # qp = MultiFieldQueryParser(SEARCH_FIELDS, analyzer)
    # qquery = qp.parse(StringReader(query_str))
    flags = [BooleanClause.Occur.SHOULD] * len(SEARCH_FIELDS)

    query = MultiFieldQueryParser.parse(
        query_str,
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

            # field = input("Field (title/description/tags/genre/starring/company): ")
            # if field.strip() == "":
            #     field = "title"

            search(q)

        except Exception as e:
            print("Error:", e)
