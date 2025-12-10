import csv, json
from collections import defaultdict
import os 

VOCAB = "index/vocab.tsv"
POSTINGS = "index/postings.tsv"
DOCS = "index/docs.tsv"
ENTITIES = "entities.jsonl"

CUSTOM_STOPWORDS = {
    "recap", "review", "reviews", "drama", "kdrama", "k drama", "show", "cast", "scene", 
     "plot", "recaps", "tv", "korean"
}
def load_index():
    vocab = {}
    with open(VOCAB, encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            term = row["term"]
            vocab[term] = {
                "idf_ordinary": float(row["idf_ordinary"]),
                "idf_log": float(row["idf_log"])
            }

    postings = defaultdict(list)
    with open(POSTINGS, encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            term = row["term"]
            plist = row["postings"].split(", ")
            for pair in plist:
                doc, tf = pair.strip("()").split(":")
                postings[term].append((int(doc), float(tf)))
    return vocab, postings

def load_docs():
    docs = {}
    with open(DOCS, encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            docs[int(row["doc_id"])] = row["path"]
    return docs

def load_entities():
    entities = {}
    if not os.path.exists(ENTITIES):
        return entities

    with open(ENTITIES, encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
                file = obj.get("file", "")
                entities[file] = obj
            except json.JSONDecodeError:
                continue
    return entities


def search(query, vocab, postings, idf_type="log"):
    query_terms = [t.lower() for t in query.split() if t.lower() not in CUSTOM_STOPWORDS]
    scores = defaultdict(float)

    # For each term in the query, compute TF–IDF score for matching documents
    for term in query_terms:
        if term not in postings:
            continue
        for doc_id, tf in postings[term]:
            idf = vocab[term][f"idf_{idf_type}"]
            scores[doc_id] += tf * idf

    # Sort documents by score from highest to lowest
    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    return ranked


if __name__ == "__main__":
    vocab, postings = load_index()
    docs = load_docs()
    entities = load_entities()
    
    while True:
        query = input("Enter your query: ").strip()
        if not query or query.lower() in ["exit", "quit"]:
            print("End")
            break

        for idf_type in ["ordinary", "log"]:
            results = search(query, vocab, postings, idf_type=idf_type)
            print(f"\nResults for IDF {idf_type}")
            if not results:
                print("No results found")
                continue
            for doc_id, score in results[:5]:
                html_path = docs[doc_id].replace("data-extracted", "data-filtered").replace(".txt", ".html")
                file_name = os.path.basename(html_path)

                # print info about pages
                meta = entities.get(file_name, {})
                title = meta.get("title", "(no title)")
                description = meta.get("description", "")
                url = meta.get("url", "")
                tags = ", ".join(meta.get("tags", []))

                print(f"\nDoc {doc_id:3d} | Score={score:.3f}")
                print(f"Title: {title}")
                if description:
                    print(f"Desc:  {description[:120]}{'...' if len(description)>120 else ''}")
                if tags:
                    print(f"Tags:  {tags}")
                if url:
                    print(f"URL:   {url}")
                print(f"File:  {html_path}")






