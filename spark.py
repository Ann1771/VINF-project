from pyspark.sql import SparkSession
import re, os
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from collections import OrderedDict

try:
    import html
    unescape = html.unescape
except ImportError:
    import HTMLParser
    unescape = HTMLParser.HTMLParser().unescape

spark = SparkSession.builder.appName("WikiExtraction").getOrCreate()
input_path = "file:///app/enwiki-latest-pages-articles.xml"

infobox_pattern = re.compile(
    r"\{\{Infobox\s*(television|tv|drama|series|show|web|serial|miniseries)[\s\|]",
    re.I
)


def _format_date(m):
    y = m.group(1)
    mm = m.group(2)
    dd = m.group(3)

    result = y
    if mm and mm.strip().isdigit():
        result += "-" + mm.strip()
    if dd and dd.strip().isdigit():
        result += "-" + dd.strip()

    return result


country_pattern = re.compile(
    r"^\|\s?(?:country|country_of_origin)\s?=[ ]*(.*?)(?=\n\|\s*[A-Za-z_]+\s*=|\n\}\}\n[^}|])",
    re.I | re.M | re.S
)

language_pattern = re.compile(
    r"^\|\s?(?:language|original_language)\s?=[ ]*(.*?)(?=\n\|\s*[A-Za-z_]+\s*=|\n\}\}\n[^}|])",
    re.I | re.M | re.S
)

import re

def clean_wikitext(val):
    if not val:
        return ""
    val = unescape(val)
    
    val = re.sub(r"\n\|\s*$", "", val)      
    # val = re.sub(r"\}\}\n\}\}$", "}}", val)
    val = re.sub(r"\n+", " ", val)          
    val = re.sub(
    r"\{\{\s*(?:start|end)[\s_]*date\b[^|}]*"
    r"(?:\|[^|=]*=[^|}]*)*"
    r"\|?(\d{4})(?:\|([^|}]*))?(?:\|([^|}]*))?[^}]*\}\}",
    _format_date,
    val,
    flags=re.I
    )


    
    val = re.sub(r"<ref[^/>]*/>", "", val, flags=re.S)
    val = re.sub(r"\{\{\s*(?:[Cc]ite|[Rr]ef|[Cc]itation)[^}]*\}\}", "", val, flags=re.S)
    val = re.sub(r"<ref[^>]*?>.*?</ref>", "", val, flags=re.S)
    val = re.sub(r"<ref[^>]*>", "", val, flags=re.S)
    val = re.sub(r"\{\{\s*[Nn]owrap\|([^{}]+)\}\}", r"\1", val)

    val = re.sub(r"\{\{\s*[Aa]bbr\|([^|}]+)\|[^}]+\}\}",r"\1", val)
    val = re.sub(r"\{\{\s*[Ii]ll\|([^|}]+)\|[^}]*\}\}", r"\1", val)
    val = re.sub(r"''+", "", val)
    val = re.sub(r"\{\{\s*small\|([^{}]+)\}\}", r"\1", val, flags=re.I)
   
    val = re.sub( r"\{\{\s*unreliable source[\s\S]*?\}\}", "", val, flags=re.I)
    val = re.sub(r"\{\{\s*[Ll]ang\|[^{}]*\}\}", "", val)

    val = re.sub(r"\{\{Korean/auto\|.*?\}\}", "", val, flags=re.S)
    val = re.sub(r"\{\{\s*(?:[Kk]orean|[Kk]o-h)\|[^{}]*\}\}", "", val)
    val = re.sub(r"\{\{\s*[Ee]fn[^}]*\}\}", "", val, flags=re.S)
    val = re.sub(r"\[\[(?!File:|Image:)(?:[^\|\]]*\|)?([^\]]+)\]\]", r"\1", val)
    val = re.sub(r"\{\{\s*[Bb]ased on\|([^|{}]+)\|([^{}]+)\}\}", r"\1 by \2", val)


    
    val = re.sub(
        r"\{\{\s*(?:KRW|USD|SK\ won)\s*\|\s*([^\}|]+?)\s*\}\}\s*(billion|million)?",
        lambda m: (
            m.group(1).replace("billion", "").replace("million", "").strip()
            + (
                " billion" if ("billion" in m.group(1) or m.group(2) == "billion") else
                (" million" if ("million" in m.group(1) or m.group(2) == "million") else "")
            )
            + " " + (
                "SK WON" if "SK" in m.group(0).upper() else
                "KRW" if "KRW" in m.group(0).upper() else
                "USD"
            )
        ),
        val,
        flags=re.I
    )


    val = re.sub(r"<!--.*?-->", "", val)  
    val = re.sub(r"\{\{interlanguage link\|[^{}]*?lt=([^|}]+)[^{}]*?\}\}",r"\1",val)
    val = re.sub(r"\{\{(?:PL|plainlist|ubl|flatlist|hlist|[Uu]nbulleted list|[Uu]nbulleted_list)\s*\|([^\}]+)\}\}", lambda m: ", ".join(m.group(1).split("|")), val, flags=re.I)
    
    val = re.sub(r"\[\[(?:File|Image):(?:(?!\]\]).)*\]\]", "", val, flags=re.I | re.S)   
    val = re.sub(r"\{\{\s*theme song\|([^\}]+)\}\}", lambda m: ", ".join(m.group(1).split("|")), val, flags=re.I)
    val = re.sub(r"\{\{\s*hidden[^}]*\}\}", "", val, flags=re.I | re.S)
    val = re.sub(r"^\s*\*\s?", "", val, flags=re.M)   
    val = re.sub(r"\s*\*\s*", ", ", val)
    val = re.sub(r"\{\{[Uu][Bb][Ll]\|([^\}]+)\}\}", lambda m: ", ".join(m.group(1).split("|")), val)
    val = re.sub(r"<br\s*/?\s*>", ", ", val, flags=re.I)
  
   

    val = re.sub(r"\(\s*\)", "", val)
   
  
    val = re.sub(r"\{\{[\s\S]*?\}\}", "", val)  
    
    val = re.sub(r"\}\}$", "", val  )
    # val = re.sub(r"\}\}+", "", val)
    val = re.sub(r"<[^>]+>", "", val)
  
    val = re.sub(r"===+\s*[^=]+===+", "", val)

    val = re.sub(r"\(\s*\)", "", val)
    val = re.sub(r"\{[\s\S]*?\}", "", val) 
    val = re.sub(r"\s+", " ", val).strip()  

    return val

# Attribute patterns
patterns = OrderedDict([
    ("alt_name",            r"^\|\s*alt_name\s*=[ ]*(.*?)(?=\n\|\s*[A-Za-z0-9_]+\s*=|\n\}\}\n[^}|])"),
    ("creator",             r"^\|\s*creator\s*=[ ]*(.*?)(?=\n\|\s*[A-Za-z0-9_]+\s*=|\n\}\}\n[^}|])"),
    ("based_on",            r"^\|\s?(?:based_on|adaptation)\s?=[ ]*(.*?)(?=\n\|\s*[A-Za-z0-9_]+\s*=|\n\}\}\n[^}|])"),
    ("writer",              r"^\|\s?(?:writer|written_by)\s?=[ ]*(.*?)(?=\n\|\s*[A-Za-z0-9_]+\s*=|\n\}\}\n[^}|])"),
    ("director",            r"^\|\s?(?:director|directed_by)\s?=[ ]*(.*?)(?=\n\|\s*[A-Za-z0-9_]+\s+=|\n\}\}\n[^}|])"),

    ("starring",            r"^\|\s?starring\s?=[ ]*(.*?)(?=\n\|\s*[A-Za-z0-9_]+\s*=|\n\}\}\n[^{}|-])"),
    ("music",               r"^\|\s?(?:music|music_by|theme_music_composer)\s?=[ ]*(.*?)(?=\n\|\s*[A-Za-z0-9_]+\s*=|\n\}\}\n[^}|])"),
    ("opening_theme",       r"^\|\s?(?:opening_theme|open_theme)\s?=[ ]*(.*?)(?=\n\|\s*[A-Za-z0-9_]+\s*=|\n\}\}\n[^}|])"),
    ("ending_theme",        r"^\|\s?(?:ending_theme|end_theme)\s?=[ ]*(.*?)(?=\n\|\s*[A-Za-z0-9_]+\s*=|\n\}\}\n[^}|])"),

    ("executive_producer",  r"^\|\s?executive_producer\s?=[ ]*(.*?)(?=\n\|\s*[A-Za-z0-9_]+\s*=|\n\}\}\n[^}|])"),
    ("producer",            r"^\|\s?(?:producer|produced_by)\s?=[ ]*(.*?)(?=\n\|\s*[A-Za-z0-9_]+\s*=|\n\}\}\n[^}|])"),
    ("camera_setup",        r"^\|\s?(?:camera_setup|camera)\s?=[ ]*(.*?)(?=\n\|\s*[A-Za-z0-9_]+\s*=|\n\}\}\n[^}|])"),
    ("runtime",             r"^\|\s?(?:runtime|running_time)\s?=[ ]*(.*?)(?=\n\|\s*[A-Za-z0-9_]+\s*=|\n\}\}\n[^}|])"),
    ("budget",              r"^\|\s?budget\s?=[ ]*(.*?)(?=\n\|\s*[A-Za-z0-9_]+\s+=|\n\}\}\n[^}|])"),
    ("location",            r"^\|\s?location\s?=[ ]*(.*?)(?=\n\|\s*[A-Za-z0-9_]+\s*=|\n\}\}\n[^}|])"),
    ("company",             r"^\|\s?(?:company|production_company|production_companies)\s?=[ ]*(.*?)(?=\n\|\s*[A-Za-z0-9_]+\s*=|\n\}\}\n[^}|])"),
    ("network",             r"^\|\s?(?:network|original_network|channel)\s?=[ ]*(.*?)(?=\n\|\s*[A-Za-z0-9_]+\s*=|\n\}\}\n[^}|]|\n}}{{Unreliable sources)"),

    ("first_aired",         r"^\|\s?(?:first_aired|release|original_release|aired)\s?=[ ]*(.*?)(?=\n\|\s*[A-Za-z0-9_]+\s*=|\n\}\}\n[^}|])"),
    ("last_aired",          r"^\|\s?(?:last_aired|ended)\s?=[ ]*(.*?)(?=\n\|\s*[A-Za-z0-9_]+\s*=|\n\}\}\n[^}|]|\n}}')"),
    ("num_seasons",         r"^\|\s?(?:num_seasons|seasons)\s?=[ ]*(.*?)(?=\n\|\s*[A-Za-z0-9_]+\s*=|\n\}\}\n[^}|])"),
    ("num_episodes",        r"^\|\s?(?:num_episodes|episodes)\s?=[ ]*(.*?)(?=\n\|\s*[A-Za-z0-9_]+\s*=|\n\}\}\n[^}|])"),

    ("genre",               r"^\|\s?genre\s?=[ ]*(.*?)(?=\n\|\s*[A-Za-z0-9_]+\s+=|\n\}\}\n[^}|])"),

    ("language",            r"^\|\s?(?:language|original_language)\s?=[ ]*(.*?)(?=\n\|\s*[A-Za-z0-9_]+\s*=|\n\}\}\n[^}|])"),
    ("country",             r"^\|\s?(?:country|country_of_origin)\s?=[ ]*(.*?)(?=\n\|\s*[A-Za-z0-9_]+\s*=|\n\}\}\n[^}|])"),

])



def extract_attr(pattern, text):
    m = re.search(pattern, text, re.I | re.M | re.S)
    if not m:
        return None
    if m.groups():
        val = m.group(1)
    else:
        val = m.group(0)

    val = clean_wikitext(val)

    if isinstance(val, list):
        val = ", ".join(map(str, val))

    return str(val).strip()



def extract_kdrama_data(partition):
    xml = "\n".join(partition)
    found = 0 
    for page in re.findall(r"<page>(.*?)</page>", xml, re.S):
        
        
        page = re.sub(r"[^\x00-\x7F]", " ", page)
        page = re.sub(r"[ \t]+", " ", page).strip()
        page =re.sub(r"\n+","\n",page)

        title = re.search(r"<title>(.*?)</title>", page)
        page_id = re.search(r"<id>(\d+)</id>", page)
        timestamp = re.search(r"<timestamp>(.*?)</timestamp>", page)
        username = re.search(r"<username>(.*?)</username>", page)
        text_match = re.search(r"<text[^>]*>(.*?)</text>", page, re.S)
        text = text_match.group(1) if text_match else ""
        text = re.sub(r"[ \t]+", " ", text).strip()

        if not text:
            continue

        synopsis_match = re.search(r"==\s*Synopsis\s*==\s*(.*?)(?=\n==|$)", text, re.S | re.I)
        if not synopsis_match:
            synopsis_match = re.search(r"==\s*Plot\s*==\s*(.*?)(?=\n==|$)", text, re.S | re.I)
        synopsis = clean_wikitext(synopsis_match.group(1)) if synopsis_match else ""

        if not infobox_pattern.search(text):
            continue
        country_match = country_pattern.search(text)
        language_match = language_pattern.search(text)      
        country_val = clean_wikitext(country_match.group(1)).lower() if country_match else ""
        lang_val = clean_wikitext(language_match.group(1)).lower() if language_match else ""


        if not ("south korea" in country_val and "korean" in lang_val):
            continue

        attrs = {k: extract_attr(v, text) for k, v in patterns.items()}
        values = [
            str(page_id.group(1) if page_id else ""),
            str(title.group(1) if title else ""),
            str(timestamp.group(1) if timestamp else ""),
            str(username.group(1) if username else "")
        ]
        values.extend([str(attrs.get(k, "") or "") for k in patterns])
        values.extend([
            synopsis
        ])

        yield tuple(values)

columns = ["wiki_id", "title", "timestamp", "username"] + list(patterns.keys()) +["synopsis"]

rdd = spark.sparkContext.textFile(input_path, minPartitions=100)
wiki_df = spark.createDataFrame(rdd.mapPartitions(extract_kdrama_data), columns)

unescape_udf = udf(lambda x: unescape(x) if x else x, StringType())

for c, t in wiki_df.dtypes:
    if t == "string":
        wiki_df = wiki_df.withColumn(c, unescape_udf(c))

output_dir = "/app/output/wiki_kdrama_final_parquet"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

wiki_df.write.mode("overwrite").option("header", True).parquet("file://" + output_dir)

print("Extraction complete! Found Korean drama pages with attributes.")
spark.stop()
