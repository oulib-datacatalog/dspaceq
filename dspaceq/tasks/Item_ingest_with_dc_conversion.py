import pandas as pd
import jinja2
from json import loads, dumps
import requests
from more_itertools import chunked

xml = """
<dublin_core>
{% if dc_title %}<dcvalue element='title' qualifier='none'>{{ dc_title }}</dcvalue>{% endif %}
{% if dc_subject_lsch %}<dcvalue element='subject' qualifier='lcsh'>{{ dc_subject_lsch }}</dcvalue>{% endif %}
{% if dc_subject_lsch_1 %}<dcvalue element='subject' qualifier='lcsh'>{{ dc_subject_lsch_1 }}</dcvalue>{% endif %}
{% if dc_description %}<dcvalue element='description' qualifier='none'>{{ dc_description }}</dcvalue>{% endif %}
{% if dc_description_abstract %}<dcvalue element='description' qualifier='abstract'>{{ dc_description_abstract }}</dcvalue>{% endif %}
{% if dc_date_issued %}<dcvalue element='date' qualifier='issued'>{{ dc_date_issued }}</dcvalue>{% endif %}
{% if dc_publisher %}<dcvalue element='publisher' qualifier='none'>{{ dc_publisher }}</dcvalue>{% endif %}
{% if dc_language %}<dcvalue element='language' qualifier='none'>{{ dc_language }}</dcvalue>{% endif %}
{% if dc_format %}<dcvalue element='format' qualifier='none'>{{ dc_format }}</dcvalue>{% endif %}
{% if dc_type %}<dcvalue element='type' qualifier='none'>{{ dc_type }}</dcvalue>{% endif %}
{% if dc_relation_uri %}<dcvalue element='relation' qualifier='uri'>{{ dc_relation_uri }}</dcvalue>{% endif %}
{% if dc_rights_uri %}<dcvalue element='rights' qualifier='uri'>{{ dc_rights_uri }}</dcvalue>{% endif %}
</dublin_core>
"""


df = pd.read_csv(<file path>, engine="python")
df.columns = [col.replace(".", "_") for col in df.columns] #replace . with _ for easier use with jinja2

def rename_bag(row):
    return("<bag_prefix>_{0}".format(row["path and file"].split('/')[3].replace("-", "_")))


df["bag"] = df.apply(rename_bag, axis=1)

env = jinja2.Environment(trim_blocks=True, lstrip_blocks=True)
template = env.from_string(xml)


def get_bags(url):
    """ iterate over pages of search results yielding bag metadata """
    data = loads(requests.get(url).text)
    while True:
        for item in data['results']:
            yield item
        if data['next'] is not None:
            data = loads(requests.get(data['next']).text)
        else:
            break

bags = list(get_bags(<json_url_query_string>))
for bag in bags:
    bagname = bag["bag"]
    pre_pages = bag[<directory>][<subdirectory>][<subdirectory>]
    chunked_pages = chunked(pre_pages, 100)
    for pages in chunked_pages:
        data = []
        for page in pages:
            file_lookup = page.split("/")[-1].replace(".jpg", ".tif")
            s3_filename = page.replace(<url>, "").replace("+", " ")
            attributes = df[(df.filename == file_lookup) & (df.bag == bagname)].to_dict(orient="record")[0]
            xml = template.render(**attributes)
            data.append({bagname: {'files': [s3_filename], 'metadata': xml}})

        print(data)

        ingest_args = {
            "function": "<function>",
            "queue": "<queue>",
            "args": [data, "<collection>"],
            "kwargs": {},
            "tags": []
        }

        headers = {<header>: <subheader>, <subheader>: <token_variable>.format(token)}
        req = requests.post("<task>/.json", data=dumps(ingest_args), headers=headers)
        print(req.content)

