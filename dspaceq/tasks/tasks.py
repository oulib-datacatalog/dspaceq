#from celery.task import task
from lxml import etree
from itertools import compress
from json import loads
import logging
import requests
import boto3


logging.basicConfig(level=logging.INFO)

base_url = "https://cc.lib.ou.edu"
digital_object_url = "{0}/api/catalog/data/catalog/digital_objects".format(base_url)
digital_object_search = "{0}/.json?query={{\"filter\":{{\"project\":\"private\",\"bag\":{{\"$regex\":\"^share*\"}}}}}}".format(digital_object_url)
# search string on digital_objects  {"filter":{"project":"private","bag":{"$regex":"^share*"}}}
etd_search = "{0}/api/catalog/data/catalog/etd/.json?query={{\"filter\":{{\"ingested\":{{\"$ne\":true}}}}}}".format(base_url)
# search string on etd  {"filter":{"ingested":{"$ne":true}}}
alma_url = "https://api-na.hosted.exlibrisgroup.com/almaws/v1/bibs/{0}?expand=None&apikey={1}"


#Example task
#@task()
def add(x, y):
    """ Example task that adds two numbers or strings
        args: x and y
        return addition or concatination of strings
    """
    result = x + y
    return result


def _get_config_parameter(group,param,config_file="cybercom.cfg"):
    config = ConfigParser() #ConfigParser.ConfigParser()
    config.read(config_file)
    return config.get(group,param)


def get_mmsid(bag):
    """ get the mmsid from end of bag name """
    mmsid = bag.split("_")[-1].strip()  # using bag name formatting: 1990_somebag_0987654321
    if re.match("^[0-9]+$", mmsid):  # check that we have an mmsid like value
        return mmsid
    return None


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


def get_bib_record(mmsid):
    try:
        result = requests.get(alma_url.format(mmsid, alma_key))
        if result.status_code == requests.codes.ok:
            return result.content
    except:
        return {"error": "Alma Connection Error - try again later."}


def get_marc_from_bib(bib_record):
    """ returns marc xml from bib record string"""
    record = etree.fromstring(bib_record).find("record")
    record.attrib['xmlns'] = "http://www.loc.gov/MARC21/slim"
    return etree.ElementTree(record)

def marc_xml_to_dc_xml(marc_xml):
    """ returns dublin core xml from marc xml """
    marc2dc_xslt = etree.parse('xlst/MARC21slim2RDFDC.xsl')
    transform = etree.XSLT(marc2dc_xslt)
    return transform(marc_xml)


def validate_marc(marc_xml):
    with open('xlst/MARC21slim.xsd') as f:
        schema = etree.XMLSchema(etree.fromstring(f.read()))
    parser = etree.XMLParser(schema=schema)
    return etree.fromstring(etree.tostring(marc_xml), parser)


def bib_to_dc(bib_record):
    return marc_xml_to_dc_xml(validate_marc(get_marc_from_bib(bib_record)))


def list_s3_files(bag_name):
    s3_bucket='ul-bagit'
    s3_destination='private/shareok/{0}/data/'.format(bag_name)
    s3 = boto3.client('s3')
    files = [x['Key'] for x in s3.list_objects(Bucket=s3_bucket, Prefix=s3_destination)['Contents']]
    return [x for x in files if x.endswith((".pdf", ".txt"))]


def missing_or_blank(xpath_val):
    """ check if xpath is missing or blank """
    results = root.xpath(xpath_val)
    if len(results) > 0:
        return results[0].text == ""
    else:
        return True
 

def missing_fields(bib_record):
    # Fields to verify
    xpath_lookup = {
        "Title": "record/datafield[@tag=245]",
        "Author": "record/datafield[@tag=100]",
        "Publish Year": "record/datafield[@tag=264]|record/datafield[@tag=260]",
        "Thesis/Diss Tag": "record/datafield[@tag=502]|record/datafield[@tag=500]",
        "School": "record/datafield[@tag=690]",
        "Subject Heading": "record/datafield[@tag=650]"
    }
    if bib_record is not None and type(bib_record) is not dict:
        root = etree.fromstring(bib_record)            
        missing = map(missing_or_blank, xpath_lookup.values())
        return list(compress(xpath_lookup.keys(), missing))
    elif type(bib_record) is dict:
        return bib_record.values()  # The lookup of the bib_record failed pass along error message
    return ["Could not find record!"]


def check_missing(mmsids):
    """ Checks for missing fields stored in Alma """
    mmsids = [mmsids] if type(mmsids) != list else mmsids
    bib_records = map(get_bib_record, mmsids)
    missing = map(missing_fields, bib_records)
    return zip(mmsids, missing)



#@task()
def notify_etd_missing_fields():
    """
    Sends email to collections to notify of missing fields in Alma
    for requested Theses and Disertations
    """
    logging.info("Sending email notification of missing alma fields to collections")
    mmsids = [record.get("mmsid") for record in get_bags(etd_search) if record.get("mmsid") is not None]
    missing = map(check_missing, mmsids)
    print(missing)


#@task()
def ingest_thesis_disertation(bag, collection, eperson="libir@ou.edu"):
    """
    Ingest a bagged thesis or disertation into dspace

    args:
       bag (string); Name of bag to ingest
       collection (string); dspace collection id to load into
       eperson (string); email address to send notification to
    """

    logging.info("Processing bag: {0}\nCollection: {1}\nEperson: {2}".format(bag, collection, eperson))

    # Copy files from bag in S3 to staging area
    files = list_s3_files(bag_name)


    # Generate SAF package
    # Ingest SAF package
    # Cleanup


def _update_alma_url_field(bib_record, url):
    """ Updates the url text for the first instance of tag 856(ind1=4, ind2=0) and returns as string
        
        args:
          bib_record (string); bib record xml 
          url (string); url to place in the record
    """
    tree = etree.XML(bib_record)
    url_element = tree.find(".//*[@tag='856'][@ind1='4'][@ind2='0']/subfield[@code='u']")
    if url_element is None:
        record_tree = tree.find(".//record")
        datafield = etree.Element("datafield", ind1="4", ind2="0", tag="856")
        subfield = etree.Element("subfield", code="u")
        subfield.text = url
        datafield.append(subfield)
        record_tree.append(datafield)
    else:
        url_element.text = url
    return etree.tostring(tree, standalone="yes", encoding="UTF-8")


#@task()
def update_alma_url_field(mmsid, url):
    """
    Updates the Electronic location (tag 856) in Alma with the URL

    args:
       mmsid (string); MMSID of the object to update in Alma
       url (string); the corresponding url in ShareOK/Commons for the the object
    """

    """
    Example:
        <datafield ind1="4" ind2="0" tag="856">
        <subfield code="u">https://shareok.org/something/somethingelse/123454321/blah/blah</subfield>
        </datafield>
    """
    result = requests.get(alma_url.format(mmsid, alma_key))
    if result.status_code == requests.codes.ok:
        new_xml = _update_alma_url_field(result.content, url)
        put_result = requests.put(url=url, data=new_xml, headers={"content-type": "application/xml"})
        if put_result.status_code == requests.codes.ok:
            return "Updated record"
        else:
            return {"error": "Could not update record"}
    else:
        return {"error": "Could not access alma"}
        
