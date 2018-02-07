from celery.task import task
from celery import signature, group, Celery
from bson.objectid import ObjectId
from json import loads, dumps
import logging
import requests
import jinja2

from utils import *
from config import alma_url, digital_object_url

from celeryconfig import ALMA_KEY, ALMA_RW_KEY, ETD_NOTIFICATION_EMAIL, ALMA_NOTIFICATION_EMAIL, REST_ENDPOINT
import celeryconfig

logging.basicConfig(level=logging.INFO)

app = Celery()
app.config_from_object(celeryconfig)


#Example task
@task()
def add(x, y):
    """ Example task that adds two numbers or strings
        args: x and y
        return addition or concatination of strings
    """
    result = x + y
    return result


@task()
def notify_etd_missing_fields():
    """
    Sends email to collections to notify of missing fields in Alma
    for requested Theses and Disertations
    """
    emailtmplt = """The following ETD requests have missing fields:
    {% for bag in bags %}========================
      mmsid: {{ bags[bag].mmsid }}
      Missing Details:{% for field in bags[bag].missing %}
        {{ field }}{% endfor %}
      Files:{% for etd_file in bags[bag].files %}
        {{ etd_file }}{% endfor %}  
    {% endfor %}
    """

    mmsids = get_requested_mmsids()
    digitized_bags = list(get_digitized_bags(mmsids))
    digitized_mmsids = [get_mmsid(bag) for bag in digitized_bags]
    missing = [item for item in check_missing(digitized_mmsids) if item[1] != []]
    bags_missing_details = {}
    for bag in digitized_bags:
        for mmsid, items in missing:
            if mmsid in bag:
                bags_missing_details[bag] = {}
                bags_missing_details[bag]['mmsid'] = mmsid
                bags_missing_details[bag]['missing'] = items
                bags_missing_details[bag]['files'] = ["https://s3.amazonaws.com/{0}".format(x) for x in list_s3_files(bag)]
    if bags_missing_details:
        env = jinja2.Environment()
        tmplt = env.from_string(emailtmplt)
        msg = tmplt.render(bags=bags_missing_details)
        send_email = signature(
           "emailq.tasks.tasks.sendmail",
           kwargs={
               'to': ETD_NOTIFICATION_EMAIL,
               'subject': 'Missing ETD Fields',
               'body': msg
               })
        send_email.delay()
        logging.info("Sent ETD notification email")
        return "Notification Sent"
    logging.info("No missing attributes - no notification email")
    return "No Missing Details"


@task()
def ingest_thesis_dissertation(bag, collection="", dspace_endpoint="", eperson="libir@ou.edu"):
    """
    Ingest a bagged thesis or dissertation into dspace

    args:
       bag (string); Name of bag to ingest
       collection (string); dspace collection id to load into - if blank will determine from Alma
       dspace_endpoint (string); url to shareok / commons API endpoint - example: https://test.shareok.org/rest
       eperson (string); email address to send notification to
    """

    if collection == "":
        mmsid = get_mmsid(bag)
        bib_record = get_bib_record(mmsid)
        if type(bib_record) is not dict:
            collection = guess_collection(bib_record)
        else:
            logging.error("failed to get bib_record to determine")
            return bib_record  # failed - pass along error message

    logging.info("Processing bag: {0}\nCollection: {1}\nEperson: {2}".format(bag, collection, eperson))
    
    # files to include in ingest
    files = list_s3_files(bag)
    logging.info("Using files: {0}".format(files))

    mmsid = get_mmsid(bag)
    dc = bib_to_dc(get_bib_record(mmsid))

    items = [{bag: {"files": files, "metadata": dc}}]
   
    # TODO: add chain to update alma with corresponding url and update data catalog
    ingest = signature(
            "libtoolsq.tasks.tasks.awsDissertation", 
            queue="shareok-repotools-prod-workerq",
            kwargs={"dspaceapiurl":dspace_endpoint, "collectionhandle":collection, "items":items}
            )
    echo = signature(
        "dspaceq.tasks.tasks.echo_results",
        queue="shareok-dspace5xtest-test-workerq"
    )
    update_alma = update_alma_url_field.s()
    chain = (ingest | group(echo, echo_results.s()))
    chain.delay()
    return "Kicked off ingest for: {0}".format(bag)


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


@task()
def update_alma_url_field(mmsid, url, notify=True):
    """
    Updates the Electronic location (tag 856) in Alma with the URL

    args:
       mmsid (string); MMSID of the object to update in Alma
       url (string); the corresponding url in ShareOK/Commons for the the object
       notify (boolean); notify alma team of update - default is true
    """

    """
    Example:
        <datafield ind1="4" ind2="0" tag="856">
        <subfield code="u">https://shareok.org/something/somethingelse/123454321/blah/blah</subfield>
        </datafield>
    """
    msg = "URL for Alma record {0} has been changed\nfrom: {1}\nto: {2}"
    result = requests.get(alma_url.format(mmsid, ALMA_KEY))
    if result.status_code == requests.codes.ok:
        old_url = get_alma_url_field(result.content)
        new_xml = _update_alma_url_field(result.content, url)
        put_result = requests.put(url=alma_url.format(mmsid, ALMA_RW_KEY), data=new_xml, headers={"content-type": "application/xml"})
        if put_result.status_code == requests.codes.ok:
            logging.info("Alma record updated for mmsid: {0}".format(mmsid))
            if notify == True:
                send_email = signature(
                    "emailq.tasks.tasks.sendmail",
                    kwargs={
                    'to': ALMA_NOTIFICATION_EMAIL,
                    'subject': 'ETD Record Updated',
                    'body': msg.format(mmsid, old_url, url)
                })
                send_email.delay()
                logging.info("Sent Alma notification email")
            return "Updated record"
        else:
            logging.error("Could not update record: {0}".format(mmsid))
            return {"error": "Could not update record"}
    else:
        # TODO: Add retry
        logging.error("Could not access alma")
        return {"error": "Could not access alma"}


@task()
def remove_etd_catalog_record(id):
    """
    Removes the specified record from the etd digital catalog

    args:
      id (string); this is the value specified by "_id" in the digital catalog record
    """

    db_client = app.backend.database.client

    if "catalog" not in db_client.database_names():
        return {"error": "catalog is missing"}
    if "etd" not in db_client.catalog.collection_names():
        return {"error": "etd collection is missing"}

    etd = db_client.catalog.etd
    record = etd.find_one({'_id': ObjectId(id)})
    if record:
        #etd.remove({'_id': ObjectId(id)})
        etd.delete_one({'_id': ObjectId(id)})  # limit to at most one record
        logging.info("Removed {0} from etd collection")
        return "Record {0} has been removed".format(id)
    else:
        return {"error": "Record {0} not found"}


@task()
def echo_results(*args):
    return args
