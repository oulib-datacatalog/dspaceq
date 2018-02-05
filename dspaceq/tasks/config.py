base_url = "https://cc.lib.ou.edu"
digital_object_url = "{0}/api/catalog/data/catalog/digital_objects".format(base_url)
# search string on etd  {"filter":{"ingested":{"$ne":true}}}

alma_url = "https://api-na.hosted.exlibrisgroup.com/almaws/v1/bibs/{0}?expand=None&apikey={1}"