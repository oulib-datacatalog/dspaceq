#saf_builder
import jinja2
import requests
import errno
from configparser import ConfigParser
import os,json
import pandas as pd
from shutil import copyfile

states=None
template_path = os.path.dirname(os.path.realpath(__file__))
#template_path = "/Users/mstacy/github/oulib-datacatalog/cscienceq/cscienceq/tasks"

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise
    return path

def uniquify(df_columns):
    seen = set()
    for item in df_columns:
        fudge = 0
        newitem = item

        while newitem in seen:
            fudge += 1
            newitem = "{}_{}".format(item, fudge)
        yield newitem
        seen.add(newitem)

def _get_image_shareok(safDir,dspace_id,sample_id):
    url = "https://shareok.org/rest/items/{0}/bitstreams".format(dspace_id)
    print(url)
    data=requests.get(url,headers=headers).json()
    data = pd.DataFrame(data)
    files=[]
    for itm in data.itertuples():
        if itm.bundleName == 'ORIGINAL':
            files.append(itm.name)
            r= requests.get("https://shareok.org{0}".format(itm.link),stream=True)
            with open(os.path.join(safDir,itm.name), 'wb') as fd:
                for chunk in r.iter_content(chunk_size=128):
                    fd.write(chunk)
    return files
def _get_cs_images(photoDir,safDir,dspace_id,sample_id):
    filenames=[]
    if dspace_id:
        filenames = _get_image_shareok(safDir,dspace_id,sample_id)

    for filename in iglob('{0}/{1}*'.format(photoDir,sample_id)):
        copyfile(filename,os.path.join(safDir,filename.split('/')[-1]))
        filenames.append(filename.split('/')[-1])
    return list(set(filenames))

def _generate_template(data,destination,templatename):
    # load template
    #print(os.path.dirname(destination))
    templateLoader = jinja2.FileSystemLoader( searchpath=template_path)
    templateEnv = jinja2.Environment( loader=templateLoader )
    template = template = templateEnv.get_template("templates/{0}".format(templatename))
    with open(destination, 'w') as fd:
        fd.write(template.render(data))
def _get_homestate(state):
    global states
    if states is None:
        states=pd.read_csv(os.path.join(template_path,"templates","states.tmpl"))
    try:
        return states.loc[states['state'] == state].to_dict(orient='records')[0]
    except:
        raise Exception("Unable to find State data for '{0}'. Please correct data and resubmit.".format(state))
def _get_config_parameter(group,param,config_file="cybercom.cfg"):
    config = ConfigParser()
    config.read(config_file)
    return config.get(group,param)

def _post_api(url,data):
    token = _get_config_parameter('api','token')
    base_url = _get_config_parameter('api','base_url')
    headers ={"Content-Type":"application/json","Authorization":"Token {0}".format(token)}
    req = requests.post(url,data=json.dumps(data),headers=headers)
    req.raise_for_status()
    return True
def _update_zipcode(zip_code):
    data_app = "HKZ8NnhZPAvA4I18BiS9MIfZvYiwRrazc9IZxZYnztXDcYLOhEY3md3Rcmo4NyDZ"
    url ="https://www.zipcodeapi.com/rest/{0}/info.json/{1}/degrees".format(data_app,'{0:05d}'.format(zip_code))
    req = requests.get(url)
    data = req.json()
    if "error_msg" in data:
        raise Exception(data['error_msg'])
    f = lambda val: '1' if val=='T' else '0'
    update = {"city": data['city'],"zip": data['zip_code'],"dst": f(data['timezone']["is_dst"]),
            "longitude": data["lng"],"state": data["state"],"latitude": data["lat"],
            "timezone": str(int(data["timezone"]["utc_offset_sec"]/3600))
        }
    base_url = _get_config_parameter('api','base_url')
    _post_api("{0}data_store/data/citizen_science/zipcodes/".format(base_url),update)
    return update
def _get_spatial(zip_code):
    base_url = _get_config_parameter('api','base_url')
    zip_code =str('{0:05d}'.format(int(zip_code)))
    query='{"filter":{"zip":"%s"}}' % (zip_code)
    url="{0}data_store/data/citizen_science/zipcodes/.json?query={1}".format(base_url,query)
    req=requests.get(url)
    data=req.json()
    if data['count']==0:
        return _update_zipcode(zip_code)
    return data['results'][0]
def _get_handle_uri(id, df):
    for col in ['dc_identifier_uri','dc_identifier_uri_1']:
        handles=df.loc[df['id'] == id].to_dict(orient='records')[0]
        if handles[col]:
            return handles[col]
def _saf_builder(stageDir, df):
    photoDir = [name for name in os.listdir(stageDir) if "photo" in name.lower()][0]
    saf_ct=1
    headers ={'Content-Type':'application/json'}
    mapfile=[]
    for row in df.itertuples():
        if 'id' in df.columns:
            # Make saf directories for existing items
            wrkDir = mkdir_p(os.path.join(stageDir, 'saf-existing'))
            safDir= mkdir_p(os.path.join(wrkDir,str(saf_ct)))
            #get images for each saf
            filenames=_get_cs_images(photoDir,safDir,row.id,row.dwc_npdg_sampleid)
            handle= _get_handle_uri(row.id, df)
            mapfile.append("{0} {1}".format(saf_ct,"/".join(handle.split('/')[-2:])))
            _generate_template({"uri":handle},os.path.join(safDir,"dublin_core.xml"),"dublin_core.xml.tmpl")
        else:
            # Make saf directories for new items
            wrkDir = mkdir_p(os.path.join(stageDir, 'saf-new'))
            safDir = mkdir_p(os.path.join(wrkDir,str(saf_ct)))
            #get images for each saf
            filenames=_get_cs_images(photoDir,safDir,None,row.sample_id)
            _generate_template({},os.path.join(safDir,"dublin_core.xml"),"dublin_core.xml.tmpl")
        #metadata_dwc.xml template
        homestate=_get_homestate(row.state.strip().upper())
        spatial = _get_spatial(row.zip)
        data={"sampleid":row.sample_id,"internalcode":row.internal_id,
              "datecollected":row.date_collected,"isolatesRBM":row.rbm,
              "isolatesTV8":row.tv8,"detail":row.collection_detail,
              "spatial":"{0},{1}".format(spatial["latitude"],spatial["longitude"]),
              "homecity":row.city,
              "homestate":"{0} - {1}".format(homestate['long_name'],homestate['state']),
              "homezip":'{0:05d}'.format(row.zip),"imagestatus":row.photo}
        _generate_template({"dict_item":data},os.path.join(safDir,"metadata_dwc.xml"),"metadata_dwc.xml.tmpl")
        _generate_template({},os.path.join(safDir,"license.txt"),"license.tmpl")
        #print("filenames:",filenames)
        _generate_template({"photos":filenames},os.path.join(safDir,"contents"),"contents.tmpl")
        saf_ct +=1
    if mapfile:
        _generate_template({"handle_ids":mapfile},os.path.join(wrkDir,"mapfile"),"mapfile.tmpl")
    return wrkDir
