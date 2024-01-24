from flask import Flask, request, Response, send_file, send_from_directory
from logging.config import dictConfig
from urllib.parse import unquote
from typing import Union, Tuple, List
import os
import pwd
import datetime
import hashlib

BUCKETS_DIR = '/buckets'
ALL_PWD = pwd.getpwall()

app = Flask(__name__)
dictConfig({
    'version': 1,
    'formatters': {'default': {
        'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
    }},
    'handlers': {'wsgi': {
        'class': 'logging.FileHandler',
        'filename' : '/tmp/service.log',
        'formatter': 'default'
    }},
    'root': {
        'level': 'DEBUG',
        'handlers': ['wsgi']
    }
})


def get_user_by_uid(uid: int):
    for user in ALL_PWD:
        if user.pw_uid == uid:
            return user.pw_name
    return 'unknown'


def handle_get_bucket_versioning(bucket: str):
    _ = bucket
    return Response(response='Not implemented', status=501)


def dir_entry_to_xml_content(dir_entry: os.DirEntry):
    objects_xml = '''<Contents><Key>{key}</Key><LastModified>{mtime}</LastModified>
    <ETag>{etag}</ETag><Size>{size}</Size><StorageClass>STANDARD</StorageClass>
    <Owner><ID>{id}</ID><DisplayName>{user}</DisplayName></Owner>\
    </Contents>'''

    stat = dir_entry.stat()
    mtime = datetime.datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%dT%H:%M:%SZ")
    etag = hashlib.md5(dir_entry.name.encode('utf-8')).hexdigest()
    size = stat.st_size
    user_id = stat.st_uid
    user = get_user_by_uid(user_id)
    return objects_xml.format(key=dir_entry.name, mtime=mtime, etag=etag, 
                              size=size, id=user_id, user=user)


def list_bucket_contents(bucket: str, prefix: str = '', delimiter: str = '', max_keys: int = 1000) -> Tuple[List[str], List[str]]:
    common_prefixes_xml = '<CommonPrefixes><Prefix>{prefix}</Prefix></CommonPrefixes>'
    common_prefixes = ''
    objects = ''
    truncated = False
    total_cnt = 0

    if delimiter:
        path = '%s/%s/%s' % (BUCKETS_DIR, bucket, prefix)
        if prefix: common_prefixes += common_prefixes_xml.format(prefix='%s' % prefix)
        for f in os.scandir(path):
            if max_keys == total_cnt:
                truncated = True
                break
            if f.is_dir():
                if prefix: common_prefixes += common_prefixes_xml.format(prefix='%s/%s' % (prefix, f.name))
                else: common_prefixes += common_prefixes_xml.format(prefix=f.name)
            else:
                objects += dir_entry_to_xml_content(f)
            total_cnt += 1
    elif prefix:
        # only keys starting with prefix, no common-prefix
        pref_match = os.path.basename(prefix)
        prefix_dir = os.path.dirname(prefix)
        path = '%s/%s/%s' % (BUCKETS_DIR, bucket, prefix_dir)
        for f in os.scandir(path):
            if max_keys == total_cnt:
                truncated = True
                break
            if f.name.startswith(pref_match):
                objects += dir_entry_to_xml_content(f)
            total_cnt += 1
    else:
        # without common prefix
        path = '%s/%s' % (BUCKETS_DIR, bucket)
        for f in os.scandir(path):
            if max_keys == total_cnt:
                truncated = True
                break
            if f.name.startswith(path):
                objects += dir_entry_to_xml_content(f)
            total_cnt += 1
    return truncated, objects, common_prefixes


@app.route('/', methods=['GET'])
def handle_list_buckets() -> Response:
    """ List all buckets.
    """

    xml = '<?xml version="1.0" encoding="UTF-8"?><ListAllMyBucketsResult><Buckets>{buckets}</Buckets><Owner><DisplayName>Account_Name</DisplayName><ID>AIDACKCEVSQ6C2EXAMPLE</ID></Owner></ListAllMyBucketsResult>'
    buckets_xml = '<Bucket><CreationDate>{create_time}</CreationDate><Name>{bucket_name}</Name></Bucket>'
    buckets = ''

    if 'location' in request.args:
        return Response(response='''
         <?xml version="1.0" encoding="UTF-8"?>
         <LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-west-2</LocationConstraint>
         ''', status=200)

    for f in os.scandir(BUCKETS_DIR):
        if not f.is_dir():
            continue
        create_time = datetime.datetime.fromtimestamp(f.stat().st_ctime).strftime("%Y-%m-%dT%H:%M:%SZ")
        buckets += buckets_xml.format(create_time=create_time, bucket_name=f.name)        
    return Response(response=xml.format(buckets=buckets), status=200, mimetype='text/xml')


@app.route('/<bucket>', methods=['GET'])
@app.route('/<bucket>/', methods=['GET'])
def handle_get_bucket(bucket: str) -> Response:
    """ List bucket contents.

    Keyword arguments:
    bucket -- name of the bucket
    """

    fname = 'handle_get_bucket'
    app.logger.info('[%s]: method=[%s], uri=[%s], bucket=[%s], args=[%s]', 
                    fname, request.method, request.full_path, bucket, request.args)

    xml = '<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">\
             <Name>{bucket}</Name><Prefix>{prefix}</Prefix><MaxKeys>{max_keys}</MaxKeys><Marker>{marker}</Marker>\
             <IsTruncated>{truncated}</IsTruncated>{objects}{common_prefixes}</ListBucketResult>'

    if 'versioning' in request.args:
        return handle_get_bucket_versioning(bucket)

    max_keys = request.args.get('max-keys', 1000)
    prefix = request.args.get('prefix', '')
    delimiter = unquote(request.args.get('delimiter', ''))
    marker = request.args.get('marker', '')
    truncated = False

    try:
        truncated, objects, common_prefixes = list_bucket_contents(bucket=bucket, prefix=prefix, 
                                                                   delimiter=delimiter, max_keys=max_keys)
    except FileNotFoundError:
        objects = ''
        common_prefixes = ''

    truncated_str = 'True' if truncated else 'False'
        
    return Response(response=xml.format(bucket=bucket, prefix=prefix, 
                                        max_keys=max_keys, marker=marker,
                                        truncated=truncated_str, objects=objects,
                                        common_prefixes=common_prefixes), 
                    status=200, mimetype='text/xml')


@app.route('/<bucket>/<obj>', methods=['PUT'])
def handle_put_object(bucket: str, obj: str) -> Response:
    """ Handling new object upload request.

    Keyword arguments:
    bucket -- name of the bucket where object is stored
    obj -- name of new object
    """
    app.logger.info('method=[%s], uri=[%s], bucket=[%s], object=[%s], args=[%s]', 
                    request.method, request.full_path, bucket, obj, request.args)

    if not os.path.exists('/buckets/%s' % bucket):
        return Response(response='Bucket does not exist', status=404)

    try:
        with open('/buckets/%s/%s' % (bucket, obj), 'wb') as f:
            f.write(request.data)
        return Response(response='', status=200)
    except Exception as e:
        app.logger.error('Exception: [%s]', e)
        return Response(response='Internal Server Error', status=500)


@app.route('/<bucket>/<path:path>')
def dispatcher(bucket: str, path: str) -> Response:
    """ Generic handler of not implemented requests.

    Keyword arguments:
    bucket -- name of the bucket
    path -- remaining path string
    """
    app.logger.info('method=[%s], uri=[%s], path=[%s], args=[%s]', 
                    request.method, request.full_path, path, request.args)
    return Response(response='Not implemented', status=501)


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True, port=8080)
