'''
download all files
'''
import ast
import gzip
import json
import os
import sys

import boto3


def get_all_keys(client, bucket, prefix, keys, marker):
    response = client.list_objects(Bucket=bucket, Prefix=prefix, Marker=marker)
    if 'Contents' in response:
        keys += [content.get('Key') for content in response.get("Contents")]
        if 'IsTruncated' in response:
            return get_all_keys(client, bucket, prefix, keys, keys[-1])
    return keys


def get_log_params(filename):
    logs = []
    print(filename)
    with gzip.open(filename, 'rt') as f:
        for line in f:
            log = ast.literal_eval(line)
            logs.append(log)
    return logs


def main():
    s3_client = boto3.client('s3')
    bucket_name = 'bucket_name'
    prefix = "key_name"
    keys = get_all_keys(s3_client, bucket_name, prefix, [], "")

    for key in keys[:1]:
        filename = key.split('/')[-1]
        s3_client.download_file(bucket_name, key, filename)
        params = get_log_params(filename)

        print(len(params))
        print(params)
        
        os.remove(filename)

    return 0

if __name__ == "__main__":
    sys.exit(main())
