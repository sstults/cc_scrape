"""
Extracts all Common Crawl content from a domain
"""

import logging
import os
import sys
from argparse import ArgumentParser

import boto3
from botocore import UNSIGNED
from botocore.client import Config

from cdx_index_client import main as cdx


def boto_file(offset, length, filename, domain):

    # Boto3 anonymous login to common crawl
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

    # Count the range
    offset_end = offset + length - 1
    byte_range = 'bytes={offset}-{end}'.format(offset=offset, end=offset_end)
    gzipped_text = s3.get_object(Bucket='commoncrawl', Key=filename, Range=byte_range)['Body'].read()

    # The requested file in GZIP
    with open("cc_temp/{domain}-{offset}-{end}.gz".format(domain=domain, offset=offset, end=offset_end), 'w+b') as f:
        f.write(gzipped_text)


def main(tmpdir, coll, domain):
    in_file = os.path.join(tmpdir, coll + '0')  # TODO: Be smarter about this

    with open(in_file, 'r') as f:
        x = 0
        for line in f:
            if 'crawldiagnostics' not in line:
                x += 1
                filename, offset, length = line.split()
                logging.info('doing file: {}'.format(x))
                boto_file(int(offset), int(length), filename, domain)


if __name__ == '__main__':
    _coll = 'CC-MAIN-2019-13'
    _tmpdir = 'cc_temp'

    parser = ArgumentParser()
    parser.add_argument('domain')
    args = parser.parse_args()
    _domain = args.domain

    if not os.path.isdir('cc_temp'):
        os.mkdir('cc_temp')

    sys.argv = ['--coll={}'.format(_coll),
                '--fl=filename,offset,length',
                '--output-prefix={}/'.format(_tmpdir),
                'http://{}/*'.format(_domain)]

    cdx()
    main(_tmpdir, _coll, _domain)
