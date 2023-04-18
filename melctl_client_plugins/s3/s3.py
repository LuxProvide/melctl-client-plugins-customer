# BSD 3-Clause License
# 
# Copyright (c) 2023, LuxProvide S.A.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# 
# 1. Redistributions of source code must retain the above copyright notice, this
#    list of conditions and the following disclaimer.
# 
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
# 
# 3. Neither the name of the copyright holder nor the names of its
#    contributors may be used to endorse or promote products derived from
#    this software without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS 

# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

__email__      = 'jean-philippe.clipffel@lxp.lu'
__author__     = 'Jean-Philippe Clipffel <jean-philippe.clipffel@lxp.lu>'
__license__    = 'BSD-3-Clause'
__copyright__  = 'Copyright (c) 2023 LuxProvide S.A.'
__maintainer__ = 'Jean-Philippe Clipffel'


import os
import re
import asyncio
import aiofiles
from pathlib import Path
import aiobotocore.session
from textwrap import dedent

from typing import Any

from melctl_client.commands import Command

from .config import settings


class S3:

    # Match an S3 URN
    path_regex: str = r'^(s3://)((?P<bucket>[^/]+)(/(?P<prefix>.*))?)?'

    # Match an S3 bucket URN
    bucket_regex: str = r'^(?P<scheme>s3://)(?P<bucket>.*)'

    @classmethod
    def parse_path(cls, path: str) -> tuple[str, str]:
        """Parses an S3 path.

        :param path: Path to parse as `[s3://]<bucket>[/...]`
        """
        path_rx = re.match(cls.path_regex, path)
        if path_rx is not None:
            bucket = path_rx.group('bucket')
            prefix = path_rx.group('prefix')
            return(
                bucket,
                prefix and prefix.removesuffix('/') or ''
            )
        else:
            raise Exception(f'Failed to parse path: {path}')

    def __init__(self):
        self.session = aiobotocore.session.get_session()
        self.client_kwargs = {
            'service_name': 's3',
            'endpoint_url': f'{settings.s3_scheme}{settings.s3_host}',
            'region_name': settings.s3_region,
            'aws_access_key_id': settings.s3_access_key,
            'aws_secret_access_key': settings.s3_secret_key
        }

    async def list_buckets(self) -> dict:
        """List buckets.
        """
        async with self.session.create_client(**self.client_kwargs) as client:
            return await client.list_buckets()

    async def list_objects(self, bucket: str, prefix: str = '', delimiter: str = ''):
        """List a given bucket objects.

        :param bucket: Bucket name
        :param prefix: Objects prefix / path
        :param delimiter: Objects prefix / path delimiter
        """
        async with self.session.create_client(**self.client_kwargs) as client:
            paginator = client.get_paginator('list_objects')
            async for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter=delimiter):
                yield page
    
    async def create_bucket(self, name: str, path: str) -> dict:
        """Creates a new bucket.

        :param path: Bucket path on the file system.
        """

        def set_path(request, **kwargs):
            request.headers.add_header('x-ddn-specified-directory', path)

        async with self.session.create_client(**self.client_kwargs) as client:
            client.meta.events.register('before-sign.*', set_path)
            return await client.create_bucket(Bucket=name)
    
    async def delete_bucket(self, name: str) -> dict:
        """Deletes a bucket.

        :param name: Bucket name
        """
        async with self.session.create_client(**self.client_kwargs) as client:
            return await client.delete_bucket(Bucket=name)

    async def _delete_objects(self, bucket: str, objs: list[dict[str, str]]) -> dict:
        """Deletes a list of objects.

        :param bucket: Bucket name
        :param objs: List of objects references
        """
        if len(objs) > 0:
            async with self.session.create_client(**self.client_kwargs) as client:
                return await client.delete_objects(
                    Bucket=bucket,
                    Delete={'Objects': objs, 'Quiet': False}
                )
        return {}

    async def delete_objects(self, bucket: str, path: str, recursive: bool = False) -> dict:
        """Deletes object(s) from a given bucket.

        :param bucket: Bucket name
        :param path: Objects prefix / path
        :param recursive: Delete objects matching a prefix (i.e. directories)
        """
        prefix_path = Path(path)
        to_delete = list()
        results: dict[str, Any] = {'Deleted': [], 'Errors': []}
        # Proceed
        async with self.session.create_client(**self.client_kwargs) as client:
            async for page in self.list_objects(bucket, path):
                for obj in page.get('Contents', []):
                    obj_path = Path(obj['Key'])
                    # Mark select object for removal
                    if obj_path == prefix_path:
                        to_delete.append({'Key': obj['Key']})
                    # Mark selected object in directory for removal
                    elif obj_path.is_relative_to(prefix_path) and recursive:
                        to_delete.append({'Key': obj['Key']})
                    # Fail as attempting to delete list with recursive option set
                    else:
                        raise Exception(
                            'Refusing to delete a path/prefix without recursive option set'
                        )
                    # Delete marked objects
                    if len(to_delete) > settings.s3_delete_chunk:
                        r = await self._delete_objects(bucket, to_delete)
                        results['Deleted'] += (r.get('Deleted', []))
                        results['Errors'] += (r.get('Errors', []))
            # Delete remaining marked objects
            r = await self._delete_objects(bucket, to_delete)
            results['Deleted'] += (r.get('Deleted', []))
            results['Errors'] += (r.get('Errors', []))
        # Add extra reporting fields
        results.update({
            'DeletedCount': len(results['Deleted']),
            'ErrorsCount': len(results['Errors']),
        })
        return results

    async def get_objects(self, bucket: str, path: str, dest: str):
        """Downloads object(s) from a given bucket.

        :param bucket: Bucket name
        :param path: Objects prefix / path
        :param dest: Destination path
        """

        async def download(client, obj_key: str, dest_path: str):
            wrote: int = 0
            async with aiofiles.open(dest_path, 'wb') as dest_file:
                obj = await client.get_object(Bucket=bucket, Key=obj_key)
                async with obj['Body'] as stream:
                    wrote += await dest_file.write(await stream.read())
            return {'Key': obj_key, 'Path': dest_path, 'Bytes': wrote}

        prefix_path = Path(path)
        dest_root = Path(dest)
        to_download = list()
        results = []
        # Proceed
        async with self.session.create_client(**self.client_kwargs) as client:
            async for page in self.list_objects(bucket, path):
                tasks = []
                for obj in page.get('Contents', []):
                    obj_path = Path(obj['Key'])
                    # Mark selected object for downloading
                    if obj_path == prefix_path:
                        to_download.append(obj['Key'])
                    # Mark selected object in directory for downloading
                    elif obj_path.is_relative_to(prefix_path):
                        to_download.append(obj['Key'])
                # Single file to download
                if len(to_download) == 1:
                    if dest_root.is_dir():
                        dest_path = Path(dest_root, to_download[0])
                    else:
                        dest_path = Path(dest_root)
                    tasks.append(download(client, to_download[0], str(dest_path)))
                # Multiple files to download
                elif len(to_download) > 1:
                    tasks = []
                    for obj_key in to_download:
                        dest_path = Path(dest_root, Path(obj_key))
                        Path(dest_path.parent).mkdir(parents=True, exist_ok=True)
                        tasks.append(download(client, obj_key, str(dest_path)))
                # Download
                results += await asyncio.gather(*tasks)
        # Done
        return results

    async def put_objects(self, path: str, bucket: str, bucket_prefix: str):
        """Uploads file(s) to a given bucket.

        TODO (@jpclipffel): Support for multi-part upload (>5Gb)

        :param src_path: Source path
        :param bucket: Bucket name
        :param bucket_prefix: Bucket pseudo-path
        """

        async def upload_direct(client, src_path: str, obj_key: str):
            async with aiofiles.open(src_path, 'rb') as src_file:
                response = await client.put_object(
                    Bucket=bucket,
                    Key=obj_key,
                    Body=await src_file.read()
                )
            return {'Path': src_path, 'Key': obj_key, 'Response': response}

        async def upload_multipart(client, src_path: str, obj_key: str):
            raise NotImplementedError('Multipart upload')

        src_path = Path(path)
        tasks = list()
        results = []
        # Proceed
        async with self.session.create_client(**self.client_kwargs) as client:
            # Upload a single file
            if src_path.is_file():
                obj_key = len(bucket_prefix) > 0 and bucket_prefix or src_path.name
                if src_path.stat().st_size >= settings.s3_upload_mp_from:
                    tasks.append(upload_multipart(client, str(src_path), obj_key))
                else:
                    tasks.append(upload_direct(client, str(src_path), obj_key))
            # Upload a directory content
            elif src_path.is_dir():
                for dirpath, _, filenames in os.walk(src_path):
                    for file in filenames:
                        _src_path = Path(dirpath, file)
                        obj_key = str(Path(
                            bucket_prefix,
                            dirpath.removeprefix(str(src_path.parent)).lstrip('/'),
                            file
                        ))
                        if _src_path.stat().st_size >= settings.s3_upload_mp_from:
                            tasks.append(upload_multipart(client, str(_src_path), obj_key))
                        else:
                            tasks.append(upload_direct(client, str(_src_path), obj_key))
            # Upload
            results = await asyncio.gather(*tasks)
        # Done
        return results

    async def reindex_bucket(self, bucket: str, write: bool, update: bool, delete: bool, mode: str = 'headers'):
        """Sync a bucket with its underlying file system path.

        :param bucket: Bucket name
        :param write: If `True`, add new found files to bucket
        :param update: If `True`, update changed files metadata in bucket
        :param delete: If `True`, remove deleted files from bucket
        :param mode: Operation mode, either `query`, `headers` or `payload`
        """

        def get_ops():
            return list(filter(None, [
                write and 'WRITE' or None,
                update and 'UPDATE' or None,
                delete and 'DELETE' or None
            ]))

        def set_request(request, **kwargs):
            # All modes - Patch URL
            request.url = f"{'/'.join(request.url.split('/')[0:-1])}?sync"
            # Headers mode - Add headers
            if mode == 'headers':
                ops = get_ops()
                if len(ops) > 0:
                    request.headers.add_header('x-ddn-bucket-sync-ops', ','.join(ops))

        async with self.session.create_client(**self.client_kwargs) as client:
            # All modes - Patch URL
            client.meta.events.register('before-sign.*', set_request)
            # Headers and Query modes - No payload
            if mode in ('query', 'headers'):
                response = await client.put_object(
                    Bucket=bucket,
                    Key='bucketsync_payload_empty'
                )
            # Payload mode - Add payload
            elif mode == 'payload':
                response = await client.put_object(
                    Bucket=bucket,
                    Key='bucketsync_payload',
                    Body=f'<operation><operationTypes>{",".join(get_ops())}</operationTypes></operation>'
                )
            else:
                raise Exception(f'Invalid mode: got {mode}, excepts one of query,headers,payload')
            # Get response
            print(response, type(response))
            return response


class Login(Command):
    """Login to S3 and store credentials.
    """

    def __init__(self, subparser):
        super().__init__(subparser, 'login')

    def target(self, args):
        # Get S3 information
        req = self.session.post(f'{self.url}/s3/login')
        req.raise_for_status()
        jsdata = req.json()
        # ---
        access_key_path = Path(settings.Config.secrets_dir, 's3_access_key')
        secret_key_path = Path(settings.Config.secrets_dir, 's3_secret_key')
        # Write access key
        with open(access_key_path, 'w') as fd:
            fd.write(jsdata['accesskey'])
        # Write secret key
        with open(secret_key_path, 'w') as fd:
            fd.write(jsdata['secretkey'])
        # Done
        return {
            'access_key': str(access_key_path),
            'secret_key': str(secret_key_path)
        }


class Logout(Command):
    """Remove stored S3 credentials.
    """

    def __init__(self, subparser):
        super().__init__(subparser, 'logout')

    def target(self, args):
        access_key_path = Path(settings.Config.secrets_dir, 's3_access_key')
        secret_key_path = Path(settings.Config.secrets_dir, 's3_secret_key')
        for path in (access_key_path, secret_key_path):
            os.remove(path)


class Info(Command):
    """Return information about S3 account.
    """

    def __init__(self, subparser):
        super().__init__(subparser, 'info', headers=['user', 'uuid', 'fs_paths'])
    
    def target(self, args):
        # Get S3 information
        req = self.session.get(f'{self.url}/s3/accesskeys')
        if req.status_code in (200, 404):
            return req.json()
        req.raise_for_status()


class List(Command):
    """List buckets and bucket content.
    """

    def __init__(self, subparser):
        super().__init__(subparser, 'ls')
        self.parser.add_argument('path', nargs='?', type=str, default=None,
            help='Path as [s3://]<bucket>[/...]')
        self.parser.add_argument('-r', '--recursive', action='store_true', default=False,
            help='List objects in sub-folders')
        self.s3 = S3()

    async def list_objects(self, bucket: str, prefix: str, args):
        """List a bucket content.

        :param bucket: Bucket name
        :param prefix: Objects prefix
        :param args: Parsed arguments
        """
        self.headers = ['Name', 'Size', 'LastModified']
        root = Path(prefix)
        dirs = set()
        objs = []
        # Iterate over all objects in buckets & prefix
        async for page in self.s3.list_objects(bucket, prefix):
            for obj in page.get('Contents', []):
                path = Path(obj['Key'])
                parts = path.parts[len(root.parts):]
                # Object is in prefix / directory or directly referenced via prefix
                if args.recursive or len(parts) <= 1:
                    obj.update({
                        # Formatted attributes
                        'Name': args.recursive and obj['Key'] or path.name,
                        'LastModified': obj['LastModified'].strftime(settings.s3_strftime),
                        # Extra attributes
                        'Kind': 'object',
                        'Bucket': bucket
                    })
                    objs.append(obj)
                # Object is in sub-prefix / sub-path
                elif len(parts) > 1 and parts[0] not in dirs:
                    dirs.add(parts[0])
                    objs.append({
                        # Generated attributes
                        'Key': str(parts[0]),
                        'Name': f'{parts[0]}/',
                        'LastModified': None,
                        'ETag': None,
                        'Size': 0,
                        'StorageClass': None,
                        'Owner': {
                            'DisplayName': None,
                            'ID': None
                        },
                        # Extra attributes
                        'Kind': 'directory',
                        'Bucket': bucket
                    })
                # ---
        # Done
        return sorted(objs, key=lambda o: (o['Kind'], o['Name']))

    async def list_buckets(self, args):
        """List all buckets.

        :param args: Parsed arguments
        """
        self.headers = ['Name', 'CreationDate']
        buckets = []
        for bucket in (await self.s3.list_buckets()).get('Buckets', []):
            bucket.update({
                # Formatted attributes
                'CreationDate': bucket['CreationDate'].strftime(settings.s3_strftime)
            })
            buckets.append(bucket)
        return sorted(buckets, key=lambda b: b['Name'])

    def target(self, args):
        bucket, prefix = S3.parse_path(args.path or 's3://')
        if bucket is not None:
            return asyncio.run(self.list_objects(bucket, prefix, args))
        else:
            return asyncio.run(self.list_buckets(args))


class MakeBucket(Command):
    """Create a bucket.
    """

    # Regex to match common file system paths and extract prefix.
    rx_canon_path = re.compile(r'^/mnt/tier[0-9]+/(users|project)(/lxp)?/(?P<prefix>[^/]+)')

    def __init__(self, subparser):
        super().__init__(subparser, 'mb', headers=('BucketName', 'BucketPath'))
        self.parser.add_argument('--path', type=str, required=True, help='Bucket path')
        self.parser.add_argument('--name', type=str, required=True, help='Bucket name')
        self.parser.add_argument('--allow-rootpath', action='store_true', default=False,
            help='Allow creating bucket in FS path roots')
        self.parser.add_argument('--allow-badprefix', action='store_true', default=False,
            help='Allow creating bucket without proper prefix')
        self.s3 = S3()

    def canonize_name(self, args) -> str:
        match = self.rx_canon_path.match(args.path)
        if match is not None:
            excepted_prefix = match.group('prefix')
            if not args.name.startswith(excepted_prefix) and not args.allow_badprefix:
                return f'''{excepted_prefix}-{args.name.strip('-')}'''
        return args.name

    def target(self, args):
        # Get S3 info
        req = self.session.get(f'{self.url}/s3/accesskeys')
        if req.status_code in (200, 404):
            s3_info = req.json()
        req.raise_for_status()
        # Proceed
        for fs_path, fs_path_raw in [(Path(p), p) for p in s3_info.get('fs_paths')]:
            # Check for root path
            if Path(args.path) == fs_path and not args.allow_rootpath:
                raise Exception(
                    f'''Rejecting bucket creation in root paths by default; '''
                    f'''Consider '--path {args.path.rstrip('/')}/<sub-directory>' '''
                )
            # Check for proper fs_path and proceed
            if args.path.startswith(fs_path_raw):
                bucket_name = self.canonize_name(args)
                data = asyncio.run(self.s3.create_bucket(
                    bucket_name,
                    args.path
                ))
                # Extra attributes
                data.update({
                    'BucketName': bucket_name,
                    'BucketPath': args.path
                })
                return data
        raise Exception(
            f'''Requested path '{args.path}' is not in the list of allowed '''
            f'''file system paths: {', '.join(s3_info.get('fs_paths'))}'''
        )


class DeleteBucket(Command):
    """Delete a bucket.
    """

    def __init__(self, subparser):
        super().__init__(subparser, 'rb', headers=('DeletedCount', 'ErrorsCount'))
        self.parser.add_argument('name', type=str, help='Bucket name')
        self.parser.add_argument('-r', '--recursive', action='store_true', default=False,
            help='Remove all bucket content recursively')
        self.s3 = S3()

    def target(self, args):
        results = {}
        if args.recursive:
            results.update(asyncio.run(self.s3.delete_objects(args.name, '', args.recursive)))
        results.update(asyncio.run(self.s3.delete_bucket(args.name)))
        return results


class Copy(Command):
    """Copy data to/from bucket.
    """

    def __init__(self, subparser):
        super().__init__(subparser, 'cp')
        self.parser.add_argument('src', type=str,
            help='Source path (local path or S3 URL)')
        self.parser.add_argument('dest', type=str, default='./',
            help='Destination path (local path or S3 URL)')
        self.s3 = S3()

    def download(self, args):
        bucket, prefix = self.s3.parse_path(args.src)
        self.headers = ('Key', 'Path', 'Bytes')
        return asyncio.run(self.s3.get_objects(bucket, prefix, args.dest))

    def upload(self, args):
        bucket, prefix = self.s3.parse_path(args.dest)
        self.headers = ('Path', 'Key')
        return asyncio.run(self.s3.put_objects(args.src, bucket, prefix))

    def target(self, args):
        if args.src.startswith('s3://'):
            return self.download(args)
        elif args.dest.startswith('s3://'):
            return self.upload(args)


class DeleteObjects(Command):
    """Remove object(s).
    """

    def __init__(self, subparser):
        super().__init__(subparser, 'del', headers=('DeletedCount', 'ErrorsCount'))
        self.parser.add_argument('path', type=str, help='Object path')
        self.parser.add_argument('-r', '--recursive', action='store_true', default=False,
            help='Remove all bucket content recursively')
        self.s3 = S3()

    def target(self, args):
        return asyncio.run(self.s3.delete_objects(
            *S3.parse_path(args.path),
            args.recursive
        ))


class Reindex(Command):
    """Reindex a bucket.
    """

    def __init__(self, subparser):
        super().__init__(subparser, 'reindex', headers=('bucket', 'HTTPStatusCode'))
        self.parser.add_argument('name', type=str, help='Bucket name')
        self.parser.add_argument('-m', '--mode', type=str,
            choices=('query', 'headers', 'payload'),
            default='headers')
        for op, help in (
            ('write',      'new files indexation in bucket'),
            ('update', 'changed files re-indexation in bucket'),
            ('delete', 'deleted files de-indexation from bucket')
            ):
            self.parser.add_argument(f'--{op}', dest=op, action='store_true',
                default=True, help=f'Enable {help}')
            self.parser.add_argument(f'--no-{op}', dest=op, action='store_false',
                default=True, help=f'Disable {help}')
        self.s3 = S3()

    def target(self, args):
        bucket = S3.parse_path(args.name)[0]
        data = asyncio.run(self.s3.reindex_bucket(
            bucket,
            args.write,
            args.update,
            args.delete,
            args.mode
        ))
        print(type(data))
        # Extra attributes
        data.update({
            'bucket': bucket,
            'HTTPStatusCode': data.get('ResponseMetadata', {}).get('HTTPStatusCode'),
        })
        # Done
        return data


class GenConf(Command):
    """Generates other S3 clients configuration.
    """

    generators: dict[str, str] = {
        'aws-conf': '_aws_conf',
        'aws-auth': '_aws_auth',
        's3cmd': '_s3cmd'
    }

    def __init__(self, subparser):
        super().__init__(subparser, 'genconf')
        self.parser.add_argument('kind', type=str, choices=(self.generators.keys()))

    def _aws_conf(self, args):
        return f'''\
            [default]
            region = {settings.s3_region}
        '''
    
    def _aws_auth(self, args):
        return f'''\
            [default]
            aws_access_key_id = {settings.s3_access_key}
            aws_secret_access_key = {settings.s3_secret_key}
        '''

    def _s3cmd(self, args):
        return f'''\
            [default]
            access_key = {settings.s3_access_key}
            secret_key = {settings.s3_secret_key}
            host_base = {settings.s3_host}
            check_ssl_certificate = True
            host_bucket = yes
            bucket_location = {settings.s3_region}
            guess_mime_type = True
            use_https = True
        '''

    def target(self, args):
        print(dedent(getattr(self, self.generators[args.kind])(args)))
