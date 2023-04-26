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


import re
import sys
import json
from pathlib import Path

from melctl_client.commands import Command, SimpleCommand
from melctl_client.config import settings


cfgfile = Path(settings.Config.secrets_dir, 's8s_config.json')




class _S8SCommand:
    """Base S8S command.
    """

    all_keys: list[str]      = ['region', 'pool', 'master', 'token']
    required_keys: list[str] = ['region', 'pool', 'master', 'token']

    def __init__(self, *args, **kwargs):
        self.parser.add_argument('config', type=str, default=None,
            nargs='?', help='Configuration name')
        # ---
        if 'region' in self.required_keys:
            self.parser.add_argument('--region', type=str, default=None,
                required=False, help='Region name')
        if 'pool' in self.required_keys:
            self.parser.add_argument('--pool', type=str, default=None,
                required=False, help='Pool name')
        if 'master' in self.required_keys:
            self.parser.add_argument('--master', type=str, default=None,
                required=False, help='Master URL')
        if 'token' in self.required_keys:
            self.parser.add_argument('--token', type=str, default=None,
                required=False, help='Join token')

    def update_args(self, args):
        """Update an `argparse` namespace with a configuration.

        :param config_name: Configuration name, set to `None` to bypass
        :param args: Argparse namespace
        """
        # Load configuration and patch arguments
        if getattr(args, 'config') is not None and len(getattr(args, 'config', '')) > 0:
            if cfgfile.exists():
                # Load configuration
                try:
                    with open(cfgfile, 'r') as fd:
                        config = json.load(fd)[args.config]
                except KeyError:
                    raise Exception(f'S8S configuration "{args.config}" is not defined')
                # Patch arguments
                try:
                    for key in self.all_keys:
                        if getattr(args, key, None) in (None, ''):
                            setattr(args, key, config[key])
                except KeyError:
                    raise Exception(f'Missing parameter "{key}" in S8S configuration "{args.config}"')
            else:
                raise Exception(f'S8S configurations file does not exists ({str(cfgfile)})')
        # Check arguments
        missing = []
        for key in self.required_keys:
            if getattr(args, key, None) in (None, ''):
                missing.append(key)
        if len(missing) > 0:
            raise Exception(f'Missing arguments or configuration keys: {", ".join(missing)}')
        # Done
        return args


class S8SCommand(Command, _S8SCommand):
    """MelCtl `Command` wrapper.
    """

    def __init__(self, *args, **kwargs):
        Command.__init__(self, *args, **kwargs)
        _S8SCommand.__init__(self, *args, **kwargs)


class S8SSimpleCommand(SimpleCommand, _S8SCommand):
    """MelCtl `SimpleCommand` wrapper.
    """

    def __init__(self, *args, **kwargs):
        SimpleCommand.__init__(self, *args, **kwargs)
        _S8SCommand.__init__(self, *args, **kwargs)

    def target(self, args):
        args = self.update_args(args)
        return super().target(args)


class SetConfig(Command):
    """Add an S8S configuration.
    """

    def __init__(self, subparser):
        super().__init__(subparser, 'set-config')
        self.parser.add_argument('name', type=str,
            help='Configuration name')
        self.parser.add_argument('--region', type=str, required=True,
            help='Region name')
        self.parser.add_argument('--pool', type=str, required=True,
            help='Pool name')
        self.parser.add_argument('--master', type=str, required=True,
            help='Master node URL')
        self.parser.add_argument('--token', type=str, required=True,
            help='Cluster join token')
    
    def target(self, args):
        # Load S8S configuration
        if cfgfile.exists():
            with open(cfgfile, 'r') as fd:
                config = json.load(fd)
        else:
            config = {}
        # Setup configuration
        config[args.name] = {
            'region': args.region,
            'pool': args.pool,
            'master': args.master,
            'token': args.token
        }
        # Write configuration
        with open(cfgfile, 'w') as fd:
            json.dump(config, fd, indent=2)
        # Done
        return {
            'file': str(cfgfile),
            'name': args.name
        }


class GetConfig(Command):
    """Returns all or one of S8S configuration.
    """

    def __init__(self, subparser):
        super().__init__(subparser, 'get-config')
        self.parser.add_argument('name', type=str, nargs='?', default=None,
            help='Configuration name')
        self.parser.add_argument('--token', action='store_true', default=False,
            help='Display token'
        )

    def target(self, args):
        if not cfgfile.exists():
            return []
        try:
            with open(cfgfile, 'r') as fd:
                config = json.load(fd)
            # Select config
            if args.name is not None:
                config = {args.name: config[args.name]}
            # Cleanup token is not requested
            if args.token is False:
                for k, v in config.items():
                    del v['token']
            # Done
            return [
                {**{'name': k}, **v} for k, v in config.items()
            ]
        except KeyError:
            print(f'Configuration {args.name} is not defined', file=sys.stderr)
            sys.exit(1)
        except Exception as error:
            print(f'Failed to load configuration file {str(cfgfile)}: {str(error)}')
            sys.exit(1)


class DelConfig(Command):
    """Remove an S8S configuration.
    """

    def __init__(self, subparser):
        super().__init__(subparser, 'del-config')
        self.parser.add_argument('name', type=str,default=None,
            help='Configuration name')

    def target(self, args):
        if not cfgfile.exists():
            return {}
        with open(cfgfile, 'r') as fd:
            config = json.load(fd)
            if args.name in config:
                del config[args.name]
        with open(cfgfile, 'w') as fd:
            json.dump(config, fd, indent=2)
        return {
            'deleted': args.name
        }


class RegionsList(SimpleCommand):
    """Lists S8S regions.
    """

    def __init__(self, subparser):
        super().__init__(subparser, 'list-regions', 'GET', 's8s/regions')


class PoolsList(S8SSimpleCommand):
    """Lists all S8S pools.
    """

    required_keys: list[str] = ['region', ]

    def __init__(self, subparser):
        super().__init__(subparser, 'list-pools', 'GET', 's8s/regions/{region}/pools',
            headers=['name', 'nodes_count', 'nodes_list'])
        # self.parser.add_argument('region', type=str)

    def render(self, args, data):
        for pool in data:
            pool['nodes_count'] = len(pool['nodes'])
            pool['nodes_list'] = [n['name'] for n in pool['nodes']]
        return data


class PoolsGet(S8SSimpleCommand):
    """Show information about an S8S pool.
    """

    required_keys: list[str] = ['region', 'pool']

    def __init__(self, subparser):
        super().__init__(subparser, 'get-pool', 'GET', 's8s/regions/{region}/pools/{pool}',
            headers=['name', 'nodes_count', 'nodes_list'])

    def render(self, args, data):
        for pool in data:
            pool['nodes_count'] = len(pool['nodes'])
            pool['nodes_list'] = [n['name'] for n in pool['nodes']]
        return data


class Status(S8SSimpleCommand):
    """Shows a cluster pool status.
    """

    required_keys: list[str] = ['region', 'pool']

    def __init__(self, subparser):
        super().__init__(subparser, 'status', 'GET', 's8s/regions/{region}/pools/{pool}',
            headers=['name', 'nodes_count', 'nodes_list'])

    def render(self, args, data):
        for pool in data:
            pool['nodes_count'] = len(pool['nodes'])
            pool['nodes_list'] = [n['name'] for n in pool['nodes']]
        return data


class Resources(S8SCommand):
    """Shows available resources.
    """

    required_keys: list[str] = ['region', ]

    def __init__(self, subparser):
        super().__init__(subparser, 'resources',
            headers=['region', 'nodes_by_features'])
        self.parser.add_argument('--nodeslist', action='store_true',
            default=False, help='Include nodes list')

    def target(self, args):
        # Patch args from config
        args = self.update_args(args)
        # Patch headers
        if args.nodeslist:
            self.headers.append('nodes_list')
        # Proceed
        req = self.session.get(
            f'{self.url}/s8s/regions/{args.region}/resources',
            params={
                'nodeslist': args.nodeslist
            }
        )
        self.raise_for_status(req)
        return req.json()


class Scale(Command):
    """Scales an S8S clusters.
    """

    partitions = ('cpu', 'gpu', 'mem', 'fpga')
    regex_scalespecs = re.compile(r'^(?P<nodes>[0-9]+):(?P<time>[0-9]+)$')

    @classmethod
    def type_scalespecs(cls, arg):
        try:
            return cls.regex_scalespecs.match(arg).groupdict()
        except Exception:
            raise TypeError()

    @classmethod
    def end_with_usage(cls, msgs: list[str]):
        for msg in msgs:
            print(msg.format(
                melctl=f'{Path(sys.argv[0]).parts[-1]}',
                cmd=f'{Path(sys.argv[0]).parts[-1]} {sys.argv[1]} {sys.argv[2]}'
            ),
            file=sys.stderr)
        sys.exit(1)

    def __init__(self, subparser):
        super().__init__(subparser, 'scale')
        self.parser.add_argument('name', type=str, nargs='?', default=None,
            help='Configuration name')
        self.parser.add_argument('--region', type=str, default=None,
            help='Region name')
        self.parser.add_argument('--pool', type=str, default=None,
            help='Pool name')
        self.parser.add_argument('--master', type=str, default=None,
            help='Master node URL')
        self.parser.add_argument('--token', type=str, default=None,
            help='Cluster join token')
        self.parser.add_argument('--dry-run', action='store_true', default=False,
            help='Dry run mode')
        for p in self.partitions:
            self.parser.add_argument(f'--{p}', dest=f'specs_{p}',
                type=Scale.type_scalespecs, default=[], action='append',
                help=f'{p.upper()} scale specification as "<nodes>:<seconds>"')

    def reprocess_args(self, args):
        """Controls and update the arguments.
        """
        # Manual arguments
        manual_args = list(filter(
            None,
            (args.region, args.pool, args.master, args.token))
        )
        # Check and load configuration from file or arguments
        if args.name is not None:
            # Config or argument, not both
            if len(manual_args) > 0:
                self.end_with_usage((
                    'usage: {cmd} {{name|--region,--pool,--master,--token}}',
                    '{cmd}: error: either {{name}} or {{--region,--pool,--master,--token}} are required'
                ))
            # Load configuration
            try:
                with open(cfgfile, 'r') as fd:
                    cfg = json.load(fd)[args.name]
                    args.region = cfg['region']
                    args.pool = cfg['pool']
                    args.master = cfg['master']
                    args.token = cfg['token']
            except Exception as error:
                self.end_with_usage((
                    '{cmd}: error: configuration ' + args.name + ' not found or invalid',
                    f'You may create the configuration with {{melctl}} {sys.argv[1]} set-config'
                ))
        elif len(manual_args) < 4:
            self.end_with_usage((
                'usage: {cmd} {{name|--region,--pool,--master,--token}}',
                '{cmd}: error: either {{name}} or {{--region,--pool,--master,--token}} are required'
            ))
        # Done
        return args

    def target(self, args):
        args = self.reprocess_args(args)
        scale_specs = []
        scale_stats = []
        # Process nodes specifications
        for part in self.partitions:
            for part_specs in getattr(args, f'specs_{part}'):
                for _ in range(int(part_specs['nodes'])):
                    scale_specs.append({
                        'region': args.region,
                        'pool': args.pool,
                        'features': {
                            part: True
                        },
                        'seconds': int(part_specs['time']),
                        'master': args.master,
                        'token': args.token
                    })
        # Ensure nodes are requested
        if len(scale_specs) < 1:
            _parts = ', '.join([
                f'[--{p} <nodes>:<seconds>]'
                for p in self.partitions
            ])
            print(
                f'Scaling specifications required: {_parts}',
                file=sys.stderr
            )
            sys.exit(1)
        # Request nodes
        for node_specs in scale_specs:
            req = self.session.post(
                f'{self.url}/s8s/regions/{args.region}/pools/{args.pool}',
                json=node_specs,
                params={
                    'dry_run': args.dry_run
                }
            )
            self.raise_for_status(req)
            scale_stats.append(req.json())
        # Done
        return scale_stats
