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


__version__    = '1.2.0'


from setuptools import setup, find_namespace_packages
from textwrap import dedent


setup(
    name='melctl_client_plugins_customer',
    author='jpclipffel',
    url='https://github.com/LuxProvide/melctl-client-plugins-customer',
    version=__version__,
    description='Customers plugins for LuxProvide API command-line client',
    long_description=dedent('''\
        This package provides MelCtl plugins for LuxProvide customers.

        - Documentation_
        - Repository_

        About MelCtl
        ============

        MelCtl provides a set of APIs to interact with LuxProvide_'s HPC (High
        Performance Computer) and Cloud services.

        The MelCtl command-line client allows our customers and internal users
        to interact with MelCtl APIs.

        The base package (`MelCtl client`_) provides basic plugins
        (e.g. authentication). Supplementary packages such as
        `melctl_client_plugins_customer` provides extra plugins to interact with
        more services from the command line.

        About LuxProvide_
        =================

        LuxProvide is Luxembourg’s one-stop-shop High Performance Computing
        Centre, with missions to provide high performance computing
        capabilities, high-speed connectivity and advanced applications on a
        national, European and international scale, serving the public and
        private sectors.


        .. _Documentation: https://luxprovide.github.io/melctl-client/
        .. _Repository: https://github.com/LuxProvide/melctl-client-plugins-customer/
        .. _LuxProvide: https://luxprovide.lu/
        .. _MelCtl client: https://pypi.org/project/melctl-client/
    '''),
    long_description_content_type='text/x-rst',
    packages=find_namespace_packages(include=['melctl_client_plugins.*']),
    install_requires=[
        'melctl_client >= 5.2.0, < 6.0.0',
        'aiofiles >= 22.1',
        'aiobotocore >= 2.4',
        # Library stubs
        'types-aiofiles >= 22.1',
    ]
)
