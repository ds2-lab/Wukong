#!/usr/bin/env python

import os
from setuptools import setup
import sys
import versioneer

requires = open('requirements.txt').read().strip().split('\n')
install_requires = []
extras_require = {}
for r in requires:
    if ';' in r:
        # requirements.txt conditional dependencies need to be reformatted for wheels
        # to the form: `'[extra_name]:condition' : ['requirements']`
        req, cond = r.split(';', 1)
        cond = ':' + cond
        cond_reqs = extras_require.setdefault(cond, [])
        cond_reqs.append(req)
    else:
        install_requires.append(r)

setup(name='wukong',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      description='Static Scheduler for Wukong',
      url='https://github.com/mason-leap-lab/Wukong/tree/socc2020',
      maintainer='Benjamin Carver',
      maintainer_email='bcarver2@gmu.edu',
      license='BSD',
      package_data={'': ['templates/index.html', 'template.html'],
                    'wukong': ['bokeh/templates/*.html']},
      include_package_data=True,
      install_requires=install_requires,
      extras_require=extras_require,
      packages=['wukong',
                'wukong.bokeh',
                'wukong.cli',
                'wukong.comm',
                'wukong.deploy',
                'wukong.diagnostics',
                'wukong.protocol'],
      long_description=(open('README.rst').read() if os.path.exists('README.rst')
                        else ''),
      classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Topic :: Scientific/Engineering",
        "Topic :: System :: Distributed Computing",
      ],
      entry_points='''
        [console_scripts]
        dask-ssh=wukong.cli.dask_ssh:go
        dask-submit=wukong.cli.dask_submit:go
        dask-remote=wukong.cli.dask_remote:go
        dask-scheduler=wukong.cli.dask_scheduler:go
        dask-worker=wukong.cli.dask_worker:go
        dask-mpi=wukong.cli.dask_mpi:go
      ''',
      zip_safe=False)
