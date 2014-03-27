from setuptools import setup

setup(
    name='multibackup',
    scripts=[
        'bin/backup',
        'bin/codabackup',
        'bin/prunebackups',
        'bin/rbdbackup',
        'bin/rgwbackup',
        'bin/rsyncbackup',
    ],
    description='Simple multi-source backup system using LVM snapshots',
    license='GNU General Public License, version 2',
    install_requires=[
        'boto',
        'pybloom', # >= 2.0
        'python-dateutil',
        'PyYAML',
        'xattr',
    ],
    dependency_links=[
        'git+https://github.com/jaybaird/python-bloomfilter@v2.0#egg=pybloom',
    ],
    zip_safe=True,
)
