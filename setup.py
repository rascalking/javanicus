#!/usr/bin/env python

from setuptools import setup


if __name__ == '__main__':
    setup(
        name='Javanicus',
        version='0.1',

        description='A WebHDFS FUSE driver',
        url='http://github.com/rascalking/javanicus',
        author='David Bonner',
        author_email='dbonner@gmail.com',
        license='GPLv3',

        py_modules=['javanicus'],
        install_requires=['fusepy', 'requests'],
        entry_points={
            'console_scripts': ['javanicus = javanicus:main'],
        },

        classifiers=[
            'Development Status :: 4 - Beta',
            'Environment :: Console',
            'Environment :: No Input/Output (Daemon)',
            'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
            'Operating System :: MacOS :: MacOS X',
            'Operating System :: POSIX :: Linux',
            'Programming Language :: Python :: 2.6',
            'Programming Language :: Python :: 2.7',
            'Topic :: System :: Distributed Computing',
            'Topic :: System :: Filesystems',
        ],
    )
