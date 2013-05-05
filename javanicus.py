#!/usr/bin/env python

'''
javanicus from
http://www.iucnredlist.org/details/176718/0

base ls command
$ curl -i -X GET "http://localhost:50070/webhdfs/v1/?op=LISTSTATUS"

webhdfs docs
http://hadoop.apache.org/docs/r1.0.4/webhdfs.html

fusepy docs
https://github.com/terencehonles/fusepy
'''

'''
Copyright 2013, David Bonner <dbonner@gmail.com>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
'''


import errno
import grp
import logging
import os
import pwd
import stat
import urlparse

import fuse
import requests


class Javanicus(fuse.Operations):
    MODE_FLAGS = {
        'DIRECTORY': stat.S_IFDIR,
        'FILE': stat.S_IFREG,
        'SYMLINK': stat.S_IFLNK,
    }


    def __init__(self, host, port, mountpoint='.', debug=True):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.setLevel(logging.DEBUG if debug else logging.INFO)

        self._host = host
        self._port = port
        self._mountpoint = os.path.abspath(mountpoint).rstrip('/')

        self._base_url = 'http://%s:%s/webhdfs/v1/' % (self._host, self._port)
        self._session = requests.session()


    def __call__(self, *args, **kwargs):
        self._logger.debug('%s %s', args, kwargs)
        return super(Javanicus, self).__call__(*args, **kwargs)


    def _url(self, path):
        # strip the leading / to please urljoin
        return urlparse.urljoin(self._base_url, path.lstrip('/'))


    def _gid(self, group):
        try:
            return grp.getgrnam(group).gr_gid
        except KeyError:
            return 0


    def _group(self, gid):
        try:
            return grp.getgrgid(gid).gr_name
        except KeyError:
            return 'root'


    def _uid(self, username):
        try:
            return pwd.getpwnam(username).pw_uid
        except KeyError:
            return 0


    def _user(self, uid):
        try:
            return pwd.getpwuid(uid).pw_name
        except KeyError:
            return 'root'


    @property
    def _current_user(self):
        uid = fuse.fuse_get_context()[0]
        return self._user(uid)


    def _raise_and_log_for_status(self, response):
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            self._logger.warn('\n%s %s\n\n%s\n%s',
                              response.request.method,
                              response.request.url,
                              e,
                              response.text) 
            raise


    def chmod(self, path, mode):
        '''
        PUT /webhdfs/v1/<PATH>?op=SETPERMISSION[&permission=<OCTAL>]
        '''
        response = self._session.put(self._url(path),
                                    params={'op': 'SETPERMISSION',
                                            'permission': oct(mode),
                                            'user.name': self._current_user})
        self._raise_and_log_for_status(response)
        return 0


    def chown(self, path, uid, gid):
        '''
        PUT /webhdfs/v1/<PATH>?op=SETOWNER[&user=<USER>][&group=<GROUP>]
        '''
        user = self._user(uid)
        group = self._group(gid)
        response = self._session.put(self._url(path),
                                     params={'op': 'SETOWNER',
                                             'user.name': self._current_user,
                                             'user': user,
                                             'group': group})
        self._raise_and_log_for_status(response)
        return 0


    def create(self, path, mode):
        '''
        <put to namenode, don't auto-follow the redirect>

        PUT /webhdfs/v1/<PATH>?op=CREATE[&overwrite=<true|false>]
                                        [&blocksize=<LONG>]
                                        [&replication=<SHORT>]
                                        [&permission=<OCTAL>]
                                        [&buffersize=<INT>]

        <namenode responds with a redirect to a datanode, which we ignore>
        HTTP/1.1 307 TEMPORARY_REDIRECT
        Location: http://<DATANODE>:<PORT>/webhdfs/v1/<PATH>?op=CREATE...
        Content-Length: 0
        '''
        response = self._session.put(self._url(path),
                                     params={'op': 'CREATE',
                                             'user.name': self._current_user,
                                             'mode': oct(mode)})
        self._raise_and_log_for_status(response)
        return 0


    def destroy(self, path):
        self._session.close()


    def getattr(self, path, fh=None):
        '''
        GET /webhdfs/v1/<PATH>?op=GETFILESTATUS
        '''
        response = self._session.get(self._url(path),
                                    params={'op': 'GETFILESTATUS'})
	try:
            self._raise_and_log_for_status(response)
        except requests.HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                raise fuse.FuseOSError(errno.ENOENT)
            else:
                print e.response.request.method, e.response.request.url
                import ipdb; ipdb.set_trace()
                raise
        except Exception as e:
            import ipdb; ipdb.set_trace()
            raise
        hdfs_status = response.json()['FileStatus']
        # NB - timestamps in hdfs_status are milliseconds since epoch,
        #      and timestamps in status need to be seconds since epoch
        status = {
            'st_atime': hdfs_status['accessTime'] / float(1000),
            'st_gid': self._gid(hdfs_status['group']),
            'st_mode': (int(hdfs_status['permission'], 8)
                        | self.MODE_FLAGS[hdfs_status['type']]),
            'st_mtime': hdfs_status['modificationTime'] / float(1000),
            'st_size': hdfs_status['length'],
            'st_uid': self._uid(hdfs_status['owner']),
        }

        return status


    def mkdir(self, path, mode):
        '''
        PUT /webhdfs/v1/<PATH>?op=MKDIRS[&permission=<OCTAL>]
        '''
        response = self._session.put(self._url(path),
                                    params={'op': 'MKDIRS',
                                            'permission': permission})
        self._raise_and_log_for_status(response)
        if not response.json()['boolean']:
            raise fuse.FuseOSError(errno.EREMOTEIO)
        return 0


    def read(self, path, size, offset, fh):
        '''
        GET /webhdfs/v1/<PATH>?op=OPEN[&offset=<LONG>][&length=<LONG>]
                                      [&buffersize=<INT>]

        <namenode redirects to datanode, requests will auto-follow>
        '''
        response = self._session.get(self._url(path), params={'op': 'OPEN',
                                                              'offset': offset,
                                                              'length': size})
        self._raise_and_log_for_status(response)
        # TODO - sanity check the response size vs. requested size?
        return response.content


    def readdir(self, path, fh):
        '''
        GET /webhdfs/v1/<PATH>?op=LISTSTATUS
        '''
        response = self._session.get(self._url(path),
                                    params={'op': 'LISTSTATUS'})
        self._raise_and_log_for_status(response)
        dir_contents = ['.', '..']
        for status in response.json()['FileStatuses']['FileStatus']:
            dir_contents.append(status['pathSuffix'])
        return dir_contents


    def rename(self, old, new):
        '''
        PUT /webhdfs/v1/<PATH>?op=RENAME&destination=<PATH>
        '''
        response = self._session.put(self._url(old), params={'op': 'RENAME',
                                                            'destination': new})
        self._raise_and_log_for_status(response)
        if not response.json()['boolean']:
            raise fuse.FuseOSError(errno.EREMOTEIO)
        return 0



    def rmdir(self, path):
        '''
        DELETE /webhdfs/v1/<path>?op=DELETE[&recursive=<true|false>]
        '''
        response = self._session.delete(self._url(path), params={'op': 'DELETE'})
        self._raise_and_log_for_status(response)
        if not response.json()['boolean']:
            raise fuse.FuseOSError(errno.EREMOTEIO)
        return 0


    def truncate(self, path, length, fh=None):
        if length == 0:
            self._logger.debug('truncate -> getattr')
            mode = stat.S_IMODE(self.getattr(path, fh)['st_mode'])
            self._logger.debug('truncate -> create')
            response = self._session.put(
                           self._url(path),
                           params={'op': 'CREATE',
                                   'user.name': self._current_user,
                                   'mode': oct(mode),
                                   'overwrite': 'true'})
            self._raise_and_log_for_status(response)
            return self.create(path, mode)
        else:
            raise FuseOSError(errno.EROFS)


    def unlink(self, path):
        '''
        DELETE /webhdfs/v1/<path>?op=DELETE[&recursive=<true|false>]
        '''
        response = self._session.delete(self._url(path),
                                        params={'op': 'DELETE',
                                                'user.name': self._current_user})
        self._raise_and_log_for_status(response)
        if not response.json()['boolean']:
            raise fuse.FuseOSError(errno.EREMOTEIO)
        return 0


    def write(self, path, data, offset, fh):
        '''
        <put to namenode, don't auto-follow the redirect>

        PUT /webhdfs/v1/<PATH>?op=CREATE[&overwrite=<true|false>]
                                        [&blocksize=<LONG>]
                                        [&replication=<SHORT>]
                                        [&permission=<OCTAL>]
                                        [&buffersize=<INT>]

        <namenode responds with a redirect to a datanode>
        HTTP/1.1 307 TEMPORARY_REDIRECT
        Location: http://<DATANODE>:<PORT>/webhdfs/v1/<PATH>?op=CREATE...
        Content-Length: 0

        <put to datanode with empty file contents...do we need to do this?>
        PUT /webhdfs/v1/<PATH>?op=CREATE...

        <datanode responds with 201 created>
        HTTP/1.1 201 Created
        Location: webhdfs://<HOST>:<PORT>/<PATH>
        Content-Length: 0
        '''
        '''
        There's no way to write specific byte ranges to a file in the webhdfs
        api.  So, we use a couple different approaches.

        If the offset < the current size, we have to get the whole file (in
        memory for now, should pick a threshold where we write it to a temp
        file), patch it locally, then write the whole goddamned thing back up.

        If the offset == the current size, we can just use append.

        If the offset > the current size, we pad it out with zeroes,
        then use append.
        '''
        s1_response = self._session.put(self._url(path),
                                        params={'op': 'CREATE',
                                                'user.name': self._current_user})
        self._raise_and_log_for_status(s1_response)
        s2_url = s1_response.headers['location'].replace('webhdfs:', 'http:')
        s2_response = self._session.put(s2_url,
                                        params={'op': 'CREATE',
                                                'user.name': self._current_user})
        self._raise_and_log_for_status(s2_response)
        return 0


if __name__ == '__main__':
    import logging
    logging.basicConfig()

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('host')
    parser.add_argument('port')
    parser.add_argument('mountpoint')
    args = parser.parse_args()

    fs = fuse.FUSE(Javanicus(args.host, args.port, args.mountpoint),
                   args.mountpoint,
                   foreground=True, nothreads=True)
