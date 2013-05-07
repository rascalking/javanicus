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
import shutil
import stat
import urllib
import urlparse

import fuse
import requests


class WebHDFS(object):
    '''
    Abstracts away some basic file operations for a webhdfs server.
    '''
    class WebHDFSError(IOError): pass
    class WebHDFSFileNotFoundError(WebHDFSError): pass


    def __init__(self, host, port, debug=True):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.setLevel(logging.DEBUG if debug else logging.INFO)

        self._host = host
        self._port = port
        self._base_url = 'http://%s:%s/webhdfs/v1/' % (self._host, self._port)
        self._session = requests.session()


    def _raise_and_log_for_status(self, response):
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            request_line = '%s %s' % (response.request.method,
                                      response.request.url)
            if e.response.status_code == requests.codes.not_found:
                raise WebHDFS.WebHDFSFileNotFoundError(response.request.url)
            else:
                self._logger.warn('\n%s\n\n%s\n%s',
                                  request_line, e, response.text)
                import ipdb; ipdb.set_trace()
                raise WebHDFS.WebHDFSError('%s returned %s: %s'
                                           % (request_line, e,
                                              e.response.text))


    def _url(self, path):
        # strip the leading / to please urljoin
        return urlparse.urljoin(self._base_url, path.lstrip('/'))


    def close(self):
        self._session.close()
        self._session = None


    def create(self, path, permissions, user=None):
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
        params = {'op': 'CREATE',
                  'permission': oct(int(permissions))}
        if user is not None:
            params['user.name'] = user

        response = self._session.put(self._url(path), params=params)
        self._raise_and_log_for_status(response)
        return 0


    def delete(self, path, recursive=False, user=None):
        '''
        DELETE /webhdfs/v1/<path>?op=DELETE[&recursive=<true|false>]
        '''
        params = {'op': 'DELETE',
                  'recursive': str(recursive).lower()}
        if user is not None:
            params['user.name'] = user

        response = self._session.delete(self._url(path), params=params)
        self._raise_and_log_for_status(response)
        if not response.json()['boolean'] == True:
            raise IOError('Error deleting %s: %s' % (path, response.text))
        return 0


    def get(self, path, user=None):
        '''
        GET /webhdfs/v1/<PATH>?op=OPEN[&offset=<LONG>][&length=<LONG>]
                                      [&buffersize=<INT>]

        <namenode redirects to datanode, requests will auto-follow>
        '''
        params = {'op': 'OPEN'}
        if user is not None:
            params['user.name'] = user

        response = self._session.get(self._url(path), params=params)
        self._raise_and_log_for_status(response)
        return response.content


    def getattr(self, path, user=None):
        '''
        GET /webhdfs/v1/<PATH>?op=GETFILESTATUS
        '''
        params={'op': 'GETFILESTATUS'}
        if user is not None:
            params['user.name'] = user

        response = self._session.get(self._url(path), params=params)
        self._raise_and_log_for_status(response)
        return response.json()['FileStatus']


    def list(self, path, user=None):
        '''
        GET /webhdfs/v1/<PATH>?op=LISTSTATUS
        '''
        params = {'op': 'LISTSTATUS'}
        if user is not None:
            params['user.name'] = user

        response = self._session.get(self._url(path), params=params)
        self._raise_and_log_for_status(response)
        return response.json()['FileStatuses']['FileStatus']


    def put(self, path, data, mode=None, user=None):
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

        <put to datanode with file contents>
        PUT /webhdfs/v1/<PATH>?op=CREATE...

        <datanode responds with 201 created>
        HTTP/1.1 201 Created
        Location: webhdfs://<HOST>:<PORT>/<PATH>
        Content-Length: 0

        '''
        # append is the same flow as write, but POST with op=APPEND
        params = {'op': 'CREATE',
                  'overwrite': 'true'}
        if user is not None:
            params['user.name'] = user

        s1_response = self._session.put(self._url(path), params=params,
                                        allow_redirects=False)
        self._raise_and_log_for_status(s1_response)

        if 'location' not in s1_response.headers:
            raise fuse.FuseOSError(errno.EIO)
        s2_url = s1_response.headers['location']

        s2_response = self._session.put(s2_url, data=data)
        self._raise_and_log_for_status(s2_response)
        return len(data)


    def rename(self, old, new, user=None):
        '''
        PUT /webhdfs/v1/<PATH>?op=RENAME&destination=<PATH>
        '''
        params = {'op': 'RENAME',
                  'destination': new}
        if user is not None:
            params['user.name'] = user
        response = self._session.put(self._url(old), params=params)
        self._raise_and_log_for_status(response)
        if not response.json()['boolean']:
            raise fuse.FuseOSError(errno.EREMOTEIO)
        return 0


class Javanicus(fuse.Operations):
    MODE_FLAGS = {
        'DIRECTORY': stat.S_IFDIR,
        'FILE': stat.S_IFREG,
        'SYMLINK': stat.S_IFLNK,
    }
    TMPDIR = '/var/tmp/javanicus'


    def __init__(self, host, port, mountpoint='.', debug=True):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.setLevel(logging.DEBUG if debug else logging.INFO)

        self._hdfs = WebHDFS(host, port, debug)
        self._mountpoint = os.path.abspath(mountpoint).rstrip('/')

        if os.path.exists(self.TMPDIR):
            shutil.rmtree(self.TMPDIR, ignore_errors=True)
        self._tmpfiles = {}


    def __call__(self, *args, **kwargs):
        self._logger.debug('%s %s', args, kwargs)
        return super(Javanicus, self).__call__(*args, **kwargs)


    def _tmp_path(self, path):
        tmp_path = os.path.join(self.TMPDIR, path.lstrip('/'))
        tmp_path_dirname = os.path.dirname(tmp_path)
        if not os.path.isdir(tmp_path_dirname):
            os.makedirs(tmp_path_dirname)
        return tmp_path


    def _open_tmpfile(self, path):
        tmp_path = self._tmp_path(path)
        tmp_fh = open(tmp_path, 'w+b')
        self._tmpfiles[path] = {'fh': tmp_fh,
                                'path': tmp_path,
                                'dirty': False}
        self._logger.debug('Opened temp copy %s of WebHDFS file %s',
                           tmp_path, path)
        return tmp_fh


    def _gid(self, group):
        try:
            return grp.getgrnam(group).gr_gid
        except KeyError:
            self._logger.debug('No gid found for group %s, defaulting to 0',
                               group)
            return 0


    def _group(self, gid):
        try:
            return grp.getgrgid(gid).gr_name
        except KeyError:
            self._logger.debug('No group found for gid %s, defaulting to root',
                               gid)
            return 'root'


    def _uid(self, user):
        try:
            return pwd.getpwnam(user).pw_uid
        except KeyError:
            self._logger.debug('No uid found for user %s, defaulting to 0',
                               user)
            return 0


    def _user(self, uid):
        try:
            return pwd.getpwuid(uid).pw_name
        except KeyError:
            self._logger.debug('No user found for uid %s, defaulting to root',
                               uid)
            return 'root'


    @property
    def _current_user(self):
        uid = fuse.fuse_get_context()[0]
        return self._user(uid)


    def access(self, path, amode):
        # map from requested bitmask to bitmask to check against.  we don't
        # have to worrk about os.F_OK, because fuse will do a getattr to make
        # sure the path exists before calling access().
        bits_to_consider = {
            os.R_OK: {'user': stat.S_IRUSR,
                      'group': stat.S_IRGRP,
                      'other': stat.S_IROTH},
            os.W_OK: {'user': stat.S_IWUSR,
                      'group': stat.S_IWGRP,
                      'other': stat.S_IWOTH},
            os.X_OK: {'user': stat.S_IXUSR,
                      'group': stat.S_IXGRP,
                      'other': stat.S_IXOTH},
        }

        # stat the file, get the mode
        status = self.getattr(path)
        mode = status['st_mode']

        # check for our current uid/gid
        uid, gid, _ = fuse.fuse_get_context()

        # separate out the bits that were requested
        bits_requested = [b for b in os.R_OK, os.W_OK, os.X_OK if b & amode]

        # figure out which of user/group/other we might possibly match against
        ugo_matches = ['other']
        if gid == status['st_gid']:
            ugo_matches.insert(0, 'group')
        if uid == status['st_uid']:
            ugo_matches.insert(0, 'user')

        # finally, check for sufficient permissions.  raise an error as
        # soon as we see a requested mode that doesn't match.  if we make it
        # through this, then we have sufficient permissions.
        for bit in bits_requested:
            for ugo in ugo_matches:
                if bits_to_consider[bit][ugo] & mode:
                    break
            else:
                permission_name = {os.R_OK: 'read',
                                   os.W_OK: 'write',
                                   os.X_OK: 'execute'}[bit]
                self._logger.warning(
                    'No %s permission for uid %s, gid %s for \'%s\'.',
                    permission_name, uid, gid, path)
                raise fuse.FuseOSError(errno.EACCES)
        return 0


    def create(self, path, mode):
        permissions = stat.S_IMODE(mode)
        rv = self._hdfs.create(path, permissions, user=self._current_user)
        if rv == 0:
            # only open the tmpfile if the create call succeeds
            self._open_tmpfile(path)
        return rv


    def destroy(self, path):
        self._hdfs.close()
        self._hdfs = None


    def flush(self, path, fh):
        return self.fsync(path, None, fh)


    def fsync(self, path, datasync, fh):
        if self._tmpfiles[path]['dirty']:
            tmp_fh = self._tmpfiles[path]['fh']

            # save current file pointer
            current_location = tmp_fh.tell()

            # seek to beginning, read in whole file
            tmp_fh.seek(0)
            data = tmp_fh.read()

            # restore file pointer
            tmp_fh.seek(current_location)

            # write file to hdfs
            self._hdfs.put(path, data, user=self._current_user)
            self._tmpfiles[path]['dirty'] = False
        return 0


    def getattr(self, path, fh=None):
        try:
            hdfs_status = self._hdfs.getattr(path, user=self._current_user)
        except WebHDFS.WebHDFSFileNotFoundError as e:
            raise fuse.FuseOSError(errno.ENOENT)


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


    def open(self, path, flags):
        # TODO - support more than one handle on file at once
        if path in self._tmpfiles:
            raise fuse.FuseOSError(errno.EIO)

        tmp_fh = self._open_tmpfile(path)
        tmp_fh.write(self._hdfs.get(path, user=self._current_user))
        tmp_fh.seek(0)

        return 0


    def read(self, path, size, offset, fh):
        tmp_fh = self._tmpfiles[path]['fh']
        tmp_fh.seek(offset)
        return tmp_fh.read(size)


    def readdir(self, path, fh):
        statuses = self._hdfs.list(path, user=self._current_user)
        dir_contents = ['.', '..'] + [s['pathSuffix'] for s in statuses]
        return dir_contents


    def release(self, path, fh):
        # TODO - support more than one handle on file at once
        self.fsync(path, None, fh)

        # clean up the local temp copy
        tmp_fh = self._tmpfiles[path]['fh']
        tmp_fh.close()
        tmp_path = self._tmpfiles[path]['path']
        os.remove(tmp_path)
        del(self._tmpfiles[path])
        return 0


    def rename(self, old, new):
        assert old not in self._tmpfiles and new not in self._tmpfiles
        try:
            hdfs_status = self._hdfs.getattr(new, user=self._current_user)
        except WebHDFS.WebHDFSFileNotFoundError as e:
            pass
        else:
            self.unlink(new)

        return self._hdfs.rename(old, new, user=self._current_user)


    def truncate(self, path, length, fh=None):
        tmp_fh = self._tmpfiles[path]['fh']
        tmp_fh.truncate(length)
        return 0


    def unlink(self, path):
        # TODO: flush?
        return self._hdfs.delete(path, user=self._current_user)


    def write(self, path, data, offset, fh):
        # TODO - support more than one handle on file at once
        tmp_fh = self._tmpfiles[path]['fh']
        tmp_fh.seek(offset)
        tmp_fh.write(data)
        self._tmpfiles[path]['dirty'] = True
        self._logger.debug('Wrote %s bytes to temp copy of %s',
                           len(data), path)
        return len(data)


class OldJavanicus(fuse.Operations):
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
        There's no way to write specific byte ranges to a file in the webhdfs
        api.  So, we use a couple different approaches.

        If the offset < the current size, we have to get the whole file (in
        memory for now, should pick a threshold where we write it to a temp
        file), patch it locally, then write the whole goddamned thing back up.

        If the offset == the current size, we can just use append.

        If the offset > the current size, we pad it out with zeroes,
        then use append.
        '''
        current_size = self.getattr(path)['st_size']
        if offset > current_size:
            zeroes = '\0' * offset
            self._write_file(path, zeroes, append=True)
            self._write_file(path, data, append=True)
        elif offset == current_size:
            self._write_file(path, data, append=True)
        else: # offset < current_size
            # get file
            contents[offset:offset+len(data)] = data
            self._write_file(path, contents)
        return self._write_file(path, data)


    def _write_file(self, path, data, append=False):
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
        # append is the same flow as write, but POST with op=APPEND
        method = self._session.post if append else self._session.put
        op = 'APPEND' if append else 'CREATE'
        overwrite = 'false' if append else 'true'

        s1_response = method(self._url(path),
                             params={'op': op,
                                     'user.name': self._current_user,
                                     'overwrite': overwrite},
                             allow_redirects=False)
        self._raise_and_log_for_status(s1_response)
        if 'location' not in s1_response.headers:
            raise fuse.FuseOSError(errno.EIO)
        s2_url = s1_response.headers['location'].replace('webhdfs:', 'http:')
        s2_response = method(s2_url,
                             params={'op': op,
                                     'user.name': self._current_user,
                                     'overwrite': overwrite},
                             data=data)
        self._raise_and_log_for_status(s2_response)
        return len(data)


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
