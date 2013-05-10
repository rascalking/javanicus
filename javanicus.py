#!/usr/bin/env python

'''
Javanicus - a WebHDFS FUSE driver
https://github.com/rascalking/javanicus


The name Javanicus comes from googling for "elephant snake", eg.
http://www.iucnredlist.org/details/176718/0

A simple ls command will be tranlsated to:
$ curl -i -X GET "http://localhost:50070/webhdfs/v1/?op=LISTSTATUS"

WebHDFS docs:
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
import tempfile
import time
import urllib
import urlparse

import fuse
import requests


class WebHDFS(object):
    '''
    Abstracts away some basic file operations for a webhdfs server.
    '''
    class WebHDFSError(IOError): pass
    class WebHDFSDirectoryNotEmptyError(WebHDFSError): pass
    class WebHDFSFileNotFoundError(WebHDFSError): pass
    class WebHDFSPermissionError(WebHDFSError): pass


    def __init__(self, host, port, debug=False):
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
            if e.response.status_code == requests.codes.not_found:
                raise WebHDFS.WebHDFSFileNotFoundError(response.request.url)
            elif e.response.status_code == requests.codes.forbidden:
                exception = e.response.json()['RemoteException']
                if exception['message'].endswith(' is non empty'):
                    raise WebHDFS.WebHDFSDirectoryNotEmptyError(
                              response.request.url)
                elif exception['exception'] == 'AccessControlException':
                    raise WebHDFS.WebHDFSPermissionError(response.request.url)

            # if we get this far, there's no special handling for this.
            # log it and reraise it.
            request_line = '%s %s' % (response.request.method,
                                      response.request.url)
            self._logger.warn('\n%s\n\n%s\n%s',
                              request_line, e, response.text)
            raise WebHDFS.WebHDFSError('%s returned %s: %s'
                                       % (request_line, e, e.response.text))


    def _url(self, path):
        # strip the leading / to please urljoin
        return urlparse.urljoin(self._base_url, path.lstrip('/'))


    def checksum(self, path, user=None):
        '''
        GET /webhdfs/v1/<PATH>?op=GETFILECHECKSUM
        '''
        params = {'op': 'GETFILECHECKSUM'}
        if user is not None:
            params['user.name'] = user

        response = self._session.get(self._url(path), params=params)
        self._raise_and_log_for_status(response)
        return response.json()['FileChecksum']


    def chmod(self, path, permissions, user=None):
        '''
        PUT /webhdfs/v1/<PATH>?op=SETPERMISSION[&permission=<OCTAL>]
        '''
        params = {'op': 'SETPERMISSION',
                  'permission': oct(int(permissions))}
        if user is not None:
            params['user.name'] = user

        response = self._session.put(self._url(path), params=params)
        self._raise_and_log_for_status(response)
        return 0


    def chown(self, path, to_user, to_group, user=None):
        '''
        PUT /webhdfs/v1/<PATH>?op=SETOWNER[&user=<USER>][&group=<GROUP>]
        '''
        params = {'op': 'SETOWNER',
                  'user': to_user,
                  'group': to_group}
        if user is not None:
            params['user.name'] = user

        response = self._session.put(self._url(path), params=params)
        self._raise_and_log_for_status(response)
        return 0


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


    def mkdir(self, path, permissions=None, user=None):
        '''
        PUT /webhdfs/v1/<PATH>?op=MKDIRS[&permission=<OCTAL>]
        '''
        params = {'op': 'MKDIRS'}
        if user is not None:
            params['user.name'] = user
        if permissions is not None:
            params['permission'] = oct(int(permissions))

        response = self._session.put(self._url(path), params=params)
        self._raise_and_log_for_status(response)
        if not response.json()['boolean']:
            raise fuse.FuseOSError(errno.EREMOTEIO)
        return 0


    def put(self, path, data, permissions=None, user=None):
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
        if permissions is not None:
            params['permission'] = oct(int(permissions))

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


    def utime(self, path, atime, mtime, user=None):
        params = {'op': 'SETTIMES',
                  'accesstime': int(atime) * 1000,
                  'modificationtime': int(mtime) * 1000}
        if user is not None:
            params['user.name'] = user
        response = self._session.put(self._url(path), params=params)
        self._raise_and_log_for_status(response)
        return 0


class Javanicus(fuse.Operations):
    MODE_FLAGS = {
        'DIRECTORY': stat.S_IFDIR,
        'FILE': stat.S_IFREG,
        'SYMLINK': stat.S_IFLNK,
    }


    def __init__(self, host, port, mountpoint='.', debug=True):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.setLevel(logging.DEBUG if debug else logging.INFO)

        self._hdfs = WebHDFS(host, port, debug)
        self._mountpoint = os.path.abspath(mountpoint).rstrip('/')

        self._tmpdir = tempfile.mkdtemp(prefix='javanicus')
        self._tmpfiles = {}


    def __call__(self, *args, **kwargs):
        self._logger.debug('%s %s', args, kwargs)
        return super(Javanicus, self).__call__(*args, **kwargs)


    ######
    ######
    ## Tempfile methods
    ##
    ## Methods related to creating, updating, and deleting local copies of files
    ##
    ## TODO: refactor these out into a separate class
    def _tmp_path(self, path):
        tmp_path = os.path.join(self._tmpdir, path.lstrip('/'))
        tmp_path_dirname = os.path.dirname(tmp_path)
        if not os.path.isdir(tmp_path_dirname):
            os.makedirs(tmp_path_dirname)
        return tmp_path


    def _open_tmpfile(self, path):
        tmp_path = self._tmp_path(path)
        tmp_fh = open(tmp_path, 'w+b')
        self._tmpfiles[path] = {'fh': tmp_fh,
                                'path': tmp_path,
                                'cksum': '',
                                'dirty': False}
        self._logger.debug('Opened temp copy %s of WebHDFS file %s',
                           tmp_path, path)
        return tmp_fh


    def _push_tmpfile_if_dirty(self, path):
        if self._tmpfiles[path]['dirty']:
            tmp_fh = self._tmpfiles[path]['fh']
            tmp_fh.seek(0)
            full_data = tmp_fh.read()
            self._hdfs.put(path, full_data, user=self._current_user)
            self._logger.debug(
                'Wrote full file (%s bytes) to WebHDFS copy of %s',
                len(full_data), path)
            self._tmpfiles[path]['dirty'] = False
            self._set_tmpfile_cksum(path)


    def _refresh_tmpfile(self, path):
        # verifies our local copy vs. webhdfs, fetches new local copy as needed
        cksum = self._hdfs.checksum(path, user=self._current_user)['bytes']
        if cksum != self._tmpfiles[path]['cksum']:
            # the server always wins
            self._tmpfiles[path]['dirty'] = False

            # get the new one
            self._logger.debug(
                'Tossing temp copy of %s because checksum changed'
                ' from \'%s\' to \'%s\'',
                path, self._tmpfiles[path]['cksum'], cksum)
            tmp_fh = self._tmpfiles[path]['fh']
            tmp_fh.seek(0)
            tmp_fh.write(self._hdfs.get(path, user=self._current_user))

            # be paranoid, assume the cksum may have changed.  this doesn't
            # completely protect us, but in the absence of something better...
            self._set_tmpfile_cksum(path)


    def _remove_tmpfile(self, path):
        tmp_fh = self._tmpfiles[path]['fh']
        tmp_fh.close()
        tmp_path = self._tmpfiles[path]['path']
        os.remove(tmp_path)
        del(self._tmpfiles[path])


    def _set_tmpfile_cksum(self, path):
        cksum = self._hdfs.checksum(path, user=self._current_user)['bytes']
        self._tmpfiles[path]['cksum'] = cksum


    ######
    ######
    ## UID<->user, GID<->group lookup methods.
    ##
    ## TODO: memoize

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


    def chmod(self, path, mode):
        permissions = stat.S_IMODE(mode)
        return self._hdfs.chmod(path, permissions, user=self._current_user)


    def chown(self, path, uid, gid):
        try:
            return self._hdfs.chown(path,
                                    to_user=self._user(uid),
                                    to_group=self._user(gid),
                                    user=self._current_user)
        except WebHDFS.WebHDFSPermissionError as e:
            raise fuse.FuseOSError(errno.EPERM)


    def create(self, path, mode):
        assert path not in self._tmpfiles
        permissions = stat.S_IMODE(mode)
        rv = self._hdfs.create(path, permissions, user=self._current_user)
        if rv == 0:
            # only open the tmpfile if the create call succeeds
            self._open_tmpfile(path)
            self._set_tmpfile_cksum(path)
        return rv


    def destroy(self, path):
        self._hdfs.close()
        self._hdfs = None
        shutil.rmtree(self._tmpdir)


    def flush(self, path, fh):
        return self.fsync(path, None, fh)


    def fsync(self, path, datasync, fh):
        self._refresh_tmpfile(path)
        self._push_tmpfile_if_dirty(path)
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


    def mkdir(self, path, mode):
        permissions = stat.S_IMODE(mode)
        return self._hdfs.mkdir(path, permissions=permissions,
                                user=self._current_user)


    def open(self, path, flags):
        if path in self._tmpfiles:
            raise fuse.FuseOSError(errno.EIO)

        tmp_fh = self._open_tmpfile(path)
        self._refresh_tmpfile(path)
        return 0


    def read(self, path, size, offset, fh):
        self._refresh_tmpfile(path)
        tmp_fh = self._tmpfiles[path]['fh']
        tmp_fh.seek(offset)
        return tmp_fh.read(size)


    def readdir(self, path, fh):
        statuses = self._hdfs.list(path, user=self._current_user)
        dir_contents = ['.', '..'] + [s['pathSuffix'] for s in statuses]
        return dir_contents


    def release(self, path, fh):
        self._push_tmpfile_if_dirty(path)
        self._remove_tmpfile(path)
        return 0


    def rename(self, old, new):
        assert old not in self._tmpfiles and new not in self._tmpfiles
        try:
            hdfs_status = self._hdfs.getattr(new, user=self._current_user)
        except WebHDFS.WebHDFSFileNotFoundError as e:
            pass
        else:
            try:
                self.unlink(new)
            except WebHDFS.WebHDFSDirectoryNotEmptyError as e:
                raise fuse.FuseOSError(errno.ENOTEMPTY)

        return self._hdfs.rename(old, new, user=self._current_user)


    def rmdir(self, path):
        assert not any(p.startswith(path) for p in self._tmpfiles)
        try:
            return self._hdfs.delete(path, user=self._current_user)
        except WebHDFS.WebHDFSDirectoryNotEmptyError as e:
            raise fuse.FuseOSError(errno.ENOTEMPTY)


    def truncate(self, path, length, fh=None):
        def _truncate(self, path):
            self._refresh_tmpfile(path)

            # now, truncate locally
            tmp_fh = self._tmpfiles[path]['fh']
            tmp_fh.truncate(length)

            # flag it dirty, push it to the server
            self._tmpfiles[path]['dirty'] = True
            self._push_tmpfile_if_dirty(path)
            return 0

        # a file doesn't have to be open to call truncate on it
        if path not in self._tmpfiles:
            self._open_tmpfile(path)
            try:
               return _truncate(self, path)
            finally:
                self._remove_tmpfile(path)
        else:
            return _truncate(self, path)


    def unlink(self, path):
        return self._hdfs.delete(path, user=self._current_user)


    def utimens(self, path, times=None):
        if times:
            atime, mtime = times
        else:
            now = time.time()
            atime, mtime = now, now
        return self._hdfs.utime(path, atime, mtime, user=self._current_user)


    def write(self, path, data, offset, fh):
        self._refresh_tmpfile(path)

        tmp_fh = self._tmpfiles[path]['fh']
        tmp_fh.seek(offset)
        tmp_fh.write(data)
        self._logger.debug('Wrote %s bytes to temp copy of %s',
                           len(data), path)
        self._tmpfiles[path]['dirty'] = True
        return len(data)


def main():
    import logging
    logging.basicConfig()

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('host')
    parser.add_argument('--port', type=int, default=50070)
    parser.add_argument('mount')
    parser.add_argument('--debug', action='store_true', default=False)
    parser.add_argument('--foreground', action='store_true', default=False)
    args = parser.parse_args()

    javanicus = Javanicus(args.host, args.port, args.mount, args.debug)
    fs = fuse.FUSE(javanicus,
                   args.mount,
                   foreground=args.foreground,
                   nothreads=True,
                   debug=args.debug)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
