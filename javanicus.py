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

# TODO - error handling pass
# TODO - fix user/group <-> uid/gid
# TODO - auth!

import errno
import stat
import urlparse

import fuse
import requests


class Javanicus(fuse.Operations, fuse.LoggingMixIn):
    MODE_FLAGS = {
        'DIRECTORY': stat.S_IFDIR,
        'FILE': stat.S_IFREG,
        'SYMLINK': stat.S_IFLNK,
    }


    def __init__(self, host, port, path='.'):
        self.host = host
        self.port = port
        self.base_url = 'http://%s:%s/webhdfs/v1/' % (self.host, self.port)
        self.root = path
        self.session = requests.session()
        self.context = dict(zip(('uid', 'gid', 'pid'), fuse.fuse_get_context()))


    def _url(path):
        # strip the leading / to please urljoin
        return urlparse.urljoin(self.base_url, path.lstrip('/'))


    def chmod(self, path, mode):
        '''
        PUT /webhdfs/v1/<PATH>?op=SETPERMISSION[&permission=<OCTAL>]
        '''
        response = self.session.put(self._url(path), params={'op': 'SETPERMISSION',
                                                             'permission': oct(mode)})
        response.raise_for_status()
        return 0


    def chown(self, path, uid, gid):
        '''
        PUT /webhdfs/v1/<PATH>?op=SETOWNER[&user=<USER>][&group=<GROUP>]
        '''
        response = self.session.put(self._url(path), params={'op': 'SETOWNER',
                                                             'user': uid,
                                                             'group': gid})
        response.raise_for_status()
        return 0


    def create(self, path, mode):
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
        s1_response = self.session.put(self._url(path), params={'op': 'CREATE',
                                                                'mode': oct(mode)})
        s1_response.raise_for_status()
        s2_url = s1_response.headers['location']
        s2_response = self.session.put(s2_url, params={'op': 'CREATE',
                                                       'mode': oct(mode)})
        s2_response.raise_for_status()
        return 0


    def destroy(self, path):
        self.session.close()


    def getattr(self, path, fh=None):
        '''
        GET /webhdfs/v1/<PATH>?op=GETFILESTATUS
        '''
        response = self.session.get(self._url(path), params={'op': 'GETFILESTATUS'})
        response.raise_for_status()
        hdfs_status = response.json()['FileStatus']
        status = {
            'st_atime': hdfs_status['accessTime'],
            'st_gid': hdfs_status['group'],
            'st_mode': (int(hdfs_status['permission'], 8)
                        | self.MODE_FLAGS[hdfs_status['type']]),
            'st_mtime': hdfs_status['modificationTime'],
            'st_size': hdfs_stats['length'],
        }
        if stat.S_

        return status


    def mkdir(self, path, mode):
        '''
        PUT /webhdfs/v1/<PATH>?op=MKDIRS[&permission=<OCTAL>]
        '''
        response = self.session.put(self._url(path), params={'op': 'MKDIRS',
                                                             'permission': permission})
        response.raise_for_status()
        return 0 if response.json()['boolean'] else -errno.EREMOTEIO


    def read(self, path, size, offset, fh):
        '''
        GET /webhdfs/v1/<PATH>?op=OPEN[&offset=<LONG>][&length=<LONG>][&buffersize=<INT>]

        <namenode redirects to datanode, requests will auto-follow>
        '''
        response = self.session.get(self._url(path), params={'op': 'OPEN',
                                                             'offset': offset,
                                                             'length': size})
        response.raise_for_status()
        # TODO - sanity check the response size vs. requested size?
        return response.content


    def readdir(self, path, fh):
        '''
        GET /webhdfs/v1/<PATH>?op=LISTSTATUS
        '''
        response = self.session.get(self._url(path), params={'op': 'LISTSTATUS'})
        response.raise_for_status()
        dir_contents = ['.', '..']
        for status in response.json()['FileStatuses']['FileStatus']:
            dir_contents.append(status['pathSuffix'])
        return dir_contents


    def rename(self, old, new):
        '''
        PUT /webhdfs/v1/<PATH>?op=RENAME&destination=<PATH>
        '''
        response = self.session.put(self._url(old), params={'op': 'RENAME',
                                                            'destination': new})
        response.raise_for_status()
        return 0 if response.json()['boolean'] else -errno.EREMOTEIO


    def rmdir(self, path):
        '''
        DELETE /webhdfs/v1/<path>?op=DELETE[&recursive=<true|false>]
        '''
        response = self.session.delete(self._url(path), params={'op': 'DELETE'})
        response.raise_for_status()
        return 0 if response.json()['boolean'] else -errno.EREMOTEIO


    def symlink(self, target, source):
        '''
        PUT /webhdfs/v1/<PATH>?op=CREATESYMLINK&destination=<PATH>[&createParent=<true|false>]
        '''
        response = self.session.put(self._url(source), params={'op': 'CREATESYMLINK',
                                                               'destination': target})
        response.raise_for_status()
        return 0


    def truncate(self, path, length, fh=None):
        '''
        like create, but actually send the file data

        do i need to copy the original down, truncate it, then upload it?
        i hope not.
        '''
        pass


    def unlink(self, path):
        '''
        DELETE /webhdfs/v1/<path>?op=DELETE[&recursive=<true|false>]
        '''
        response = self.session.delete(self._url(path), params={'op': 'DELETE'})
        response.raise_for_status()
        return 0 if response.json()['boolean'] else -errno.EREMOTEIO


    def write(self, path, data, offset, fh):
        '''
        like create, but actually send the file data

        do i need to copy the original down, patch it, then upload it?
        i hope not.
        '''
        pass


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('host')
    parser.add_argument('port')
    parser.add_argument('mountpoint')
    args = parser.parse_args

    fs = fuse.FUSE(Javanicus(args.url), args.mountpoint,
                   foreground=True, nothreads=True)
