# Copyright (c) 2009-2012, Andrew McNabb
# Copyright (c) 2003-2008, Brent N. Chun

import re
import sys
import fnmatch

HOST_FORMAT = 'Host format is [user@]host[:port] [user]'


def read_host_files(paths, host_glob, default_user=None, default_port=None):
    """Reads the given host files.

    Returns a list of (host, port, user) triples.
    """
    hosts = []
    if paths:
        for path in paths:
            hosts.extend(read_host_file(path, host_glob, default_user=default_user))
    return hosts


def read_host_file(path, host_glob, default_user=None, default_port=None):
    """Reads the given host file.

    Lines are of the form: host[:port] [login].
    Returns a list of (host, port, user) triples.
    """
    lines = []
    f = open(path)
    for line in f:
        lines.append(line.strip())
    f.close()

    hosts = []
    for line in lines:
        # remove trailing comments
        line = re.sub('#.*', '', line)
        line = line.strip()
        # skip blank lines (or lines with only comments)
        if not line:
            continue
        host, port, user = parse_host_entry(line, default_user, default_port)
        if host and (not host_glob or fnmatch.fnmatch(host, host_glob)):
            hosts.append((host, port, user))
    return hosts


# TODO: deprecate the second host field and standardize on the
# [user@]host[:port] format.
def parse_host_entry(line, default_user, default_port):
    """Parses a single host entry.

    This may take either the of the form [user@]host[:port] or
    host[:port][ user].

    Returns a (host, port, user) triple.
    """
    fields = line.split()
    if len(fields) > 2:
        sys.stderr.write('Bad line: "%s". Format should be'
                ' [user@]host[:port] [user]\n' % line)
        return None, None, None
    host_field = fields[0]
    host, port, user = parse_host(host_field, default_port=default_port)
    if len(fields) == 2:
        if user is None:
            user = fields[1]
        else:
            sys.stderr.write('User specified twice in line: "%s"\n' % line)
            return None, None, None
    if user is None:
        user = default_user
    return host, port, user


def parse_host_string(host_string, default_user=None, default_port=None):
    """Parses a whitespace-delimited string of "[user@]host[:port]" entries.

    Returns a list of (host, port, user) triples.
    """
    hosts = []
    entries = host_string.split()
    for entry in entries:
        hosts.append(parse_host(entry, default_user, default_port))
    return hosts


def parse_host(host, default_user=None, default_port=None):
    """Parses host entries of the form "[user@]host[:port]".

    Returns a (host, port, user) triple.
    """
    m = re.match('^(?:([^@]+)@)?(.*?)(?::([0-9]+))?$', host)
    host = m.group(2)
    user = m.group(1) or default_user
    port = m.group(3) or default_port
    if host.startswith('[') and host.endswith(']'):
        host = host[1:-1]
    return (host, port, user)


