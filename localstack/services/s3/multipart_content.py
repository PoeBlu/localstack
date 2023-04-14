import cgi
import email.parser
from localstack.utils.common import to_bytes


def _iter_multipart_parts(some_bytes, boundary):
    """ Generate a stream of dicts and bytes for each message part.

        Content-Disposition is used as a header for a multipart body:
        https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Disposition
    """
    try:
        parse_data = email.parser.BytesHeaderParser().parsebytes
    except AttributeError:
        # Fall back in case of Python 2.x
        parse_data = email.parser.HeaderParser().parsestr

    while True:
        try:
            part, some_bytes = some_bytes.split(boundary, 1)
        except ValueError:
            # Ran off the end, stop.
            break

        if b'\r\n\r\n' not in part:
            # Real parts have headers and a value separated by '\r\n'.
            continue

        part_head, _ = part.split(b'\r\n\r\n', 1)
        head_parsed = parse_data(part_head.lstrip(b'\r\n'))

        if 'Content-Disposition' in head_parsed:
            _, params = cgi.parse_header(head_parsed['Content-Disposition'])
            yield params, part


def expand_multipart_filename(data, headers):
    """ Replace instance of '${filename}' in key with given file name.

        Data is given as multipart form submission bytes, and file name is
        replace according to Amazon S3 documentation for Post uploads:
        http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPOST.html
    """
    _, params = cgi.parse_header(headers.get('Content-Type', ''))

    if 'boundary' not in params:
        return data

    boundary = params['boundary'].encode('ascii')
    data_bytes = to_bytes(data)

    filename = next(
        (
            disposition['filename']
            for disposition, _ in _iter_multipart_parts(data_bytes, boundary)
            if disposition.get('name') == 'file' and 'filename' in disposition
        ),
        None,
    )
    if filename is None:
        # Found nothing, return unaltered
        return data

    for (disposition, part) in _iter_multipart_parts(data_bytes, boundary):
        if disposition.get('name') == 'key' and b'${filename}' in part:
            search = boundary + part
            replace = boundary + part.replace(b'${filename}', filename.encode('utf8'))

            if search in data_bytes:
                return data_bytes.replace(search, replace)

    return data


def find_multipart_redirect_url(data, headers):
    """ Return object key and redirect URL if they can be found.

        Data is given as multipart form submission bytes, and redirect is found
        in the success_action_redirect field according to Amazon S3
        documentation for Post uploads:
        http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPOST.html
    """
    _, params = cgi.parse_header(headers.get('Content-Type', ''))
    key, redirect_url = None, None

    if 'boundary' not in params:
        return key, redirect_url

    boundary = params['boundary'].encode('ascii')
    data_bytes = to_bytes(data)

    for (disposition, part) in _iter_multipart_parts(data_bytes, boundary):
        if disposition.get('name') == 'key':
            _, value = part.split(b'\r\n\r\n', 1)
            key = value.rstrip(b'\r\n--').decode('utf8')

    if key:
        for (disposition, part) in _iter_multipart_parts(data_bytes, boundary):
            if disposition.get('name') == 'success_action_redirect':
                _, value = part.split(b'\r\n\r\n', 1)
                redirect_url = value.rstrip(b'\r\n--').decode('utf8')

    return key, redirect_url
