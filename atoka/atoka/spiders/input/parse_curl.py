import os


def get_headers_from_curl(file_path):
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f'No such file - {file_path}')
    curl_headers = []
    with open(file_path, 'r') as curl:
        for line in curl:
            if line.strip().startswith('-H'):
                curl_headers.append(line)

    curl_headers = [obj.split('\'')[1] for obj in curl_headers]
    curl_headers_dict = {obj.split(': ')[0]: ' '.join(obj.split(': ')[1:]) for obj in curl_headers}
    return curl_headers_dict
