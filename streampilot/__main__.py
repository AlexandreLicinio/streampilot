import os, argparse
from .server import *

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-port', type=str, help='port for the server to listen on', required=False)
    parser.add_argument('-name', type=str, help='name to identify the user', required=False)
    parser.add_argument('-max_streamhubs', type=str, help='maximum number of Streamhubs to monitor', required=False)
    parser.add_argument('-user', type=str, help='login username (default: admin)', required=False)
    parser.add_argument('-password', type=str, help='login password (default: admin)', required=False)
    parser.add_argument('-mode', type=str, choices=['http', 'proxy'],
                        help='http = direct (default) | proxy = behind HTTPS reverse proxy', required=False)
    parsed_args = vars(parser.parse_args())

    if parsed_args.get('port'):
        os.environ['StreamPilot'] = parsed_args['port']
    if parsed_args.get('name'):
        os.environ['CLIENT_NAME'] = parsed_args['name']
    if parsed_args.get('max_streamhubs'):
        os.environ['MAX_STREAMHUB'] = parsed_args['max_streamhubs']
    if parsed_args.get('user'):
        os.environ['SP_USER'] = parsed_args['user']
    if parsed_args.get('password'):
        os.environ['SP_PASSWORD'] = parsed_args['password']
    if parsed_args.get('mode'):
        os.environ['SP_MODE'] = parsed_args['mode']

    os.sys.exit(run())
