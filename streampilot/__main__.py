import os, argparse
from .server import *

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-port', type=str, help='port for the server to listen on', required=False, action='store')
    parser.add_argument('-name', type=str, help='name to identify the user', required=False, action='store')
    parser.add_argument('-max_streamhubs', type=str, help='maximum number of Streamhubs to monitor', required=False, action='store')
    parser.add_argument('-user', type=str, help='login username (default: admin)', required=False, action='store')
    parser.add_argument('-password', type=str, help='login password (default: admin)', required=False, action='store')
    parsed_args = vars(parser.parse_args())

    if parsed_args.get('port'):
        os.environ.update({'StreamPilot': parsed_args.get('port')})
    if parsed_args.get('name'):
        os.environ.update({'CLIENT_NAME': parsed_args.get('name')})
    if parsed_args.get('max_streamhubs'):
        os.environ.update({'MAX_STREAMHUB': parsed_args.get('max_streamhubs')})
    if parsed_args.get('user'):
        os.environ.update({'SP_USER': parsed_args.get('user')})
    if parsed_args.get('password'):
        os.environ.update({'SP_PASSWORD': parsed_args.get('password')})
    os.sys.exit(run())
