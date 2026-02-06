import os

file = os.path.dirname(os.path.realpath(__file__))
if not file in os.sys.path:
   os.sys.path.append(file)

