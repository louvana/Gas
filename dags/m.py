from mistralai.client import Mistral
import requests
import numpy as np
import faiss
import os
from getpass import getpass

api_key= getpass("type")
client = Mistral(api_key=api_key)