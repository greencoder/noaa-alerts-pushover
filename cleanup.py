import arrow
import bs4
import os
import time

CUR_DIR = os.path.dirname(os.path.realpath(__file__))
OUTPUT_DIR = os.path.join(CUR_DIR, 'output')

files_to_delete = []
output_files = os.listdir(OUTPUT_DIR)

for filename in output_files:
    
    if filename.endswith('.html'):

        filepath = os.path.join(OUTPUT_DIR, filename)

        with open(filepath, 'r') as f:
            contents = f.read()
            soup = bs4.BeautifulSoup(contents, 'html.parser')

        meta_el = soup.find('meta', {'name': 'expires'})

        if meta_el is None:
            continue

        if meta_el.has_attr('content'):
            expires_at = int(meta_el['content'])
        else:
            continue

        now_ts = arrow.utcnow().timestamp
        if expires_at < now_ts:
            files_to_delete.append(filepath)


for filepath in files_to_delete:
    print 'Deleting filepath: %s' % filepath
    os.remove(filepath)
