import os
import tempfile
import shutil
import requests


def download_file(url, output=None, callback=None, chunk_size=1024*32):
    with requests.session() as sess:
 
        res = sess.get(url, stream=True)
        
        if not res.status_code == 200:
            raise IOError("Remote file not available")
    
        if output is None:
            output = os.path.basename(url)

        output_is_path = isinstance(output, str)

        if output_is_path:
            tmp_file = tempfile.mktemp()
            filehandle = open(tmp_file, 'wb')
        else:
            tmp_file = None
            filehandle = output

        try:
            total = res.headers.get('Content-Length')

            if total is not None:
                total = int(total)

            for chunk in res.iter_content(chunk_size=chunk_size):
                filehandle.write(chunk)
                if callback:
                    callback(len(chunk), total)
            if tmp_file:
                filehandle.close()
                shutil.copy(tmp_file, output)
        except IOError as ne:
            raise IOError("Error when downloading file", ne)
        finally:
            try:
                if tmp_file:
                    os.remove(tmp_file)
            except OSError:
                pass

        return output