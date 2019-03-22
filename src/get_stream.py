import streamlink
import os
import time
from threading import Thread
import ffmpeg
import Algorithmia
from subprocess import Popen, PIPE

client = Algorithmia.client(os.getenv('ALGORITHMIA_API_KEY'))

def get_stream(url, quality='480p'):
    streams = streamlink.streams(url)
    if streams:
        return streams[quality].to_url()
    else:
        raise ValueError("No steams were available")


def create_video_blocks(stream, path_naming='/tmp/stream_file_%d.mp4', segment_time = '00:00:10'):
    print("starting file download coroutine")
    proc = ffmpeg.input(stream)
    proc = ffmpeg.output(proc, path_naming, segment_time= segment_time, f="segment", reset_timestamps='1')
    proc.run_async()
    return None


def upload_files(client, local_path_naming='/tmp/stream_file_%d.mp4', remote_path_naming='data://.my/streaming_collection/stream_file_%d.mp4'):
    itr = 0
    print("starting file upload coroutine")
    while True:
        time.sleep(1)
        local_file_path = local_path_naming.replace('%d', str(itr))
        remote_file_path = remote_path_naming.replace('%d', str(itr))
        if os.path.exists(local_file_path):
            file_check_process = Popen("lsof -f -- {}".format(local_file_path).split(' '), stdout=PIPE)
            file_chcker = file_check_process.wait()
            if file_chcker == 1:
                upload_file(client, local_file_path, remote_file_path)
                itr += 1


def upload_file(client, local_path, remote_path):
    client.file(remote_path).putFilez(local_path)
    print("uploaded {}".format(remote_path))
    return remote_path


def main():
    stream = get_stream("https://www.twitch.tv/dreamleague")
    download_t = Thread(target=create_video_blocks, args=(stream,))
    upload_t = Thread(target=upload_files, args=(client,))
    print("starting to await")
    download_t.run()
    upload_t.run()
    download_t.join()
    upload_t.join()

if __name__ == "__main__":
    main()
