from kafka import KafkaConsumer
from time import time
import os
import json
from subprocess import Popen, PIPE


def execute_hive_command(cmd):
    hive = Popen(['hive', '-e', cmd], stdout=PIPE, stderr=PIPE)
    stdout, stderr = hive.communicate()
    if hive.returncode:
        raise Exception(stderr)
    return stdout


hadoop_dst = "/final10002/db1/table1/"
field_terminator = "\001"
line_terminator = "\n"
execute_hive_command(f"""
    use default;
    CREATE EXTERNAL TABLE IF NOT EXISTS articles (
        author STRING,
        title STRING,
        description STRING,
        url STRING,
        urlToImage STRING,
        publishedAt STRING,
        content STRING,
        source_name STRING
    )
    ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY '\001' 
    STORED AS TEXTFILE
    LOCATION '/final10002/db1/table1/';
""")


def upload_to_hdfs(src, dst):
    """
    Upload file to HDFS and return True if successful, else return False
    """
    os.system(f"hadoop fs -copyFromLocal {src} {dst}")
    return os.system(f"hadoop fs -test -e {dst}") == 0  # check existence


def get_article_line(jo):
    # Ensure all fields are present and are string type
    line_data = [
        str(jo.get('author', '')),
        str(jo.get('title', '')),
        str(jo.get('description', '')),
        str(jo.get('url', '')),
        str(jo.get('urlToImage', '')),
        str(jo.get('publishedAt', '')),
        str(jo.get('content', '')),
        str(jo.get('source_name', ''))
    ]
    return field_terminator.join(line_data)


def quote_field(value):
    if ',' in value or '\n' in value or '"' in value:
        return '"' + value.replace('"', '""') + '"'
    return value


def preprocess_article(j):
    for key, value in j.items():
        if isinstance(value, str):
            value = value.replace('\r\n', ' ').replace('\n', ' ')
            j[key] = quote_field(value)
    return j


error_dir = "failed_msg"
if not os.path.exists(error_dir):
    os.makedirs(error_dir)

consumer = KafkaConsumer('my-news',
                         group_id='g1',
                         bootstrap_servers='localhost:9092',
                         value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                         auto_offset_reset='latest',
                         enable_auto_commit=True)
while True:
    start_time = time()
    file_name = f"article_{int(start_time)}.csv"
    json_obj = []

    try:
        for message in consumer:
            preprocessed_article = preprocess_article(message.value)
            print("%s" % preprocessed_article["title"])
            json_obj.append(preprocessed_article)

            if time() - start_time > 60:
                if not json_obj:
                    break  # do not create empty file

                with open(file_name, 'w') as file:
                    for article in json_obj:
                        # Assume article is a dict and values are in the same order as columns in Hive table
                        line = get_article_line(article)
                        file.write(line + line_terminator)

                # Upload current minute's file
                if upload_to_hdfs(file_name, hadoop_dst + file_name):
                    os.remove(file_name)
                else:
                    print(f"Failed to copy {file_name} to HDFS. Moving file to error directory.")
                    os.rename(file_name, os.path.join(error_dir, file_name))

                # Process files in error directory
                for error_file in os.listdir(error_dir):
                    hdfs_path_error = hadoop_dst + error_file
                    if upload_to_hdfs(os.path.join(error_dir, error_file), hdfs_path_error):
                        os.remove(os.path.join(error_dir, error_file))
                    else:
                        print(f"Failed to copy {error_file} from error directory to HDFS.")

                break
    except Exception as e:
        print(f"Error in consumer loop: {e}")
