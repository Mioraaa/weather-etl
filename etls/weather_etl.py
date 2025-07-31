

import s3fs

def connect_to_s3():
    try:
        s3 = s3fs.S3FileSystem(
            anon=False,
            key=, 
            secret=
        )
        return s3
    except Exception as e:
        print("Error while connecting to S3: {0}".format(e))


def create_bucket_if_not_exist(s3: s3fs.S3FileSystem, bucket_name: str):
    try:
        if not s3.exists(bucket_name):
            s3.mkdir(bucket_name)
            print(f"bucket {bucket_name} created.")
        else:
            print(f"bucket {bucket_name} already exists.")
    except Exception as e:
        print("Error while creating bucket {0}".format(e))
