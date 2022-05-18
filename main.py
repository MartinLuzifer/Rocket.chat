from time import time
from conf import ssh, mongo

import pymongo
from sshtunnel import SSHTunnelForwarder


def re_index(db_name):
    start = time()
    print(f'reIndex {db_name} collection')
    db[db_name].reindex()
    print(f'End reIndex {db_name} \n')
    print(f'{db_name} has been reIndex() for {round((time() - start), 3)}ms')


server = SSHTunnelForwarder(

    ssh_address_or_host=ssh.get('MONGO_HOST'),
    ssh_port=ssh.get('MONGO_PORT'),
    ssh_username=ssh.get('MONGO_USER'),
    ssh_password=ssh.get('MONGO_PASS'),
    remote_bind_address=('127.0.0.1', 27017)

)

if __name__ == '__main__':

    try:
        server.start()
    except:
        print("Invalid data in conf.py")
    else:
        print(f"ssh {ssh.get('MONGO_USER')}@{ssh.get('MONGO_HOST')}:{ssh.get('MONGO_PORT')} connect...")

        client = pymongo.MongoClient('127.0.0.1', server.local_bind_port)
        db = client[mongo.get('MONGO_COLLECTION_NAME')]

        now = time()
        for db_names in db.list_collection_names():
            re_index(db_names)
        print(f'Выполнено: {round(time() - now, 3)} seconds')

        server.stop()
        print(f"ssh {ssh.get('MONGO_USER')}@{ssh.get('MONGO_HOST')}:{ssh.get('MONGO_PORT')} disconnect...")



