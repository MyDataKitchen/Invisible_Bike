from data_pipeline.models.mysql import insert_data_to_record, get_data_processed_record

def taipei_youbike_elt():

    while True:




if __name__ == '__main__':

    while True:
        docs = col.find({ 'created_at' : { '$gt' : temp_time }}, allow_disk_use=True).sort("created_at", 1)
        for doc in docs:
            print(parse_event_data(doc))

        try:
            docs.next()
        except StopIteration:
            docs.rewind()
            time.sleep(running_period)
            print("sleep...")